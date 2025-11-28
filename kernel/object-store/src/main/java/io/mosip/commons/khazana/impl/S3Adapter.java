package io.mosip.commons.khazana.impl;

import io.mosip.commons.khazana.config.LoggerConfiguration;
import io.mosip.commons.khazana.dto.ObjectDto;
import io.mosip.commons.khazana.exception.ObjectStoreAdapterException;
import io.mosip.commons.khazana.spi.ObjectStoreAdapter;
import io.mosip.commons.khazana.util.ObjectStoreUtil;
import io.mosip.kernel.core.exception.ExceptionUtils;
import io.mosip.kernel.core.logger.spi.Logger;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static io.mosip.commons.khazana.config.LoggerConfiguration.REGISTRATIONID;
import static io.mosip.commons.khazana.config.LoggerConfiguration.SESSIONID;
import static io.mosip.commons.khazana.constant.KhazanaConstant.TAGS_FILENAME;
import static io.mosip.commons.khazana.constant.KhazanaErrorCodes.OBJECT_STORE_NOT_ACCESSIBLE;

@Service
@Qualifier("S3Adapter")
public class S3Adapter implements ObjectStoreAdapter {

    private final Logger LOGGER = LoggerConfiguration.logConfig(S3Adapter.class);

    @Value("${object.store.s3.accesskey:accesskey:accesskey}")
    private String accessKey;

    @Value("${object.store.s3.secretkey:secretkey:secretkey}")
    private String secretKey;

    @Value("${object.store.s3.url:null}")
    private String url;

    @Value("${object.store.s3.region:null}")
    private String region;

    @Value("${object.store.s3.readlimit:10000000}")
    private int readlimit;

    @Value("${object.store.connection.max.retry:20}")
    private int maxRetry;

    @Value("${object.store.max.connection:200}")
    private int maxConnection;

    @Value("${object.store.connection.timeout:5000}")
    private int connectionTimeout;

    @Value("${object.store.socket.timeout:10000}")
    private int socketTimeout;

    @Value("${object.store.client.execution.timeout:15000}")
    private int clientExecutionTimeout; // not directly used in v2, but kept for compatibility

    @Value("${object.store.s3.use.account.as.bucketname:false}")
    private boolean useAccountAsBucketname;

    @Value("${object.store.s3.bucket-name-prefix:}")
    private String bucketNamePrefix;

    private static final String SEPARATOR = "/";

    private static final String TAG_BACKWARD_COMPATIBILITY_ERROR =
            "Object-prefix is already an object, please choose a different object-prefix name";

    private static final String TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR =
            "Access Denied";

    private volatile S3Client s3Client;
    private int retry = 0;
    private final List<String> existingBuckets = new ArrayList<>();

    /* ==========================================================================================
       S3 CLIENT (Singleton, Connection-Pooled)
       ========================================================================================== */
    private S3Client getConnection(String bucketName) {
        if (s3Client != null) {
            return s3Client;
        }

        synchronized (this) {
            if (s3Client != null) {
                return s3Client;
            }

            try {
                AwsBasicCredentials credentials =
                        AwsBasicCredentials.create(accessKey, secretKey);

                ApacheHttpClient.Builder httpBuilder = ApacheHttpClient.builder()
                        .maxConnections(maxConnection)
                        .connectionTimeout(Duration.ofMillis(connectionTimeout))
                        .socketTimeout(Duration.ofMillis(socketTimeout))
                        .tcpKeepAlive(true);

                S3Configuration s3Config = S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build();

                S3ClientBuilder builder = S3Client.builder()
                        .credentialsProvider(StaticCredentialsProvider.create(credentials))
                        .httpClientBuilder(httpBuilder)
                        .serviceConfiguration(s3Config);

                if (region != null && !"null".equalsIgnoreCase(region)) {
                    builder = builder.region(Region.of(region));
                }

                if (url != null && !"null".equalsIgnoreCase(url)) {
                    builder = builder.endpointOverride(URI.create(url));
                }

                s3Client = builder.build();

                retry = 0;
            } catch (Exception e) {
                if (retry >= maxRetry) {
                    retry = 0;
                    s3Client = null;
                    LOGGER.error(SESSIONID, REGISTRATIONID,
                            "Maximum retry limit exceeded. Could not obtain S3 connection for " + bucketName
                                    + ". Retry count :" + retry,
                            ExceptionUtils.getStackTrace(e));
                    throw new ObjectStoreAdapterException(
                            OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                            OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
                } else {
                    s3Client = null;
                    retry = retry + 1;
                    LOGGER.error(SESSIONID, REGISTRATIONID,
                            "Exception occurred while obtaining S3 connection for " + bucketName
                                    + ". Will try again. Retry count : " + retry,
                            ExceptionUtils.getStackTrace(e));
                    return getConnection(bucketName);
                }
            }
        }
        return s3Client;
    }

    /* ==========================================================================================
       BUCKET / KEY HELPERS – EXACTLY LIKE ORIGINAL (Case A)
       ========================================================================================== */

    private String addBucketPrefix(String bucketName) {
        if (bucketNamePrefix == null || bucketNamePrefix.isEmpty()) {
            return bucketName;
        }
        if (bucketName.startsWith(bucketNamePrefix)) {
            LOGGER.debug("Already bucketName with prefix is present " + bucketName);
            return bucketName;
        } else {
            String withPrefix = bucketNamePrefix + bucketName;
            LOGGER.debug("Adding Prefix to bucketName " + withPrefix);
            return withPrefix;
        }
    }

    private boolean doesBucketExists(S3Client client, String bucketName) {
        // use account as bucket name and bucket name is present in existing bucket list
        if (useAccountAsBucketname && existingBuckets.contains(bucketName)) {
            return true;
        } else if (useAccountAsBucketname && !existingBuckets.contains(bucketName)) {
            boolean exists = headBucketExists(client, bucketName);
            if (exists) {
                existingBuckets.add(bucketName);
            }
            return exists;
        } else {
            return headBucketExists(client, bucketName);
        }
    }

    private boolean headBucketExists(S3Client client, String bucketName) {
        try {
            client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
            return true;
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return false;
            }
            throw e;
        }
    }

    private String buildFinalObjectName(String container, String source, String process, String objectName) {
        if (useAccountAsBucketname) {
            // container/source/process/objectName
            return ObjectStoreUtil.getName(container, source, process, objectName);
        } else {
            // source/process/objectName
            return ObjectStoreUtil.getName(source, process, objectName);
        }
    }

    private String resolveBucketName(String account, String container) {
        String bucketName;
        if (useAccountAsBucketname) {
            bucketName = account;
        } else {
            bucketName = container;
        }
        bucketName = addBucketPrefix(bucketName);
        bucketName = bucketName.toLowerCase(); // S3 requirement
        return bucketName;
    }

    /* ==========================================================================================
       OBJECT OPERATIONS
       ========================================================================================== */

    @Override
    public InputStream getObject(String account, String container, String source, String process, String objectName) {
        String finalObjectName = buildFinalObjectName(container, source, process, objectName);
        String bucketName = resolveBucketName(account, container);

        S3Client client = getConnection(bucketName);
        ResponseInputStream<GetObjectResponse> s3Object = null;

        try {
            s3Object = client.getObject(
                    GetObjectRequest.builder()
                            .bucket(bucketName)
                            .key(finalObjectName)
                            .build()
            );

            if (s3Object != null) {
                ByteArrayOutputStream temp = new ByteArrayOutputStream();
                IOUtils.copy(s3Object, temp);
                return new ByteArrayInputStream(temp.toByteArray());
            }
        } catch (Exception e) {
            s3Client = null;
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occurred to getObject for : " + container,
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        } finally {
            if (s3Object != null) {
                try {
                    s3Object.close();
                } catch (IOException e) {
                    LOGGER.error(SESSIONID, REGISTRATIONID,
                            "IO occurred : " + container,
                            ExceptionUtils.getStackTrace(e));
                }
            }
        }
        return null;
    }

    @Override
    public boolean exists(String account, String container, String source, String process, String objectName) {
        String finalObjectName = buildFinalObjectName(container, source, process, objectName);
        String bucketName = resolveBucketName(account, container);

        S3Client client = getConnection(bucketName);
        try {
            client.headObject(
                    HeadObjectRequest.builder()
                            .bucket(bucketName)
                            .key(finalObjectName)
                            .build()
            );
            return true;
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return false;
            }
            return false; // safest behavior similar to "doesObjectExist" false
        }
    }

    @Override
    public boolean putObject(String account, final String container, String source,
                             String process, String objectName, InputStream data) {

        String finalObjectName = buildFinalObjectName(container, source, process, objectName);
        String bucketName = resolveBucketName(account, container);

        S3Client client = getConnection(bucketName);

        if (!doesBucketExists(client, bucketName)) {
            client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
            if (useAccountAsBucketname) {
                existingBuckets.add(bucketName);
            }
        }

        Path tempFile = null;
        try {
            RequestBody body;

            if (data instanceof FileInputStream fis) {
                long size = fis.getChannel().size();
                body = RequestBody.fromInputStream(data, size);
            } else if (data instanceof ByteArrayInputStream) {
                byte[] bytes = ((ByteArrayInputStream) data).readAllBytes();
                body = RequestBody.fromBytes(bytes);
            } else {
                tempFile = Files.createTempFile("khazana-s3-put-", ".tmp");
                try (OutputStream out = Files.newOutputStream(tempFile)) {
                    byte[] buf = new byte[64 * 1024];
                    int n;
                    while ((n = data.read(buf)) != -1) {
                        out.write(buf, 0, n);
                    }
                }
                body = RequestBody.fromFile(tempFile);
            }

            client.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(finalObjectName)
                            .build(),
                    body
            );

            return true;
        } catch (Exception e) {
            s3Client = null;
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occurred to putObject for : " + container,
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        } finally {
            if (tempFile != null) {
                try {
                    Files.deleteIfExists(tempFile);
                } catch (IOException ignore) {
                }
            }
        }
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source,
                                                 String process, String objectName,
                                                 Map<String, Object> metadata) {

        String finalObjectName = buildFinalObjectName(container, source, process, objectName);
        String bucketName = resolveBucketName(account, container);

        S3Client client = getConnection(bucketName);
        ResponseInputStream<GetObjectResponse> s3Object = null;

        try {
            s3Object = client.getObject(
                    GetObjectRequest.builder()
                            .bucket(bucketName)
                            .key(finalObjectName)
                            .build()
            );

            Map<String, String> userMeta = new HashMap<>();
            Map<String, String> existingUserMeta = s3Object.response().metadata();
            if (existingUserMeta != null) {
                userMeta.putAll(existingUserMeta);
            }

            for (Map.Entry<String, Object> m : metadata.entrySet()) {
                userMeta.put(m.getKey(), m.getValue() != null ? m.getValue().toString() : null);
            }

            // read object content into memory (same as original behavior)
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            IOUtils.copy(s3Object, baos);
            byte[] content = baos.toByteArray();

            client.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(finalObjectName)
                            .metadata(userMeta)
                            .build(),
                    RequestBody.fromBytes(content)
            );

            return metadata;
        } catch (Exception e) {
            s3Client = null;
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occurred to addObjectMetaData for : " + container,
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        } finally {
            if (s3Object != null) {
                try {
                    s3Object.close();
                } catch (IOException e) {
                    LOGGER.error(SESSIONID, REGISTRATIONID,
                            "IO occurred : " + container,
                            ExceptionUtils.getStackTrace(e));
                }
            }
        }
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container,
                                                 String source, String process,
                                                 String objectName, String key, String value) {
        Map<String, Object> meta = new HashMap<>();
        meta.put(key, value);
        String finalObjectName;
        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
        }
        return addObjectMetaData(account, container, source, process, finalObjectName, meta);
    }

    @Override
    public Map<String, Object> getMetaData(String account, String container,
                                           String source, String process, String objectName) {

        String finalObjectName = buildFinalObjectName(container, source, process, objectName);
        String bucketName = resolveBucketName(account, container);

        S3Client client = getConnection(bucketName);
        try {
            HeadObjectResponse response =
                    client.headObject(
                            HeadObjectRequest.builder()
                                    .bucket(bucketName)
                                    .key(finalObjectName)
                                    .build()
                    );

            Map<String, Object> metaData = new HashMap<>();
            Map<String, String> userMeta = response.metadata();
            if (userMeta != null) {
                userMeta.forEach(metaData::put);
            }
            return metaData;
        } catch (Exception e) {
            s3Client = null;
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occurred to getMetaData for : " + container,
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public Integer incMetadata(String account, String container, String source,
                               String process, String objectName, String metaDataKey) {
        Map<String, Object> metadata = getMetaData(account, container, source, process, objectName);
        if (metadata.get(metaDataKey) != null) {
            metadata.put(metaDataKey, Integer.valueOf(metadata.get(metaDataKey).toString()) + 1);
            addObjectMetaData(account, container, source, process, objectName, metadata);
            return Integer.valueOf(metadata.get(metaDataKey).toString());
        }
        return null;
    }

    @Override
    public Integer decMetadata(String account, String container, String source,
                               String process, String objectName, String metaDataKey) {
        Map<String, Object> metadata = getMetaData(account, container, source, process, objectName);
        if (metadata.get(metaDataKey) != null) {
            metadata.put(metaDataKey, Integer.valueOf(metadata.get(metaDataKey).toString()) - 1);
            addObjectMetaData(account, container, source, process, objectName, metadata);
            return Integer.valueOf(metadata.get(metaDataKey).toString());
        }
        return null;
    }

    @Override
    public boolean deleteObject(String account, String container, String source, String process, String objectName) {
        String finalObjectName = buildFinalObjectName(container, source, process, objectName);
        String bucketName = resolveBucketName(account, container);

        S3Client client = getConnection(bucketName);
        client.deleteObject(
                DeleteObjectRequest.builder()
                        .bucket(bucketName)
                        .key(finalObjectName)
                        .build()
        );
        return true;
    }

    @Override
    public boolean removeContainer(String account, String container, String source, String process) {
        // Not supported as in original
        return false;
    }

    @Override
    public boolean pack(String account, String container, String source, String process, String refId) {
        // Not supported as in original
        return false;
    }

    /* ==========================================================================================
       LIST ALL OBJECTS
       ========================================================================================== */

    @Override
    public List<ObjectDto> getAllObjects(String account, String id) {
        List<S3Object> os;

        S3Client client;
        if (useAccountAsBucketname) {
            String searchPattern = id + SEPARATOR;
            account = addBucketPrefix(account);
            account = account.toLowerCase();
            client = getConnection(account);
            os = client.listObjectsV2(
                    ListObjectsV2Request.builder()
                            .bucket(account)
                            .prefix(searchPattern)
                            .build()
            ).contents();
        } else {
            id = addBucketPrefix(id);
            id = id.toLowerCase();
            client = getConnection(id);
            os = client.listObjectsV2(
                    ListObjectsV2Request.builder()
                            .bucket(id)
                            .build()
            ).contents();
        }

        if (os != null && !os.isEmpty()) {
            List<ObjectDto> objectDtos = new ArrayList<>();
            os.forEach(o -> {
                String[] tempKeys = o.key().split(SEPARATOR);

                // ignore Tag file entries
                if (useAccountAsBucketname) {
                    if (tempKeys.length > 1 && tempKeys[1] != null && tempKeys[1].endsWith(TAGS_FILENAME)) {
                        tempKeys = null;
                    }
                } else {
                    if (tempKeys.length > 0 && tempKeys[0] != null && tempKeys[0].endsWith(TAGS_FILENAME)) {
                        tempKeys = null;
                    }
                }

                String[] keys = removeIdFromObjectPath(useAccountAsBucketname, tempKeys);
                if (ArrayUtils.isNotEmpty(keys)) {
                    ObjectDto objectDto = null;
                    switch (keys.length) {
                        case 1:
                            objectDto = new ObjectDto(null, null, keys[0], Date.from(o.lastModified()));
                            break;
                        case 2:
                            objectDto = new ObjectDto(keys[0], null, keys[1], Date.from(o.lastModified()));
                            break;
                        case 3:
                            objectDto = new ObjectDto(keys[0], keys[1], keys[2], Date.from(o.lastModified()));
                            break;
                        default:
                            break;
                    }
                    if (objectDto != null) {
                        objectDtos.add(objectDto);
                    }
                }
            });
            return objectDtos;
        }

        return null;
    }

    private String[] removeIdFromObjectPath(boolean useAccountAsBucketname, String[] keys) {
        return (useAccountAsBucketname && ArrayUtils.isNotEmpty(keys))
                ? (String[]) ArrayUtils.remove(keys, 0)
                : keys;
    }

    /* ==========================================================================================
       TAGS (Same behavior as original)
       ========================================================================================== */

    @Override
    public Map<String, String> addTags(String account, String container, Map<String, String> tags) {
        String bucketName;
        String finalObjectName;
        try {
            if (useAccountAsBucketname) {
                bucketName = account;
                finalObjectName = ObjectStoreUtil.getName(container, null, TAGS_FILENAME);
            } else {
                bucketName = container;
                finalObjectName = TAGS_FILENAME;
            }
            bucketName = addBucketPrefix(bucketName);
            bucketName = bucketName.toLowerCase();

            S3Client client = getConnection(bucketName);
            if (!doesBucketExists(client, bucketName)) {
                client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
                if (useAccountAsBucketname) {
                    existingBuckets.add(bucketName);
                }
            }

            for (Map.Entry<String, String> entry : tags.entrySet()) {
                String tagName;
                InputStream data = IOUtils.toInputStream(entry.getValue(), StandardCharsets.UTF_8);
                tagName = ObjectStoreUtil.getName(finalObjectName, entry.getKey());
                try {
                    client.putObject(
                            PutObjectRequest.builder()
                                    .bucket(bucketName)
                                    .key(tagName)
                                    .build(),
                            RequestBody.fromInputStream(data, entry.getValue().getBytes(StandardCharsets.UTF_8).length)
                    );
                } catch (S3Exception e) {
                    String msg = e.getMessage() != null ? e.getMessage() : "";
                    if (msg.contains(TAG_BACKWARD_COMPATIBILITY_ERROR)
                            || msg.contains(TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR)) {

                        // Backward compatibility: old single-object tag storage
                        try {
                            client.headObject(
                                    HeadObjectRequest.builder()
                                            .bucket(bucketName)
                                            .key(finalObjectName)
                                            .build()
                            );
                            // If object exists → delete it and retry addTags
                            client.deleteObject(
                                    DeleteObjectRequest.builder()
                                            .bucket(bucketName)
                                            .key(finalObjectName)
                                            .build()
                            );
                            addTags(account, container, tags);
                        } catch (S3Exception he) {
                            s3Client = null;
                            LOGGER.error(SESSIONID, REGISTRATIONID,
                                    "Exception occurred while addTags for : " + container,
                                    ExceptionUtils.getStackTrace(e));
                            throw new ObjectStoreAdapterException(
                                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
                        }
                    } else {
                        s3Client = null;
                        LOGGER.error(SESSIONID, REGISTRATIONID,
                                "Exception occurred while addTags for : " + container,
                                ExceptionUtils.getStackTrace(e));
                        throw new ObjectStoreAdapterException(
                                OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                                OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
                    }
                }
            }

        } catch (Exception e) {
            s3Client = null;
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occurred while addTags for : " + container,
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
        return tags;
    }

    @Override
    public Map<String, String> getTags(String account, String container) {
        Map<String, String> objectTags = new HashMap<>();
        try {
            String bucketName;
            String finalObjectName;
            if (useAccountAsBucketname) {
                bucketName = account;
                finalObjectName = ObjectStoreUtil.getName(container, null, TAGS_FILENAME) + SEPARATOR;
            } else {
                bucketName = container;
                finalObjectName = TAGS_FILENAME + SEPARATOR;
            }
            bucketName = addBucketPrefix(bucketName);
            bucketName = bucketName.toLowerCase();

            S3Client client = getConnection(bucketName);

            List<S3Object> objectSummary;
            if (useAccountAsBucketname) {
                objectSummary = client.listObjectsV2(
                                ListObjectsV2Request.builder()
                                        .bucket(bucketName)
                                        .prefix(finalObjectName)
                                        .build())
                        .contents();
            } else {
                objectSummary = client.listObjectsV2(
                                ListObjectsV2Request.builder()
                                        .bucket(bucketName)
                                        .build())
                        .contents();
            }

            List<String> tagNames = new ArrayList<>();
            if (objectSummary != null && !objectSummary.isEmpty()) {
                objectSummary.forEach(o -> {
                    String[] keys = o.key().split(SEPARATOR);
                    if (ArrayUtils.isNotEmpty(keys)) {
                        if (useAccountAsBucketname) {
                            if (keys.length > 1 && keys[1] != null && keys[1].endsWith(TAGS_FILENAME)) {
                                if (keys.length > 2) {
                                    tagNames.add(keys[2]);
                                }
                            }
                        } else {
                            if (keys.length > 0 && keys[0] != null && keys[0].endsWith(TAGS_FILENAME)) {
                                if (keys.length > 1) {
                                    tagNames.add(keys[1]);
                                }
                            }
                        }
                    }
                });
            }

            for (String tagName : tagNames) {
                String fullKey = finalObjectName + tagName;
                ResponseInputStream<GetObjectResponse> is = client.getObject(
                        GetObjectRequest.builder()
                                .bucket(bucketName)
                                .key(fullKey)
                                .build()
                );
                try {
                    objectTags.put(tagName, new String(is.readAllBytes(), StandardCharsets.UTF_8));
                } finally {
                    is.close();
                }
            }

            return objectTags;
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occurred while getTags for : " + container,
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public void deleteTags(String account, String container, List<String> tags) {
        try {
            String bucketName;
            String finalObjectName;
            if (useAccountAsBucketname) {
                bucketName = account;
                finalObjectName = ObjectStoreUtil.getName(container, null, TAGS_FILENAME);
            } else {
                bucketName = container;
                finalObjectName = TAGS_FILENAME;
            }
            bucketName = addBucketPrefix(bucketName);
            bucketName = bucketName.toLowerCase();

            S3Client client = getConnection(bucketName);
            if (!doesBucketExists(client, bucketName)) {
                client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
                if (useAccountAsBucketname) {
                    existingBuckets.add(bucketName);
                }
            }

            for (String tag : tags) {
                String tagName = ObjectStoreUtil.getName(finalObjectName, tag);
                client.deleteObject(
                        DeleteObjectRequest.builder()
                                .bucket(bucketName)
                                .key(tagName)
                                .build()
                );
            }

        } catch (Exception e) {
            s3Client = null;
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occurred while deleteTags for : " + container,
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }
}
