package io.mosip.commons.khazana.impl;

import static io.mosip.commons.khazana.config.LoggerConfiguration.REGISTRATIONID;
import static io.mosip.commons.khazana.config.LoggerConfiguration.SESSIONID;
import static io.mosip.commons.khazana.constant.KhazanaConstant.TAGS_FILENAME;
import static io.mosip.commons.khazana.constant.KhazanaErrorCodes.OBJECT_STORE_NOT_ACCESSIBLE;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import io.mosip.commons.khazana.config.LoggerConfiguration;
import io.mosip.commons.khazana.dto.ObjectDto;
import io.mosip.commons.khazana.exception.ObjectStoreAdapterException;
import io.mosip.commons.khazana.spi.ObjectStoreAdapter;
import io.mosip.commons.khazana.util.ObjectStoreUtil;
import io.mosip.kernel.core.exception.ExceptionUtils;
import io.mosip.kernel.core.logger.spi.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

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
    private int clientExecutionTimeout; // not directly used in v2 client

    @Value("${object.store.s3.use.account.as.bucketname:false}")
    private boolean useAccountAsBucketname;

    @Value("${object.store.s3.bucket-name-prefix:}")
    private String bucketNamePrefix;

    private int retry = 0;

    private List<String> existingBuckets = new ArrayList<>();

    /** Shared S3 client (replaces AmazonS3 connection) */
    private S3Client connection = null;

    private static final String SEPARATOR = "/";

    private static final String TAG_BACKWARD_COMPATIBILITY_ERROR =
            "Object-prefix is already an object, please choose a different object-prefix name";

    private static final String TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR =
            "Access Denied";

    /* =======================================================================
       CORE S3 CLIENT (AWS SDK v2) – same semantics as original
       ======================================================================= */
    /**
     * This method will return a singleton connection. It will verify connection
     * for the first time and will reuse same connection in subsequent calls.
     *
     * @param bucketName bucket to test connectivity against
     * @return S3Client
     */
    private S3Client getConnection(String bucketName) {
        if (connection != null) {
            return connection;
        }

        try {
            AwsBasicCredentials awsCredentials =
                    AwsBasicCredentials.create(accessKey, secretKey);

            ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder()
                    .maxConnections(maxConnection)
                    .connectionTimeout(Duration.ofMillis(connectionTimeout))
                    .socketTimeout(Duration.ofMillis(socketTimeout))
                    .tcpKeepAlive(true);

            S3Configuration s3Config = S3Configuration.builder()
                    .pathStyleAccessEnabled(true)
                    .build();

            S3ClientBuilder builder = S3Client.builder()
                    .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
                    .httpClientBuilder(httpClientBuilder)
                    .serviceConfiguration(s3Config);

            if (region != null && !"null".equalsIgnoreCase(region)) {
                builder = builder.region(Region.of(region));
            }

            if (url != null && !"null".equalsIgnoreCase(url)) {
                builder = builder.endpointOverride(URI.create(url));
            }

            connection = builder.build();

            // test connection once before returning it (similar to doesBucketExistV2)
            try {
                connection.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
            } catch (Exception e) {
                // headBucket can fail if bucket does not exist – that's fine for some flows,
                // we just log similar to original behavior.
                LOGGER.debug(SESSIONID, REGISTRATIONID,
                        "headBucket failed for " + bucketName + " (may not exist yet): " + e.getMessage());
            }

            retry = 0;
        } catch (Exception e) {
            if (retry >= maxRetry) {
                // reset the connection and retry count
                retry = 0;
                connection = null;
                LOGGER.error(SESSIONID, REGISTRATIONID,
                        "Maximum retry limit exceeded. Could not obtain connection for " + bucketName
                                + ". Retry count :" + retry,
                        ExceptionUtils.getStackTrace(e));
                throw new ObjectStoreAdapterException(
                        OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                        OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
            } else {
                connection = null;
                retry = retry + 1;
                LOGGER.error(SESSIONID, REGISTRATIONID,
                        "Exception occured while obtaining connection for " + bucketName
                                + ". Will try again. Retry count : " + retry,
                        ExceptionUtils.getStackTrace(e));
                return getConnection(bucketName);
            }
        }
        return connection;
    }

    private boolean doesBucketExists(String bucketName) {
        // use account as bucket name and bucket name is present in existing bucket list
        if (useAccountAsBucketname && existingBuckets.contains(bucketName)) {
            return true;
        }
        // use account as bucket name and bucket name is not present in existing bucket list
        else if (useAccountAsBucketname && !existingBuckets.contains(bucketName)) {
            boolean doesBucketExistsInObjectStore = false;
            try {
                getConnection(bucketName).headBucket(
                        HeadBucketRequest.builder().bucket(bucketName).build());
                doesBucketExistsInObjectStore = true;
            } catch (S3Exception e) {
                doesBucketExistsInObjectStore = false;
            }
            if (doesBucketExistsInObjectStore) {
                existingBuckets.add(bucketName);
            }
            return doesBucketExistsInObjectStore;
        } else {
            try {
                getConnection(bucketName).headBucket(
                        HeadBucketRequest.builder().bucket(bucketName).build());
                return true;
            } catch (S3Exception e) {
                return false;
            }
        }
    }

    private String addBucketPrefix(String bucketName) {
        if (bucketNamePrefix == null || bucketNamePrefix.isEmpty()) {
            return bucketName;
        }
        if (bucketName.startsWith(bucketNamePrefix)) {
            LOGGER.debug("Already bucketName with prefix is present" + bucketName);
            return bucketName;
        } else {
            String newName = bucketNamePrefix + bucketName;
            LOGGER.debug("Adding  Prefix to bucketName" + newName);
            return newName;
        }
    }

    /* =======================================================================
       OBJECT CRUD
       ======================================================================= */

    @Override
    public InputStream getObject(String account, String container, String source, String process, String objectName) {
        String finalObjectName = null;
        String bucketName = null;

        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
            bucketName = account;
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName = container;
        }

        bucketName = addBucketPrefix(bucketName);
        // As per AmazonS3 bucket naming rules,name contains only lower case letters
        bucketName = bucketName.toLowerCase();

        ResponseInputStream<?> s3Object = null;
        try {
            s3Object = getConnection(bucketName).getObject(
                    GetObjectRequest.builder()
                            .bucket(bucketName)
                            .key(finalObjectName)
                            .build());
            if (s3Object != null) {
                ByteArrayOutputStream temp = new ByteArrayOutputStream();
                IOUtils.copy(s3Object, temp);
                ByteArrayInputStream bis = new ByteArrayInputStream(temp.toByteArray());
                return bis;
            }
        } catch (Exception e) {
            connection = null;
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occured to getObject for : " + container,
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
                            "IO occured : " + container, ExceptionUtils.getStackTrace(e));
                }
            }
        }
        return null;
    }

    @Override
    public boolean exists(String account, String container, String source, String process, String objectName) {
        String finalObjectName = null;
        String bucketName = null;

        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
            bucketName = account;
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName = container;
        }

        bucketName = addBucketPrefix(bucketName);
        // As per AmazonS3 bucket naming rules,name contains only lower case letters
        bucketName = bucketName.toLowerCase();

        try {
            getConnection(bucketName).headObject(
                    HeadObjectRequest.builder()
                            .bucket(bucketName)
                            .key(finalObjectName)
                            .build());
            return true;
        } catch (S3Exception e) {
            return false;
        }
    }

    @Override
    public boolean putObject(String account, final String container, String source,
                             String process, String objectName, InputStream data) {
        String finalObjectName = null;
        String bucketName = null;

        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
            bucketName = account;
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName = container;
        }

        bucketName = addBucketPrefix(bucketName);
        // As per AmazonS3 bucket naming rules,name contains only lower case letters
        bucketName = bucketName.toLowerCase();

        S3Client client = getConnection(bucketName);
        if (!doesBucketExists(bucketName)) {
            client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
            if (useAccountAsBucketname) {
                existingBuckets.add(bucketName);
            }
        }

        try {
            // We must know content-length in v2, so we buffer once (similar to original getObject)
            ByteArrayOutputStream temp = new ByteArrayOutputStream();
            IOUtils.copy(data, temp);
            byte[] bytes = temp.toByteArray();

            client.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(finalObjectName)
                            .build(),
                    RequestBody.fromBytes(bytes));

            return true;
        } catch (Exception e) {
            connection = null;
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occured to putObject for : " + container,
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    /* =======================================================================
       METADATA
       ======================================================================= */

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process,
                                                 String objectName, Map<String, Object> metadata) {
        ResponseInputStream<?> s3Object = null;
        try {
            String finalObjectName = null;
            String bucketName = null;
            if (useAccountAsBucketname) {
                finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
                bucketName = account;
            } else {
                finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
                bucketName = container;
            }
            bucketName = addBucketPrefix(bucketName);
            // As per AmazonS3 bucket naming rules,name contains only lower case letters
            bucketName = bucketName.toLowerCase();

            // Fetch existing object + metadata
            s3Object = getConnection(bucketName).getObject(
                    GetObjectRequest.builder()
                            .bucket(bucketName)
                            .key(finalObjectName)
                            .build());

            // Merge user metadata
            Map<String, String> userMeta = new HashMap<>();
            HeadObjectResponse head =
                    getConnection(bucketName).headObject(
                            HeadObjectRequest.builder()
                                    .bucket(bucketName)
                                    .key(finalObjectName)
                                    .build());
            if (head.metadata() != null) {
                userMeta.putAll(head.metadata());
            }
            for (Entry<String, Object> m : metadata.entrySet()) {
                userMeta.put(m.getKey(), m.getValue() != null ? m.getValue().toString() : null);
            }

            // Read content fully (similar to original approach)
            ByteArrayOutputStream temp = new ByteArrayOutputStream();
            IOUtils.copy(s3Object, temp);
            byte[] bytes = temp.toByteArray();

            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(finalObjectName)
                    .metadata(userMeta)
                    .build();

            getConnection(bucketName).putObject(putObjectRequest, RequestBody.fromBytes(bytes));
            return metadata;
        } catch (Exception e) {
            connection = null;
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occured to addObjectMetaData for : " + container,
                    ExceptionUtils.getStackTrace(e));
            metadata = null;
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        } finally {
            if (s3Object != null) {
                try {
                    s3Object.close();
                } catch (IOException e) {
                    LOGGER.error(SESSIONID, REGISTRATIONID,
                            "IO occured : " + container, ExceptionUtils.getStackTrace(e));
                }
            }
        }
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process,
                                                 String objectName, String key, String value) {
        Map<String, Object> meta = new HashMap<>();
        meta.put(key, value);
        String finalObjectName = null;

        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
        }

        return addObjectMetaData(account, container, source, process, finalObjectName, meta);
    }

    @Override
    public Map<String, Object> getMetaData(String account, String container, String source, String process,
                                           String objectName) {
        ResponseInputStream<?> s3Object = null;
        try {
            String finalObjectName = null;
            String bucketName = null;
            if (useAccountAsBucketname) {
                finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
                bucketName = account;
            } else {
                finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
                bucketName = container;
            }
            bucketName = addBucketPrefix(bucketName);
            // As per AmazonS3 bucket naming rules,name contains only lower case letters
            bucketName = bucketName.toLowerCase();

            Map<String, Object> metaData = new HashMap<>();

            s3Object = getConnection(bucketName).getObject(
                    GetObjectRequest.builder()
                            .bucket(bucketName)
                            .key(finalObjectName)
                            .build());
            HeadObjectResponse objectMetadata = getConnection(bucketName).headObject(
                    HeadObjectRequest.builder()
                            .bucket(bucketName)
                            .key(finalObjectName)
                            .build());
            if (objectMetadata != null && objectMetadata.metadata() != null) {
                objectMetadata.metadata().forEach((k, v) -> metaData.put(k, v));
            }
            return metaData;
        } catch (Exception e) {
            connection = null;
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occured to getMetaData for : " + container,
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
                            "IO occured : " + container, ExceptionUtils.getStackTrace(e));
                }
            }
        }
    }

    @Override
    public Integer incMetadata(String account, String container, String source, String process,
                               String objectName, String metaDataKey) {
        Map<String, Object> metadata = getMetaData(account, container, source, process, objectName);
        if (metadata.get(metaDataKey) != null) {
            metadata.put(metaDataKey, Integer.valueOf(metadata.get(metaDataKey).toString()) + 1);
            addObjectMetaData(account, container, source, process, objectName, metadata);
            return Integer.valueOf(metadata.get(metaDataKey).toString());
        }
        return null;
    }

    @Override
    public Integer decMetadata(String account, String container, String source, String process,
                               String objectName, String metaDataKey) {
        Map<String, Object> metadata = getMetaData(account, container, source, process, objectName);
        if (metadata.get(metaDataKey) != null) {
            metadata.put(metaDataKey, Integer.valueOf(metadata.get(metaDataKey).toString()) - 1);
            addObjectMetaData(account, container, source, process, objectName, metadata);
            return Integer.valueOf(metadata.get(metaDataKey).toString());
        }
        return null;
    }

    @Override
    public boolean deleteObject(String account, String container, String source,
                                String process, String objectName) {
        String finalObjectName = null;
        String bucketName = null;
        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
            bucketName = account;
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName = container;
        }
        bucketName = addBucketPrefix(bucketName);
        // As per AmazonS3 bucket naming rules,name contains only lower case letters
        bucketName = bucketName.toLowerCase();

        getConnection(bucketName).deleteObject(
                DeleteObjectRequest.builder()
                        .bucket(bucketName)
                        .key(finalObjectName)
                        .build());
        return true;
    }

    /* =======================================================================
       removeContainer / pack – not supported (same as original)
       ======================================================================= */

    @Override
    public boolean removeContainer(String account, String container, String source, String process) {
        return false;
    }

    @Override
    public boolean pack(String account, String container, String source, String process, String refId) {
        return false;
    }

    /* =======================================================================
       LIST ALL OBJECTS
       ======================================================================= */

    public List<ObjectDto> getAllObjects(String account, String id) {

        List<S3Object> os = null;
        if (useAccountAsBucketname) {
            String searchPattern = id + SEPARATOR;
            account = addBucketPrefix(account);
            // As per AmazonS3 bucket naming rules,name contains only lower case letters
            account = account.toLowerCase();
            ListObjectsV2Response resp = getConnection(account).listObjectsV2(
                    ListObjectsV2Request.builder()
                            .bucket(account)
                            .prefix(searchPattern)
                            .build());
            os = resp.contents();
        } else {
            id = addBucketPrefix(id);
            // As per AmazonS3 bucket naming rules,name contains only lower case letters
            id = id.toLowerCase();
            ListObjectsV2Response resp = getConnection(id).listObjectsV2(
                    ListObjectsV2Request.builder()
                            .bucket(id)
                            .build());
            os = resp.contents();
        }

        if (os != null && os.size() > 0) {
            List<ObjectDto> objectDtos = new ArrayList<>();
            os.forEach(o -> {
                // ignore the Tag file
                String[] tempKeys = o.key().split("/");
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
                            objectDto = new ObjectDto(null, null, keys[0], java.util.Date.from(o.lastModified()));
                            break;
                        case 2:
                            objectDto = new ObjectDto(keys[0], null, keys[1], java.util.Date.from(o.lastModified()));
                            break;
                        case 3:
                            objectDto = new ObjectDto(keys[0], keys[1], keys[2], java.util.Date.from(o.lastModified()));
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

    /**
     * If account is used as bucket name then first element of array is the packet id.
     * This method removes packet id from array so that path is same irrespective of
     * useAccountAsBucketname is true or false
     *
     * @param useAccountAsBucketname flag
     * @param keys                   path segments
     */
    private String[] removeIdFromObjectPath(boolean useAccountAsBucketname, String[] keys) {
        return (useAccountAsBucketname && ArrayUtils.isNotEmpty(keys))
                ? (String[]) ArrayUtils.remove(keys, 0)
                : keys;
    }

    /* =======================================================================
       TAGS (same layout/behavior as original)
       ======================================================================= */

    @Override
    public Map<String, String> addTags(String account, String container, Map<String, String> tags) {
        String bucketName = null;
        String finalObjectName = null;
        try {
            if (useAccountAsBucketname) {
                bucketName = account;
                finalObjectName = ObjectStoreUtil.getName(container, null, TAGS_FILENAME);
            } else {
                bucketName = container;
                finalObjectName = TAGS_FILENAME;
            }
            bucketName = addBucketPrefix(bucketName);
            // As per AmazonS3 bucket naming rules,name contains only lower case letters
            bucketName = bucketName.toLowerCase();
            S3Client client = getConnection(bucketName);
            if (!doesBucketExists(bucketName)) {
                client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
                if (useAccountAsBucketname) {
                    existingBuckets.add(bucketName);
                }
            }

            for (Entry<String, String> entry : tags.entrySet()) {
                String tagName;
                InputStream data = IOUtils.toInputStream(entry.getValue(), StandardCharsets.UTF_8);
                tagName = ObjectStoreUtil.getName(finalObjectName, entry.getKey());
                try {
                    byte[] bytes = IOUtils.toByteArray(data);
                    client.putObject(
                            PutObjectRequest.builder()
                                    .bucket(bucketName)
                                    .key(tagName)
                                    .build(),
                            RequestBody.fromBytes(bytes));
                } catch (Exception e) {
                    String msg = e.getMessage() != null ? e.getMessage() : "";
                    // this check is introduced to support backward compatibility
                    if (e instanceof S3Exception &&
                            (msg.contains(TAG_BACKWARD_COMPATIBILITY_ERROR)
                                    || msg.contains(TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR))) {

                        boolean exists = false;
                        try {
                            client.headObject(
                                    HeadObjectRequest.builder()
                                            .bucket(bucketName)
                                            .key(finalObjectName)
                                            .build());
                            exists = true;
                        } catch (Exception ex) {
                            exists = false;
                        }

                        if (exists) {
                            client.deleteObject(
                                    DeleteObjectRequest.builder()
                                            .bucket(bucketName)
                                            .key(finalObjectName)
                                            .build());
                            addTags(account, container, tags);
                        } else {
                            connection = null;
                            LOGGER.error(SESSIONID, REGISTRATIONID,
                                    "Exception occured while addTags for : " + container,
                                    ExceptionUtils.getStackTrace(e));
                            throw new ObjectStoreAdapterException(
                                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
                        }
                    } else {
                        connection = null;
                        LOGGER.error(SESSIONID, REGISTRATIONID,
                                "Exception occured while addTags for : " + container,
                                ExceptionUtils.getStackTrace(e));
                        throw new ObjectStoreAdapterException(
                                OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                                OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
                    }
                }
            }
        } catch (Exception e) {
            connection = null;
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occured while addTags for : " + container,
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
            String bucketName = null;
            String finalObjectName = null;
            if (useAccountAsBucketname) {
                bucketName = account;
                finalObjectName = ObjectStoreUtil.getName(container, null, TAGS_FILENAME) + SEPARATOR;
            } else {
                bucketName = container;
                finalObjectName = TAGS_FILENAME + SEPARATOR;
            }
            bucketName = addBucketPrefix(bucketName);
            // As per AmazonS3 bucket naming rules,name contains only lower case letters
            bucketName = bucketName.toLowerCase();
            S3Client client = getConnection(bucketName);

            List<S3Object> objectSummary;
            if (useAccountAsBucketname) {
                ListObjectsV2Response resp = client.listObjectsV2(
                        ListObjectsV2Request.builder()
                                .bucket(bucketName)
                                .prefix(finalObjectName)
                                .build());
                objectSummary = resp.contents();
            } else {
                ListObjectsV2Response resp = client.listObjectsV2(
                        ListObjectsV2Request.builder()
                                .bucket(bucketName)
                                .build());
                objectSummary = resp.contents();
            }

            List<String> tagNames = new ArrayList<>();
            if (objectSummary != null && objectSummary.size() > 0) {
                objectSummary.forEach(o -> {
                    String[] keys = o.key().split("/");
                    if (ArrayUtils.isNotEmpty(keys)) {
                        if (useAccountAsBucketname) {
                            if (keys.length > 1 && keys[1] != null && keys[1].endsWith(TAGS_FILENAME) && keys.length > 2) {
                                tagNames.add(keys[2]);
                            }
                        } else {
                            if (keys.length > 0 && keys[0] != null && keys[0].endsWith(TAGS_FILENAME) && keys.length > 1) {
                                tagNames.add(keys[1]);
                            }
                        }
                    }
                });
            }

            for (String tagName : tagNames) {
                String value = client.getObjectAsBytes(
                        GetObjectRequest.builder()
                                .bucket(bucketName)
                                .key(finalObjectName + tagName)
                                .build()).asUtf8String();
                objectTags.put(tagName, value);
            }

            return objectTags;
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occured while getTags for : " + container,
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public void deleteTags(String account, String container, List<String> tags) {
        try {
            String bucketName = null;
            String finalObjectName = null;
            if (useAccountAsBucketname) {
                bucketName = account;
                finalObjectName = ObjectStoreUtil.getName(container, null, TAGS_FILENAME);
            } else {
                bucketName = container;
                finalObjectName = TAGS_FILENAME;
            }
            bucketName = addBucketPrefix(bucketName);
            // As per AmazonS3 bucket naming rules,name contains only lower case letters
            bucketName = bucketName.toLowerCase();
            S3Client client = getConnection(bucketName);
            if (!doesBucketExists(bucketName)) {
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
                                .build());
            }

        } catch (Exception e) {
            connection = null;
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occured while deleteTags for : " + container,
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }
}
