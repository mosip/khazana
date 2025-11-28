package io.mosip.commons.khazana.impl;

import io.mosip.commons.khazana.config.LoggerConfiguration;
import io.mosip.commons.khazana.dto.ObjectDto;
import io.mosip.commons.khazana.exception.ObjectStoreAdapterException;
import io.mosip.commons.khazana.spi.ObjectStoreAdapter;
import io.mosip.commons.khazana.util.ObjectStoreUtil;
import io.mosip.kernel.core.exception.ExceptionUtils;
import io.mosip.kernel.core.logger.spi.Logger;
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;
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

    private static final Logger LOGGER = LoggerConfiguration.logConfig(S3Adapter.class);

    @Value("${object.store.s3.accesskey:accesskey:accesskey}")
    private String accessKey;

    @Value("${object.store.s3.secretkey:secretkey:secretkey}")
    private String secretKey;

    @Value("${object.store.s3.url:null}")
    private String url;

    @Value("${object.store.s3.region:null}")
    private String region;

    @Value("${object.store.s3.readlimit:10000000}")
    private int readlimit; // kept for config compatibility, not used directly in v2

    @Value("${object.store.connection.max.retry:20}")
    private int maxRetry;  // kept for config compatibility (S3Client already retries internally)

    @Value("${object.store.max.connection:200}")
    private int maxConnection;

    @Value("${object.store.connection.timeout:5000}")
    private int connectionTimeout;

    @Value("${object.store.socket.timeout:10000}")
    private int socketTimeout;

    @Value("${object.store.client.execution.timeout:15000}")
    private int clientExecutionTimeout; // not used explicitly; HTTP layer + SDK handle timeouts

    @Value("${object.store.s3.use.account.as.bucketname:false}")
    private boolean useAccountAsBucketname;

    @Value("${object.store.s3.bucket-name-prefix:}")
    private String bucketNamePrefix;

    private static final String SEPARATOR = "/";

    private static final String TAG_BACKWARD_COMPATIBILITY_ERROR =
            "Object-prefix is already an object, please choose a different object-prefix name";
    private static final String TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR = "Access Denied";

    private final List<String> existingBuckets = new ArrayList<>();

    private volatile S3Client s3Client;

    /* ==========================================================================================
       CORE CLIENT (AWS SDK v2, path-style, MinIO compatible)
       ========================================================================================== */

    private S3Client getClient() {
        if (s3Client != null) return s3Client;

        synchronized (this) {
            if (s3Client != null) return s3Client;

            AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);

            ApacheHttpClient.Builder http = ApacheHttpClient.builder()
                    .maxConnections(maxConnection)
                    .connectionTimeout(Duration.ofMillis(connectionTimeout))
                    .socketTimeout(Duration.ofMillis(socketTimeout))
                    .tcpKeepAlive(true);

            S3Configuration s3cfg = S3Configuration.builder()
                    .pathStyleAccessEnabled(true) // IMPORTANT for MinIO / on-prem S3
                    .build();

            S3ClientBuilder builder = S3Client.builder()
                    .credentialsProvider(StaticCredentialsProvider.create(credentials))
                    .httpClientBuilder(http)
                    .serviceConfiguration(s3cfg)
                    .region(Region.of(region));

            if (url != null && !"null".equalsIgnoreCase(url) && !url.isBlank()) {
                builder = builder.endpointOverride(URI.create(url));
            }

            s3Client = builder.build();
            return s3Client;
        }
    }

    /* ==========================================================================================
       BUCKET + KEY HELPERS (preserve original MOSIP layout)
       ========================================================================================== */

    private String addBucketPrefix(String bucketName) {
        if (bucketNamePrefix == null || bucketNamePrefix.isEmpty()) {
            return bucketName;
        }
        if (bucketName.startsWith(bucketNamePrefix)) {
            LOGGER.debug("Already bucketName with prefix is present " + bucketName);
            return bucketName;
        }
        String result = bucketNamePrefix + bucketName;
        LOGGER.debug("Adding Prefix to bucketName " + result);
        return result;
    }

    private String resolveBucket(String account, String container) {
        String bucketName = useAccountAsBucketname ? account : container;
        bucketName = addBucketPrefix(bucketName);
        // As per S3 bucket naming rules: lowercase only
        return bucketName.toLowerCase(Locale.ROOT);
    }

    private String resolveObjectKey(String container, String source, String process, String objectName) {
        if (useAccountAsBucketname) {
            // ORIGINAL: ObjectStoreUtil.getName(container, source, process, objectName)
            return ObjectStoreUtil.getName(container, source, process, objectName);
        } else {
            // ORIGINAL: ObjectStoreUtil.getName(source, process, objectName)
            return ObjectStoreUtil.getName(source, process, objectName);
        }
    }

    private boolean doesBucketExists(String bucketName) {
        // Preserve original semantics, but use HEAD Bucket (no ListAllMyBuckets)
        if (useAccountAsBucketname && existingBuckets.contains(bucketName)) {
            return true;
        }
        try {
            getClient().headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
            if (useAccountAsBucketname && !existingBuckets.contains(bucketName)) {
                existingBuckets.add(bucketName);
            }
            return true;
        } catch (NoSuchBucketException e) {
            return false;
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return false;
            }
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception while checking bucket existence for: " + bucketName,
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    private void ensureBucket(String bucketName) {
        try {
            if (!doesBucketExists(bucketName)) {
                getClient().createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
                if (useAccountAsBucketname && !existingBuckets.contains(bucketName)) {
                    existingBuckets.add(bucketName);
                }
            }
        } catch (S3Exception e) {
            String code = (e.awsErrorDetails() != null) ? e.awsErrorDetails().errorCode() : null;
            if ("BucketAlreadyOwnedByYou".equals(code) || "BucketAlreadyExists".equals(code)) {
                // benign race / already created
                return;
            }
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception while creating bucket: " + bucketName,
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    /* ==========================================================================================
       BASIC OBJECT OPERATIONS
       ========================================================================================== */

    @Override
    public InputStream getObject(String account, String container,
                                 String source, String process, String objectName) {
        String bucketName = resolveBucket(account, container);
        String key = resolveObjectKey(container, source, process, objectName);
        ResponseInputStream<GetObjectResponse> s3Object = null;
        try {
            s3Object = getClient().getObject(
                    GetObjectRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .build()
            );

            // Preserve original behavior: load into memory and return ByteArrayInputStream
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            s3Object.transferTo(baos);
            return new ByteArrayInputStream(baos.toByteArray());

        } catch (Exception e) {
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
                } catch (Exception ignore) {
                }
            }
        }
    }

    @Override
    public boolean exists(String account, String container,
                          String source, String process, String objectName) {
        String bucketName = resolveBucket(account, container);
        String key = resolveObjectKey(container, source, process, objectName);
        try {
            getClient().headObject(
                    HeadObjectRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .build()
            );
            return true;
        } catch (NoSuchKeyException | NoSuchBucketException e) {
            return false;
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return false;
            }
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occured to check exists for : " + container,
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public boolean putObject(String account, String container,
                             String source, String process,
                             String objectName, InputStream data) {

        String bucketName = resolveBucket(account, container);
        String key = resolveObjectKey(container, source, process, objectName);

        ensureBucket(bucketName);

        try {
            // AWS SDK v2 needs a content-length for streaming to be optimal.
            // For compatibility with v1 (where length was often unknown),
            // we buffer to memory only when required by S3Client.
            byte[] bytes = data.readAllBytes();
            RequestBody body = RequestBody.fromBytes(bytes);

            getClient().putObject(
                    PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .build(),
                    body
            );
            return true;

        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occured to putObject for : " + container,
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public boolean deleteObject(String account, String container,
                                String source, String process, String objectName) {
        String bucketName = resolveBucket(account, container);
        String key = resolveObjectKey(container, source, process, objectName);

        try {
            getClient().deleteObject(
                    DeleteObjectRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .build()
            );
            return true;
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occured to deleteObject for : " + container,
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    /* ==========================================================================================
       METADATA (user metadata, same semantics as v1)
       ========================================================================================== */

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source,
                                                 String process, String objectName,
                                                 Map<String, Object> metadata) {
        String bucketName = resolveBucket(account, container);
        String key = resolveObjectKey(container, source, process, objectName);

        try {
            // Get existing user metadata
            HeadObjectResponse head = getClient().headObject(
                    HeadObjectRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .build()
            );

            Map<String, String> mergedMeta = new HashMap<>();
            if (head.metadata() != null) {
                mergedMeta.putAll(head.metadata());
            }
            if (metadata != null) {
                for (Map.Entry<String, Object> e : metadata.entrySet()) {
                    mergedMeta.put(e.getKey(), e.getValue() != null ? e.getValue().toString() : null);
                }
            }

            // Copy object in-place with REPLACED metadata
            getClient().copyObject(
                    CopyObjectRequest.builder()
                            .sourceBucket(bucketName)
                            .sourceKey(key)
                            .destinationBucket(bucketName)
                            .destinationKey(key)
                            .metadata(mergedMeta)
                            .metadataDirective(MetadataDirective.REPLACE)
                            .build()
            );

            return metadata;
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occured to addObjectMetaData for : " + container,
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source,
                                                 String process, String objectName,
                                                 String key, String value) {
        Map<String, Object> meta = new HashMap<>();
        meta.put(key, value);
        // In v1 they recomputed finalObjectName then passed it back;
        // here we keep it logically correct: objectName is still the actual objectName.
        return addObjectMetaData(account, container, source, process, objectName, meta);
    }

    @Override
    public Map<String, Object> getMetaData(String account, String container,
                                           String source, String process,
                                           String objectName) {
        String bucketName = resolveBucket(account, container);
        String key = resolveObjectKey(container, source, process, objectName);

        try {
            HeadObjectResponse head = getClient().headObject(
                    HeadObjectRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .build()
            );

            Map<String, Object> metaData = new HashMap<>();
            if (head.metadata() != null) {
                metaData.putAll(head.metadata());
            }
            return metaData;
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occured to getMetaData for : " + container,
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
            int newValue = Integer.parseInt(metadata.get(metaDataKey).toString()) + 1;
            metadata.put(metaDataKey, newValue);
            addObjectMetaData(account, container, source, process, objectName, metadata);
            return newValue;
        }
        return null;
    }

    @Override
    public Integer decMetadata(String account, String container, String source,
                               String process, String objectName, String metaDataKey) {
        Map<String, Object> metadata = getMetaData(account, container, source, process, objectName);
        if (metadata.get(metaDataKey) != null) {
            int newValue = Integer.parseInt(metadata.get(metaDataKey).toString()) - 1;
            metadata.put(metaDataKey, newValue);
            addObjectMetaData(account, container, source, process, objectName, metadata);
            return newValue;
        }
        return null;
    }

    /* ==========================================================================================
       REMOVE CONTAINER / PACK (not supported, same as v1)
       ========================================================================================== */

    @Override
    public boolean removeContainer(String account, String container, String source, String process) {
        return false;
    }

    @Override
    public boolean pack(String account, String container, String source, String process, String refId) {
        return false;
    }

    /* ==========================================================================================
       TAGS (same structure as original v1: <id>/tags/<tagName> etc.)
       ========================================================================================== */

    @Override
    public Map<String, String> addTags(String account, String container, Map<String, String> tags) {
        String bucketName;
        String finalObjectName; // base path for tags (directory)

        if (useAccountAsBucketname) {
            bucketName = resolveBucket(account, container); // account + prefix
            finalObjectName = ObjectStoreUtil.getName(container, null, TAGS_FILENAME);
        } else {
            bucketName = resolveBucket(account, container); // container + prefix
            finalObjectName = TAGS_FILENAME;
        }

        ensureBucket(bucketName);

        try {
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                String tagName = ObjectStoreUtil.getName(finalObjectName, entry.getKey());
                try {
                    getClient().putObject(
                            PutObjectRequest.builder()
                                    .bucket(bucketName)
                                    .key(tagName)
                                    .build(),
                            RequestBody.fromString(entry.getValue())
                    );
                } catch (S3Exception e) {
                    String msg = e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage();
                    if (msg != null && (msg.contains(TAG_BACKWARD_COMPATIBILITY_ERROR)
                            || msg.contains(TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR))) {

                        // Backward compatibility handling
                        if (exists(account, container, null, null, finalObjectName)) {
                            // delete old tag file and retry
                            getClient().deleteObject(
                                    DeleteObjectRequest.builder()
                                            .bucket(bucketName)
                                            .key(finalObjectName)
                                            .build()
                            );
                            return addTags(account, container, tags);
                        } else {
                            LOGGER.error(SESSIONID, REGISTRATIONID,
                                    "Exception occured while addTags for : " + container,
                                    ExceptionUtils.getStackTrace(e));
                            throw new ObjectStoreAdapterException(
                                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
                        }
                    } else {
                        LOGGER.error(SESSIONID, REGISTRATIONID,
                                "Exception occured while addTags for : " + container,
                                ExceptionUtils.getStackTrace(e));
                        throw new ObjectStoreAdapterException(
                                OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                                OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
                    }
                }
            }
            return tags;
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occured while addTags for : " + container,
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public Map<String, String> getTags(String account, String container) {
        Map<String, String> objectTags = new HashMap<>();

        try {
            String bucketName;
            String finalObjectNamePrefix;

            if (useAccountAsBucketname) {
                bucketName = resolveBucket(account, container);
                finalObjectNamePrefix = ObjectStoreUtil.getName(container, null, TAGS_FILENAME) + SEPARATOR;
            } else {
                bucketName = resolveBucket(account, container);
                finalObjectNamePrefix = TAGS_FILENAME + SEPARATOR;
            }

            ListObjectsV2Request.Builder listReqBuilder = ListObjectsV2Request.builder()
                    .bucket(bucketName);

            if (useAccountAsBucketname) {
                listReqBuilder = listReqBuilder.prefix(finalObjectNamePrefix);
            } else {
                // original code listed all objects and filtered; we can still list all
                // for exact backward behavior, don't set prefix here
            }

            ListObjectsV2Response listResp = getClient().listObjectsV2(listReqBuilder.build());
            List<S3Object> objectSummary = listResp.contents();

            List<String> tagNames = new ArrayList<>();
            if (objectSummary != null && !objectSummary.isEmpty()) {
                objectSummary.forEach(o -> {
                    String[] keys = o.key().split(SEPARATOR);
                    if (ArrayUtils.isNotEmpty(keys)) {
                        if (useAccountAsBucketname) {
                            // key format: container/tags/tagName
                            if (keys.length >= 3 && keys[1] != null && keys[1].endsWith(TAGS_FILENAME)) {
                                tagNames.add(keys[2]);
                            }
                        } else {
                            // key format: tags/tagName
                            if (keys.length >= 2 && keys[0] != null && keys[0].endsWith(TAGS_FILENAME)) {
                                tagNames.add(keys[1]);
                            }
                        }
                    }
                });
            }

            for (String tagName : tagNames) {
                String fullKey = finalObjectNamePrefix + tagName;
                String value = getClient().getObjectAsBytes(
                        GetObjectRequest.builder()
                                .bucket(bucketName)
                                .key(fullKey)
                                .build()
                ).asUtf8String();
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
            String bucketName;
            String finalObjectName;

            if (useAccountAsBucketname) {
                bucketName = resolveBucket(account, container);
                finalObjectName = ObjectStoreUtil.getName(container, null, TAGS_FILENAME);
            } else {
                bucketName = resolveBucket(account, container);
                finalObjectName = TAGS_FILENAME;
            }

            ensureBucket(bucketName);

            for (String tag : tags) {
                String tagKey = ObjectStoreUtil.getName(finalObjectName, tag);
                getClient().deleteObject(
                        DeleteObjectRequest.builder()
                                .bucket(bucketName)
                                .key(tagKey)
                                .build()
                );
            }

        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occured while deleteTags for : " + container,
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    /* ==========================================================================================
       LIST ALL OBJECTS (getAllObjects) – same behavior as original
       ========================================================================================== */

    @Override
    public List<ObjectDto> getAllObjects(String account, String id) {

        List<S3Object> os;

        try {
            if (useAccountAsBucketname) {
                String bucketName = resolveBucket(account, id); // account + prefix
                String searchPattern = id + SEPARATOR;

                ListObjectsV2Response resp = getClient().listObjectsV2(
                        ListObjectsV2Request.builder()
                                .bucket(bucketName)
                                .prefix(searchPattern)
                                .build()
                );
                os = resp.contents();
            } else {
                // bucket = id
                String bucketName = resolveBucket(account, id); // id with prefix
                ListObjectsV2Response resp = getClient().listObjectsV2(
                        ListObjectsV2Request.builder()
                                .bucket(bucketName)
                                .build()
                );
                os = resp.contents();
            }

            if (os == null || os.isEmpty()) {
                return null;
            }

            List<ObjectDto> objectDtos = new ArrayList<>();
            os.forEach(o -> {
                String key = o.key();
                String[] tempKeys = key.split(SEPARATOR);

                // ignore the Tag file paths
                if (useAccountAsBucketname) {
                    // key example: id/tags/tagName or id/source/process/name
                    if (tempKeys.length > 1 && tempKeys[1] != null && tempKeys[1].endsWith(TAGS_FILENAME)) {
                        return; // skip tags
                    }
                } else {
                    // key example: tags/tagName or source/process/name
                    if (tempKeys.length > 0 && tempKeys[0] != null && tempKeys[0].endsWith(TAGS_FILENAME)) {
                        return; // skip tags
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

            return objectDtos.isEmpty() ? null : objectDtos;

        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occured while getAllObjects for : " + id,
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    /**
     * If account is used as bucket name then first element of array is the packet id.
     * This method removes packet id from array so that path is same irrespective
     * of useAccountAsBucketname being true or false.
     */
    private String[] removeIdFromObjectPath(boolean useAccountAsBucketname, String[] keys) {
        return (useAccountAsBucketname && ArrayUtils.isNotEmpty(keys))
                ? (String[]) ArrayUtils.remove(keys, 0)
                : keys;
    }
}
