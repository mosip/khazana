package io.mosip.commons.khazana.impl;

import static io.mosip.commons.khazana.config.LoggerConfiguration.REGISTRATIONID;
import static io.mosip.commons.khazana.config.LoggerConfiguration.SESSIONID;
import static io.mosip.commons.khazana.constant.KhazanaConstant.TAGS_FILENAME;
import static io.mosip.commons.khazana.constant.KhazanaErrorCodes.OBJECT_STORE_NOT_ACCESSIBLE;

import java.io.ByteArrayInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;
import io.mosip.commons.khazana.config.LoggerConfiguration;
import io.mosip.commons.khazana.dto.ObjectDto;
import io.mosip.commons.khazana.exception.ObjectStoreAdapterException;
import io.mosip.commons.khazana.spi.ObjectStoreAdapter;
import io.mosip.commons.khazana.util.ObjectStoreUtil;
import io.mosip.kernel.core.exception.ExceptionUtils;
import io.mosip.kernel.core.logger.spi.Logger;

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
    private int clientExecutionTimeout;
    @Value("${object.store.s3.use.account.as.bucketname:false}")
    private boolean useAccountAsBucketname;
    @Value("${object.store.s3.bucket-name-prefix:}")
    private String bucketNamePrefix;

    @Value("${object.store.chunk.size:1048576}") // 1MB chunks for streaming
    private int chunkSize;

    private static final String SEPARATOR = "/";
    private static final String TAG_BACKWARD_COMPATIBILITY_ERROR = "Object-prefix is already an object, please choose a different object-prefix name";
    private static final String TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR = "Access Denied";

    private final Set<String> existingBuckets = ConcurrentHashMap.newKeySet();
    private volatile S3Client s3Client;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private S3Client getS3Client() {
        if (s3Client == null) {
            synchronized (this) {
                if (s3Client == null) {
                    s3Client = S3Client.builder()
                            .endpointOverride(java.net.URI.create(url))
                            .region(Region.of(region))
                            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                            .httpClientBuilder(ApacheHttpClient.builder()
                                    .maxConnections(maxConnection)
                                    .connectionTimeout(Duration.ofMillis(connectionTimeout))
                                    .socketTimeout(Duration.ofMillis(socketTimeout))
                            )
                            .serviceConfiguration(S3Configuration.builder()
                                    .checksumValidationEnabled(false)
                                    .chunkedEncodingEnabled(true)
                                    .build())
                            .build();
                }
            }
        }
        return s3Client;
    }

    private boolean doesBucketExist(String bucketName) {
        if (existingBuckets.contains(bucketName)) return true;
        try {
            getS3Client().headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
            existingBuckets.add(bucketName);
            return true;
        } catch (NoSuchBucketException e) {
            return false;
        }
    }

    private String addBucketPrefix(String bucketName) {
        if (bucketNamePrefix != null && !bucketNamePrefix.isEmpty() && !bucketName.startsWith(bucketNamePrefix)) {
            LOGGER.debug("Adding Prefix to bucketName " + bucketNamePrefix + bucketName);
            return bucketNamePrefix + bucketName;
        }
        return bucketName;
    }

    @Override
    public InputStream getObject(String account, String container, String source, String process, String objectName) {
        String finalObjectName;
        String bucketName;
        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
            bucketName = account;
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName = container;
        }
        bucketName = addBucketPrefix(bucketName).toLowerCase();
        try {
            S3Client client = getS3Client();
            GetObjectRequest req = GetObjectRequest.builder().bucket(bucketName).key(finalObjectName).build();
            return client.getObject(req);
        } catch (NoSuchKeyException e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Object not found: " + finalObjectName, ExceptionUtils.getStackTrace(e));
            return null;
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception during getObject for: " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public boolean exists(String account, String container, String source, String process, String objectName) {
        String finalObjectName;
        String bucketName;
        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
            bucketName = account;
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName = container;
        }
        bucketName = addBucketPrefix(bucketName).toLowerCase();
        try {
            S3Client client = getS3Client();
            HeadObjectRequest req = HeadObjectRequest.builder().bucket(bucketName).key(finalObjectName).build();
            client.headObject(req);
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception during exists check for: " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public boolean putObject(String account, String container, String source, String process, String objectName, InputStream data) {
        String finalObjectName;
        String bucketName;
        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
            bucketName = account;
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName = container;
        }
        bucketName = addBucketPrefix(bucketName).toLowerCase();

        S3Client client = getS3Client();

        if (!doesBucketExist(bucketName)) {
            client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
            existingBuckets.add(bucketName);
        }

        try {
            final String finalBucketName = bucketName;
            return retry(() -> {
                client.putObject(
                        PutObjectRequest.builder().bucket(finalBucketName).key(finalObjectName).build(),
                        RequestBody.fromInputStream(data, -1)
                );
                return true;
            }, "putObject");
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception during putObject for: " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process, String objectName, Map<String, Object> metadata) {
        String finalObjectName;
        String bucketName;
        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
            bucketName = account;
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName = container;
        }
        bucketName = addBucketPrefix(bucketName).toLowerCase();
        S3Client client = getS3Client();
        try {
            HeadObjectResponse head = client.headObject(HeadObjectRequest.builder().bucket(bucketName).key(finalObjectName).build());
            Map<String, String> userMeta = new HashMap<>(head.metadata());
            metadata.forEach((k, v) -> userMeta.put(k, v != null ? v.toString() : null));
            CopyObjectRequest copyReq = CopyObjectRequest.builder()
                    .sourceBucket(bucketName)
                    .sourceKey(finalObjectName)
                    .destinationBucket(bucketName)
                    .destinationKey(finalObjectName)
                    .metadata(userMeta)
                    .metadataDirective(MetadataDirective.REPLACE)
                    .build();
            client.copyObject(copyReq);
            return metadata;
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,"Exception occurred to addObjectMetaData for: " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process, String objectName, String key, String value) {
        Map<String, Object> meta = new HashMap<>();
        meta.put(key, value);
        return addObjectMetaData(account, container, source, process, objectName, meta);
    }

    @Override
    public Map<String, Object> getMetaData(String account, String container, String source, String process, String objectName) {
        String finalObjectName;
        String bucketName;
        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
            bucketName = account;
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName = container;
        }
        bucketName = addBucketPrefix(bucketName).toLowerCase();
        try {
            S3Client client = getS3Client();
            HeadObjectResponse head = client.headObject(HeadObjectRequest.builder().bucket(bucketName).key(finalObjectName).build());
            Map<String, Object> metaData = new HashMap<>(head.metadata());
            return metaData;
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occurred to getMetaData for: " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public Integer incMetadata(String account, String container, String source, String process, String objectName, String metaDataKey) {
        Map<String, Object> metadata = getMetaData(account, container, source, process, objectName);
        if (metadata.get(metaDataKey) != null) {
            Integer updatedValue = Integer.valueOf(metadata.get(metaDataKey).toString()) + 1;
            metadata.put(metaDataKey, updatedValue);
            addObjectMetaData(account, container, source, process, objectName, metadata);
            return updatedValue;
        }
        return null;
    }

    @Override
    public Integer decMetadata(String account, String container, String source, String process, String objectName, String metaDataKey) {
        Map<String, Object> metadata = getMetaData(account, container, source, process, objectName);
        if (metadata.get(metaDataKey) != null) {
            Integer updatedValue = Integer.valueOf(metadata.get(metaDataKey).toString()) - 1;
            metadata.put(metaDataKey, updatedValue);
            addObjectMetaData(account, container, source, process, objectName, metadata);
            return updatedValue;
        }
        return null;
    }

    @Override
    public boolean deleteObject(String account, String container, String source, String process, String objectName) {
        String finalObjectName;
        String bucketName;
        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
            bucketName = account;
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName = container;
        }
        bucketName = addBucketPrefix(bucketName).toLowerCase();
        try {
            final String finalBucketName = bucketName;
            return retry(() -> {
                getS3Client().deleteObject(DeleteObjectRequest.builder().bucket(finalBucketName).key(finalObjectName).build());
                return true;
            }, "deleteObject");
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occurred to deleteObject for: " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public boolean removeContainer(String account, String container, String source, String process) {
        String bucketName = useAccountAsBucketname ? addBucketPrefix(account).toLowerCase() : addBucketPrefix(container).toLowerCase();
        try {
            return retry(() -> {
                getS3Client().deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build());
                existingBuckets.remove(bucketName);
                return true;
            }, "removeContainer");
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occurred to removeContainer for: " + container, ExceptionUtils.getStackTrace(e));
            return false;
        }
    }

    @Override
    public boolean pack(String account, String container, String source, String process, String refId) {
        // Packing logic if required (not implemented)
        return false;
    }

    @Override
    public List<ObjectDto> getAllObjects(String account, String container) {
        String bucketName = useAccountAsBucketname ? addBucketPrefix(account).toLowerCase()
                : addBucketPrefix(container).toLowerCase();
        List<ObjectDto> objects = new ArrayList<>();
        try {
            S3Client client = getS3Client();
            String continuationToken = null;
            do {
                final String continuationTokenCopy = continuationToken;
                ListObjectsV2Request request = ListObjectsV2Request.builder()
                        .bucket(bucketName)
                        .continuationToken(continuationTokenCopy)
                        .build();
                ListObjectsV2Response response = retry(() -> client.listObjectsV2(request), "getAllObjects");
                if (response == null) break;
                response.contents().forEach(s3Object -> {
                    if (!s3Object.key().endsWith(TAGS_FILENAME)) {
                        String key = s3Object.key();
                        // Split key into parts; adjust logic if your naming convention differs
                        String[] parts = key.split(SEPARATOR, 3);
                        String source = parts.length > 0 ? parts[0] : null;
                        String process = parts.length > 1 ? parts[1] : null;
                        String objectName = parts.length > 2 ? parts[2] : key;
                        objects.add(new ObjectDto(source, process, objectName,
                                Date.from(s3Object.lastModified())));
                    }
                });
                continuationToken = response.isTruncated() ? response.nextContinuationToken() : null;
            } while (continuationToken != null);
            return objects;
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception during getAllObjects for: " + container,
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

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
            bucketName = addBucketPrefix(bucketName).toLowerCase();
            S3Client client = getS3Client();
            if (!doesBucketExist(bucketName)) {
                client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
                existingBuckets.add(bucketName);
            }
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                String tagName = ObjectStoreUtil.getName(finalObjectName, entry.getKey());
                InputStream data = IOUtils.toInputStream(entry.getValue(), StandardCharsets.UTF_8);
                try {
                    client.putObject(PutObjectRequest.builder().bucket(bucketName).key(tagName).build(),
                            RequestBody.fromInputStream(data, entry.getValue().getBytes(StandardCharsets.UTF_8).length));
                } catch (S3Exception e) {
                    if (e.getMessage().contains(TAG_BACKWARD_COMPATIBILITY_ERROR)
                            || e.getMessage().contains(TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR)) {
                        if (client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).prefix(finalObjectName).build()).keyCount() > 0) {
                            client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(finalObjectName).build());
                            addTags(account, container, tags);
                        } else {
                            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured while addTags for : " + container, ExceptionUtils.getStackTrace(e));
                            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
                        }
                    } else {
                        LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured while addTags for : " + container, ExceptionUtils.getStackTrace(e));
                        throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                                OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured while addTags for : " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
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
            bucketName = addBucketPrefix(bucketName).toLowerCase();
            S3Client client = getS3Client();

            List<S3Object> objectSummary;
            if (useAccountAsBucketname)
                objectSummary = client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).prefix(finalObjectName).build()).contents();
            else
                objectSummary = client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).build()).contents();

            List<String> tagNames = new ArrayList<>();
            if (objectSummary != null && !objectSummary.isEmpty()) {
                objectSummary.forEach(o -> {
                    String[] keys = o.key().split("/");
                    if (ArrayUtils.isNotEmpty(keys)) {
                        if (useAccountAsBucketname) {
                            if (keys.length > 1 && keys[1] != null && keys[1].endsWith(TAGS_FILENAME))
                                tagNames.add(keys[2]);
                        } else {
                            if (keys.length > 0 && keys[0] != null && keys[0].endsWith(TAGS_FILENAME))
                                tagNames.add(keys[1]);
                        }
                    }
                });
            }
            for (String tagName : tagNames) {
                objectTags.put(tagName, client.getObjectAsBytes(GetObjectRequest.builder().bucket(bucketName).key(finalObjectName + tagName).build()).asUtf8String());
            }
            return objectTags;

        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured while getTags for : " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
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
            bucketName = addBucketPrefix(bucketName).toLowerCase();
            S3Client client = getS3Client();
            if (!doesBucketExist(bucketName)) {
                client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
                existingBuckets.add(bucketName);
            }
            for (String tag : tags) {
                String tagName = ObjectStoreUtil.getName(finalObjectName, tag);
                client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(tagName).build());
            }
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured while deleteTags for : " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }
    private <T> T retry(Callable<T> operation, String operationName) throws ObjectStoreAdapterException {
        for (int attempt = 1; attempt <= maxRetry; attempt++) {
            try {
                return operation.call();
            } catch (Exception e) {
                LOGGER.warn(SESSIONID, REGISTRATIONID, "Error during {} attempt {} for: {}", operationName, attempt, operationName, ExceptionUtils.getStackTrace(e));
                if (attempt == maxRetry) {
                    throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
                }
                try {
                    Thread.sleep(1000L * attempt); // Exponential backoff
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Interrupted during retry", ie);
                }
            }
        }
        return null;
    }
}