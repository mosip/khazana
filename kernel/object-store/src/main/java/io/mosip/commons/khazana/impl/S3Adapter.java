package io.mosip.commons.khazana.impl;

import static io.mosip.commons.khazana.config.LoggerConfiguration.REGISTRATIONID;
import static io.mosip.commons.khazana.config.LoggerConfiguration.SESSIONID;
import static io.mosip.commons.khazana.constant.KhazanaConstant.TAGS_FILENAME;
import static io.mosip.commons.khazana.constant.KhazanaErrorCodes.OBJECT_STORE_NOT_ACCESSIBLE;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
        return Map.of();
    }

    @Override
    public Map<String, String> getTags(String account, String container) {
        return Map.of();
    }

    @Override
    public void deleteTags(String account, String container, List<String> tags) {

    }

    public Map<String, String> addTags(String account, String container, String source, String process, String objectName, Map<String, String> tags) {
        String finalObjectName;
        String bucketName;
        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName) + SEPARATOR + TAGS_FILENAME;
            bucketName = account;
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName) + SEPARATOR + TAGS_FILENAME;
            bucketName = container;
        }
        bucketName = addBucketPrefix(bucketName).toLowerCase();

        S3Client client = getS3Client();
        if (!doesBucketExist(bucketName)) {
            client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
            existingBuckets.add(bucketName);
        }

        try {
            Map<String, String> existingTags = getTags(account, container, source, process, objectName);
            existingTags.putAll(tags);
            byte[] tagsBytes = objectMapper.writeValueAsString(existingTags).getBytes();
            try (InputStream tagsStream = new ByteArrayInputStream(tagsBytes)) {
                final String finalBucketName=bucketName;
                retry(() -> {
                    client.putObject(
                            PutObjectRequest.builder().bucket(finalBucketName).key(finalObjectName).build(),
                            RequestBody.fromInputStream(tagsStream, tagsBytes.length)
                    );
                    return true;
                }, "addTags");
            }
            return existingTags;
        } catch (IOException e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "IOException during addTags for: " + finalObjectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        } catch (Exception e) {
            if (e.getMessage() != null && (e.getMessage().contains(TAG_BACKWARD_COMPATIBILITY_ERROR) || e.getMessage().contains(TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR))) {
                LOGGER.error(SESSIONID, REGISTRATIONID, "Exception during addTags for: " + finalObjectName, ExceptionUtils.getStackTrace(e));
                return tags;
            }
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception during addTags for: " + finalObjectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    public Map<String, String> getTags(String account, String container, String source, String process, String objectName) {
        String finalObjectName;
        String bucketName;
        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName) + SEPARATOR + TAGS_FILENAME;
            bucketName = account;
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName) + SEPARATOR + TAGS_FILENAME;
            bucketName = container;
        }
        bucketName = addBucketPrefix(bucketName).toLowerCase();
        try {
            S3Client client = getS3Client();
            GetObjectRequest req = GetObjectRequest.builder().bucket(bucketName).key(finalObjectName).build();
            try (ResponseInputStream<GetObjectResponse> s3Stream = retry(() -> client.getObject(req), "getTags")) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                byte[] buffer = new byte[chunkSize];
                int bytesRead;
                while ((bytesRead = s3Stream.read(buffer)) != -1) {
                    baos.write(buffer, 0, bytesRead);
                }
                return objectMapper.readValue(baos.toByteArray(), Map.class);
            }
        } catch (NoSuchKeyException e) {
            return new HashMap<>();
        } catch (IOException e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "IOException during getTags for: " + finalObjectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception during getTags for: " + finalObjectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    public boolean deleteTags(String account, String container, String source, String process, String objectName) {
        String finalObjectName;
        String bucketName;
        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName) + SEPARATOR + TAGS_FILENAME;
            bucketName = account;
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName) + SEPARATOR + TAGS_FILENAME;
            bucketName = container;
        }
        bucketName = addBucketPrefix(bucketName).toLowerCase();
        try {
            final String finalBucketName=bucketName;
            return retry(() -> {
                getS3Client().deleteObject(DeleteObjectRequest.builder().bucket(finalBucketName).key(finalObjectName).build());
                return true;
            }, "deleteTags");
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception during deleteTags for: " + finalObjectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
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