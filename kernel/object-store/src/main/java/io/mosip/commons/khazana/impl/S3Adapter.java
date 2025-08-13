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
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.type.TypeReference;
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

    @Value("${object.store.s3.accesskey:accesskey}")
    private String accessKey;
    @Value("${object.store.s3.secretkey:secretkey}")
    private String secretKey;
    @Value("${object.store.s3.url:null}")
    private String url;
    @Value("${object.store.s3.region:us-east-1}")
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
    @Value("${object.store.chunk.size:1048576}")
    private int chunkSize;

    private static final String SEPARATOR = "/";
    private static final String TAG_BACKWARD_COMPATIBILITY_ERROR = "Object-prefix is already an object";
    private static final String TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR = "Access Denied";

    private final Set<String> existingBuckets = ConcurrentHashMap.newKeySet();
    private volatile S3Client s3Client;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private S3Client getS3Client() {
        if (s3Client == null) {
            synchronized (this) {
                if (s3Client == null) {
                    s3Client = S3Client.builder()
                            .endpointOverride(url != null ? java.net.URI.create(url) : null)
                            .region(Region.of(region))
                            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                            .httpClientBuilder(ApacheHttpClient.builder()
                                    .maxConnections(maxConnection)
                                    .connectionTimeout(Duration.ofMillis(connectionTimeout))
                                    .socketTimeout(Duration.ofMillis(socketTimeout)))
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
            return retry(() -> {
                getS3Client().headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
                existingBuckets.add(bucketName);
                return true;
            }, "doesBucketExist");
        } catch (NoSuchBucketException e) {
            LOGGER.info(SESSIONID, REGISTRATIONID, "Bucket does not exist: " + bucketName, "");
            return false;
        }
    }

    private String addBucketPrefix(String bucketName) {
        if (bucketNamePrefix != null && !bucketNamePrefix.isEmpty() && !bucketName.startsWith(bucketNamePrefix)) {
            LOGGER.debug(SESSIONID, REGISTRATIONID, "Adding prefix to bucket: " + bucketNamePrefix + bucketName, "");
            return bucketNamePrefix + bucketName;
        }
        return bucketName;
    }

    @Override
    public InputStream getObject(String account, String container, String source, String process, String objectName) {
        String finalObjectName = useAccountAsBucketname ?
                ObjectStoreUtil.getName(container, source, process, objectName) :
                ObjectStoreUtil.getName(source, process, objectName);
        String bucketName = useAccountAsBucketname ? account : container;
        bucketName = addBucketPrefix(bucketName).toLowerCase();
        LOGGER.info(SESSIONID, REGISTRATIONID, "Getting object: " + finalObjectName + " from bucket: " + bucketName, "");
        try {
            final String finalBucketName = bucketName;
            return retry(() -> {
                S3Client client = getS3Client();
                GetObjectRequest req = GetObjectRequest.builder().bucket(finalBucketName).key(finalObjectName).build();
                return client.getObject(req);
            }, "getObject");
        } catch (NoSuchKeyException e) {
            LOGGER.warn(SESSIONID, REGISTRATIONID, "Object not found: " + finalObjectName, ExceptionUtils.getStackTrace(e));
            return null;
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Failed to get object: " + finalObjectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public boolean exists(String account, String container, String source, String process, String objectName) {
        String finalObjectName = useAccountAsBucketname ?
                ObjectStoreUtil.getName(container, source, process, objectName) :
                ObjectStoreUtil.getName(source, process, objectName);
        String bucketName = useAccountAsBucketname ? account : container;
        bucketName = addBucketPrefix(bucketName).toLowerCase();
        LOGGER.info(SESSIONID, REGISTRATIONID, "Checking existence of object: " + finalObjectName + " in bucket: " + bucketName, "");
        try {
            final String finalBucketName = bucketName;
            return retry(() -> {
                S3Client client = getS3Client();
                HeadObjectRequest req = HeadObjectRequest.builder().bucket(finalBucketName).key(finalObjectName).build();
                client.headObject(req);
                return true;
            }, "exists");
        } catch (NoSuchKeyException e) {
            LOGGER.info(SESSIONID, REGISTRATIONID, "Object does not exist: " + finalObjectName, "");
            return false;
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Failed to check object existence: " + finalObjectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public boolean putObject(String account, String container, String source, String process, String objectName, InputStream data) {
        String finalObjectName = useAccountAsBucketname ?
                ObjectStoreUtil.getName(container, source, process, objectName) :
                ObjectStoreUtil.getName(source, process, objectName);
        String bucketName = useAccountAsBucketname ? account : container;
        bucketName = addBucketPrefix(bucketName).toLowerCase();
        LOGGER.info(SESSIONID, REGISTRATIONID, "Putting object: " + finalObjectName + " to bucket: " + bucketName, "");
        S3Client client = getS3Client();
        if (!doesBucketExist(bucketName)) {
            LOGGER.info(SESSIONID, REGISTRATIONID, "Creating bucket: " + bucketName, "");
            client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
            existingBuckets.add(bucketName);
        }
        try {
            final String finalBucketName = bucketName;
            return retry(() -> {
                client.putObject(
                        PutObjectRequest.builder().bucket(finalBucketName).key(finalObjectName).build(),
                        RequestBody.fromInputStream(data, -1));
                LOGGER.info(SESSIONID, REGISTRATIONID, "Successfully put object: " + finalObjectName, "");
                return true;
            }, "putObject");
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Failed to put object: " + finalObjectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process, String objectName, Map<String, Object> metadata) {
        String finalObjectName = useAccountAsBucketname ?
                ObjectStoreUtil.getName(container, source, process, objectName) :
                ObjectStoreUtil.getName(source, process, objectName);
        String bucketName = useAccountAsBucketname ? account : container;
        bucketName = addBucketPrefix(bucketName).toLowerCase();
        LOGGER.info(SESSIONID, REGISTRATIONID, "Adding metadata to object: " + finalObjectName + " in bucket: " + bucketName, "");
        S3Client client = getS3Client();
        try {
            return retry(() -> {
                final String finalBucketName=objectName;
                HeadObjectResponse head = client.headObject(HeadObjectRequest.builder().bucket(finalBucketName).key(finalObjectName).build());
                Map<String, String> userMeta = new HashMap<>(head.metadata());
                metadata.forEach((k, v) -> userMeta.put(k, v != null ? v.toString() : null));
                CopyObjectRequest copyReq = CopyObjectRequest.builder()
                        .sourceBucket(finalBucketName)
                        .sourceKey(finalObjectName)
                        .destinationBucket(finalBucketName)
                        .destinationKey(finalObjectName)
                        .metadata(userMeta)
                        .metadataDirective(MetadataDirective.REPLACE)
                        .build();
                client.copyObject(copyReq);
                LOGGER.info(SESSIONID, REGISTRATIONID, "Successfully added metadata to object: " + finalObjectName, "");
                return metadata;
            }, "addObjectMetaData");
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Failed to add metadata to object: " + finalObjectName, ExceptionUtils.getStackTrace(e));
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
        String finalObjectName = useAccountAsBucketname ?
                ObjectStoreUtil.getName(container, source, process, objectName) :
                ObjectStoreUtil.getName(source, process, objectName);
        String bucketName = useAccountAsBucketname ? account : container;
        bucketName = addBucketPrefix(bucketName).toLowerCase();
        LOGGER.info(SESSIONID, REGISTRATIONID, "Getting metadata for object: " + finalObjectName + " in bucket: " + bucketName, "");
        try {
            final String finalBucketName = bucketName;
            return retry(() -> {
                S3Client client = getS3Client();
                HeadObjectResponse head = client.headObject(HeadObjectRequest.builder().bucket(finalBucketName).key(finalObjectName).build());
                Map<String, Object> metaData = new HashMap<>(head.metadata());
                LOGGER.info(SESSIONID, REGISTRATIONID, "Successfully retrieved metadata for object: " + finalObjectName, "");
                return metaData;
            }, "getMetaData");
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Failed to get metadata for object: " + finalObjectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public Integer incMetadata(String account, String container, String source, String process, String objectName, String metaDataKey) {
        Map<String, Object> metadata = getMetaData(account, container, source, process, objectName);
        if (metadata.get(metaDataKey) != null) {
            metadata.put(metaDataKey, Integer.parseInt(metadata.get(metaDataKey).toString()) + 1);
            addObjectMetaData(account, container, source, process, objectName, metadata);
            return Integer.parseInt(metadata.get(metaDataKey).toString());
        }
        return null;
    }

    @Override
    public Integer decMetadata(String account, String container, String source, String process, String objectName, String metaDataKey) {
        Map<String, Object> metadata = getMetaData(account, container, source, process, objectName);
        if (metadata.get(metaDataKey) != null) {
            metadata.put(metaDataKey, Integer.parseInt(metadata.get(metaDataKey).toString()) - 1);
            addObjectMetaData(account, container, source, process, objectName, metadata);
            return Integer.parseInt(metadata.get(metaDataKey).toString());
        }
        return null;
    }

    @Override
    public boolean deleteObject(String account, String container, String source, String process, String objectName) {
        String finalObjectName = useAccountAsBucketname ?
                ObjectStoreUtil.getName(container, source, process, objectName) :
                ObjectStoreUtil.getName(source, process, objectName);
        String bucketName = useAccountAsBucketname ? account : container;
        bucketName = addBucketPrefix(bucketName).toLowerCase();
        LOGGER.info(SESSIONID, REGISTRATIONID, "Deleting object: " + finalObjectName + " from bucket: " + bucketName, "");
        try {
            final String finalBucketName = bucketName;
            return retry(() -> {
                getS3Client().deleteObject(DeleteObjectRequest.builder().bucket(finalBucketName).key(finalObjectName).build());
                LOGGER.info(SESSIONID, REGISTRATIONID, "Successfully deleted object: " + finalObjectName, "");
                return true;
            }, "deleteObject");
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Failed to delete object: " + finalObjectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public boolean removeContainer(String account, String container, String source, String process) {
        String bucketName = useAccountAsBucketname ? addBucketPrefix(account).toLowerCase() : addBucketPrefix(container).toLowerCase();
        LOGGER.info(SESSIONID, REGISTRATIONID, "Removing bucket: " + bucketName, "");
        try {
            return retry(() -> {
                getS3Client().deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build());
                existingBuckets.remove(bucketName);
                LOGGER.info(SESSIONID, REGISTRATIONID, "Successfully removed bucket: " + bucketName, "");
                return true;
            }, "removeContainer");
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Failed to remove bucket: " + bucketName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public boolean pack(String account, String container, String source, String process, String refId) {
        String bucketName = useAccountAsBucketname ? addBucketPrefix(account).toLowerCase() : addBucketPrefix(container).toLowerCase();
        String zipName = ObjectStoreUtil.getName(source, process, refId + ".zip");
        LOGGER.info(SESSIONID, REGISTRATIONID, "Packing objects into: " + zipName + " in bucket: " + bucketName, "");
        try {
            List<ObjectDto> objects = getAllObjects(account, container);
            if (objects.isEmpty()) {
                LOGGER.info(SESSIONID, REGISTRATIONID, "No objects to pack for: " + refId, "");
                return false;
            }
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ZipOutputStream zipOut = new ZipOutputStream(baos)) {
                for (ObjectDto dto : objects) {
                    String objectName = ObjectStoreUtil.getName(dto.getSource(), dto.getProcess(), dto.getObjectName());
                    try (InputStream is = getObject(account, container, dto.getSource(), dto.getProcess(), dto.getObjectName())) {
                        if (is != null) {
                            zipOut.putNextEntry(new ZipEntry(objectName));
                            byte[] buffer = new byte[chunkSize];
                            int bytesRead;
                            while ((bytesRead = is.read(buffer)) != -1) {
                                zipOut.write(buffer, 0, bytesRead);
                            }
                            zipOut.closeEntry();
                        }
                    }
                }
            }
            try (InputStream zipStream = new ByteArrayInputStream(baos.toByteArray())) {
                return putObject(account, container, source, process, refId + ".zip", zipStream);
            }
        } catch (IOException e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Failed to pack objects into: " + zipName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public List<ObjectDto> getAllObjects(String account, String container) {
        String bucketName = useAccountAsBucketname ? addBucketPrefix(account).toLowerCase() : addBucketPrefix(container).toLowerCase();
        LOGGER.info(SESSIONID, REGISTRATIONID, "Listing all objects in bucket: " + bucketName, "");
        List<ObjectDto> objects = new ArrayList<>();
        try {
            S3Client client = getS3Client();
            String continuationToken = null;
            do {
                final String finalContinuationToken = continuationToken;
                ListObjectsV2Request request = ListObjectsV2Request.builder()
                        .bucket(bucketName)
                        .continuationToken(finalContinuationToken)
                        .build();
                ListObjectsV2Response response = retry(() -> client.listObjectsV2(request), "getAllObjects");
                if (response != null) {
                    response.contents().forEach(s3Object -> {
                        if (!s3Object.key().endsWith(TAGS_FILENAME)) {
                            String key = s3Object.key();
                            String[] parts = key.split(SEPARATOR, 3);
                            String source = parts.length > 0 ? parts[0] : null;
                            String process = parts.length > 1 ? parts[1] : null;
                            String objectName = parts.length > 2 ? parts[2] : key;
                            objects.add(new ObjectDto(source, process, objectName, Date.from(s3Object.lastModified())));
                        }
                    });
                    continuationToken = response.isTruncated() ? response.nextContinuationToken() : null;
                }
            } while (continuationToken != null);
            LOGGER.info(SESSIONID, REGISTRATIONID, "Successfully listed " + objects.size() + " objects in bucket: " + bucketName, "");
            return objects;
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Failed to list objects in bucket: " + bucketName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public Map<String, String> addTags(String account, String container, Map<String, String> tags) {
        String bucketName = useAccountAsBucketname ? addBucketPrefix(account).toLowerCase() : addBucketPrefix(container).toLowerCase();
        String finalObjectName = TAGS_FILENAME;
        LOGGER.info(SESSIONID, REGISTRATIONID, "Adding tags to: " + finalObjectName + " in bucket: " + bucketName, "");
        S3Client client = getS3Client();
        if (!doesBucketExist(bucketName)) {
            client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
            existingBuckets.add(bucketName);
        }
        try {
            Map<String, String> existingTags = getTags(account, container);
            existingTags.putAll(tags);
            byte[] tagsBytes = objectMapper.writeValueAsBytes(existingTags);
            try (InputStream tagsStream = new ByteArrayInputStream(tagsBytes)) {
                return retry(() -> {
                    client.putObject(
                            PutObjectRequest.builder().bucket(bucketName).key(finalObjectName).build(),
                            RequestBody.fromInputStream(tagsStream, tagsBytes.length));
                    LOGGER.info(SESSIONID, REGISTRATIONID, "Successfully added tags to: " + finalObjectName, "");
                    return existingTags;
                }, "addTags");
            }
        } catch (IOException e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Failed to add tags to: " + finalObjectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public Map<String, String> getTags(String account, String container) {
        String bucketName = useAccountAsBucketname ? addBucketPrefix(account).toLowerCase() : addBucketPrefix(container).toLowerCase();
        String finalObjectName = TAGS_FILENAME;
        LOGGER.info(SESSIONID, REGISTRATIONID, "Getting tags for: " + finalObjectName + " in bucket: " + bucketName, "");
        try {
            return retry(() -> {
                S3Client client = getS3Client();
                GetObjectRequest req = GetObjectRequest.builder().bucket(bucketName).key(finalObjectName).build();
                try (ResponseInputStream<GetObjectResponse> s3Stream = client.getObject(req)) {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    byte[] buffer = new byte[chunkSize];
                    int bytesRead;
                    while ((bytesRead = s3Stream.read(buffer)) != -1) {
                        baos.write(buffer, 0, bytesRead);
                    }
                    Map<String, String> tags = objectMapper.readValue(baos.toByteArray(), new TypeReference<Map<String, String>>(){});
                    LOGGER.info(SESSIONID, REGISTRATIONID, "Successfully retrieved tags for: " + finalObjectName, "");
                    return tags;
                }
            }, "getTags");
        } catch (NoSuchKeyException e) {
            LOGGER.info(SESSIONID, REGISTRATIONID, "No tags found for: " + finalObjectName, "");
            return new HashMap<>();
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Failed to get tags for: " + finalObjectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public void deleteTags(String account, String container, List<String> tags) {
        String bucketName = useAccountAsBucketname ? addBucketPrefix(account).toLowerCase() : addBucketPrefix(container).toLowerCase();
        String finalObjectName = TAGS_FILENAME;
        LOGGER.info(SESSIONID, REGISTRATIONID, "Deleting tags from: " + finalObjectName + " in bucket: " + bucketName, "");
        S3Client client = getS3Client();
        try {
            Map<String, String> existingTags = getTags(account, container);
            tags.forEach(existingTags::remove);
            if (existingTags.isEmpty()) {
                retry(() -> {
                    client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(finalObjectName).build());
                    LOGGER.info(SESSIONID, REGISTRATIONID, "Successfully deleted all tags from: " + finalObjectName, "");
                    return true;
                }, "deleteTags");
            } else {
                byte[] tagsBytes = objectMapper.writeValueAsBytes(existingTags);
                try (InputStream tagsStream = new ByteArrayInputStream(tagsBytes)) {
                    retry(() -> {
                        client.putObject(
                                PutObjectRequest.builder().bucket(bucketName).key(finalObjectName).build(),
                                RequestBody.fromInputStream(tagsStream, tagsBytes.length));
                        LOGGER.info(SESSIONID, REGISTRATIONID, "Successfully updated tags for: " + finalObjectName, "");
                        return true;
                    }, "deleteTags");
                }
            }
        } catch (IOException e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Failed to delete tags from: " + finalObjectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Failed to delete tags from: " + finalObjectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    private <T> T retry(Callable<T> operation, String operationName) throws ObjectStoreAdapterException {
        for (int attempt = 1; attempt <= maxRetry; attempt++) {
            try {
                long startTime = System.currentTimeMillis();
                T result = operation.call();
                LOGGER.debug(SESSIONID, REGISTRATIONID, "Operation {} completed in {} ms", operationName, System.currentTimeMillis() - startTime);
                return result;
            } catch (Exception e) {
                LOGGER.warn(SESSIONID, REGISTRATIONID, "Retry attempt {}/{} for operation {} failed: {}", attempt, maxRetry, operationName, ExceptionUtils.getStackTrace(e));
                if (attempt == maxRetry) {
                    throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Operation " + operationName + " failed after " + maxRetry + " attempts", e);
                }
                try {
                    Thread.sleep(1000L * attempt); // Exponential backoff
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Interrupted during retry for " + operationName, ie);
                }
            }
        }
        return null;
    }
}