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
                    if (url == null || url.isEmpty()) {
                        LOGGER.error(SESSIONID, REGISTRATIONID, "S3 URL is null or empty", "");
                        throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "S3 URL configuration is missing");
                    }
                    s3Client = S3Client.builder()
                            .endpointOverride(java.net.URI.create(url))
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
                    LOGGER.info(SESSIONID, REGISTRATIONID, "Initialized S3Client", "");
                }
            }
        }
        return s3Client;
    }

    private boolean doesBucketExist(String bucketName) {
        if (bucketName == null || bucketName.isEmpty()) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Bucket name is null or empty", "");
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Bucket name is invalid");
        }
        if (existingBuckets.contains(bucketName)) {
            LOGGER.debug(SESSIONID, REGISTRATIONID, "Bucket cached as existing: " + bucketName, "");
            return true;
        }
        LOGGER.info(SESSIONID, REGISTRATIONID, "Checking bucket existence: " + bucketName, "");
        try {
            return retry(() -> {
                getS3Client().headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
                existingBuckets.add(bucketName);
                LOGGER.info(SESSIONID, REGISTRATIONID, "Bucket exists: " + bucketName, "");
                return true;
            }, "doesBucketExist", false);
        } catch (NoSuchBucketException e) {
            LOGGER.info(SESSIONID, REGISTRATIONID, "Bucket does not exist: " + bucketName, "");
            return false;
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Failed to check bucket existence: " + bucketName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Failed to check bucket existence: " + e.getMessage(), e);
        }
    }

    private String addBucketPrefix(String bucketName) {
        if (bucketName == null || bucketName.isEmpty()) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Bucket name is null or empty", "");
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Bucket name is invalid");
        }
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
        if (finalObjectName == null || finalObjectName.isEmpty()) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Object name is null or empty", "");
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Object name is invalid");
        }
        try {
            final String finalBucketName = bucketName;
            return retry(() -> {
                S3Client client = getS3Client();
                GetObjectRequest req = GetObjectRequest.builder().bucket(finalBucketName).key(finalObjectName).build();
                InputStream result = client.getObject(req);
                LOGGER.info(SESSIONID, REGISTRATIONID, "Successfully retrieved object: " + finalObjectName, "");
                return result;
            }, "getObject", false);
        } catch (NoSuchKeyException e) {
            LOGGER.info(SESSIONID, REGISTRATIONID, "Object not found: " + finalObjectName, "");
            return null;
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Failed to get object: " + finalObjectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Failed to get object: " + e.getMessage(), e);
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
        if (finalObjectName == null || finalObjectName.isEmpty()) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Object name is null or empty", "");
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Object name is invalid");
        }
        try {
            final String finalBucketName = bucketName;
            return retry(() -> {
                S3Client client = getS3Client();
                HeadObjectRequest req = HeadObjectRequest.builder().bucket(finalBucketName).key(finalObjectName).build();
                client.headObject(req);
                LOGGER.info(SESSIONID, REGISTRATIONID, "Object exists: " + finalObjectName, "");
                return true;
            }, "exists", false);
        } catch (NoSuchKeyException e) {
            LOGGER.info(SESSIONID, REGISTRATIONID, "Object does not exist: " + finalObjectName, "");
            return false;
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Failed to check object existence: " + finalObjectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Failed to check object existence: " + e.getMessage(), e);
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

        if (finalObjectName == null || finalObjectName.isEmpty()) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Object name is null or empty", "");
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Object name is invalid");
        }
        if (data == null) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "InputStream is null for object: " + finalObjectName, "");
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "InputStream is null");
        }

        S3Client client = getS3Client();
        if (!doesBucketExist(bucketName)) {
            LOGGER.info(SESSIONID, REGISTRATIONID, "Creating bucket: " + bucketName, "");
            client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
            existingBuckets.add(bucketName);
        }

        try {
            // Buffer InputStream to determine content length
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte[] buffer = new byte[chunkSize];
            int bytesRead;
            while ((bytesRead = data.read(buffer)) != -1) {
                baos.write(buffer, 0, bytesRead);
            }
            byte[] dataBytes = baos.toByteArray();
            if (dataBytes.length == 0) {
                LOGGER.error(SESSIONID, REGISTRATIONID, "InputStream is empty for object: " + finalObjectName, "");
                throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "InputStream is empty");
            }

            try (InputStream bufferedData = new ByteArrayInputStream(dataBytes)) {

                final String finalBucketName = bucketName;
                return retry(() -> {
                    client.putObject(
                            PutObjectRequest.builder().bucket(finalBucketName).key(finalObjectName).build(),
                            RequestBody.fromInputStream(bufferedData, dataBytes.length));
                    LOGGER.info(SESSIONID, REGISTRATIONID, "Successfully put object: " + finalObjectName, "");
                    return true;
                }, "putObject", true);
            }
        } catch (IllegalArgumentException e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Invalid content length for object: " + finalObjectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Invalid content length: " + e.getMessage(), e);
        } catch (IOException e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "IO error while processing object: " + finalObjectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "IO error: " + e.getMessage(), e);
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Failed to put object: " + finalObjectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Failed to put object: " + e.getMessage(), e);
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
        if (metadata == null || metadata.isEmpty()) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Metadata is null or empty for object: " + finalObjectName, "");
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Metadata is invalid");
        }
        try {
            final String finalBucketName = bucketName;

            return retry(() -> {
                S3Client client = getS3Client();
                HeadObjectResponse head = client.headObject(HeadObjectRequest.builder().bucket(finalBucketName).key(finalObjectName).build());
                Map<String, String> existingMetadata = new HashMap<>(head.metadata());
                metadata.forEach((k, v) -> existingMetadata.put(k, v != null ? v.toString() : null));
                CopyObjectRequest copyReq = CopyObjectRequest.builder()
                        .sourceBucket(finalBucketName)
                        .sourceKey(finalObjectName)
                        .destinationBucket(finalBucketName)
                        .destinationKey(finalObjectName)
                        .metadata(existingMetadata)
                        .metadataDirective(MetadataDirective.REPLACE)
                        .build();
                client.copyObject(copyReq);
                LOGGER.info(SESSIONID, REGISTRATIONID, "Successfully added metadata to object: " + finalObjectName, "");
                return new HashMap<>(existingMetadata);
            }, "addObjectMetaData", false);
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Failed to add metadata to object: " + finalObjectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Failed to add metadata: " + e.getMessage(), e);
        }
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process, String objectName, String key, String value) {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put(key, value);
        return addObjectMetaData(account, container, source, process, objectName, metadata);
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
                Map<String, Object> metadata = new HashMap<>(head.metadata());
                LOGGER.info(SESSIONID, REGISTRATIONID, "Successfully retrieved metadata for object: " + finalObjectName, "");
                return metadata;
            }, "getMetaData", false);
        } catch (NoSuchKeyException e) {
            LOGGER.info(SESSIONID, REGISTRATIONID, "Object not found for metadata: " + finalObjectName, "");
            return new HashMap<>();
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Failed to get metadata for object: " + finalObjectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Failed to get metadata: " + e.getMessage(), e);
        }
    }

    @Override
    public Integer incMetadata(String account, String container, String source, String process, String objectName, String metaDataKey) {
        String finalObjectName = useAccountAsBucketname ?
                ObjectStoreUtil.getName(container, source, process, objectName) :
                ObjectStoreUtil.getName(source, process, objectName);
        String bucketName = useAccountAsBucketname ? account : container;
        bucketName = addBucketPrefix(bucketName).toLowerCase();
        LOGGER.info(SESSIONID, REGISTRATIONID, "Incrementing metadata key: " + metaDataKey + " for object: " + finalObjectName, "");
        try {
            Map<String, Object> metadata = getMetaData(account, container, source, process, objectName);
            if (metadata.get(metaDataKey) != null) {
                Integer updatedValue = Integer.parseInt(metadata.get(metaDataKey).toString()) + 1;
                metadata.put(metaDataKey, updatedValue);
                addObjectMetaData(account, container, source, process, objectName, metadata);
                LOGGER.info(SESSIONID, REGISTRATIONID, "Successfully incremented metadata key: " + metaDataKey + " to " + updatedValue, "");
                return updatedValue;
            }
            LOGGER.info(SESSIONID, REGISTRATIONID, "Metadata key not found: " + metaDataKey, "");
            return null;
        } catch (NumberFormatException e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Invalid metadata value for key: " + metaDataKey, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Invalid metadata value: " + e.getMessage(), e);
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Failed to increment metadata for key: " + metaDataKey, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Failed to increment metadata: " + e.getMessage(), e);
        }
    }

    @Override
    public Integer decMetadata(String account, String container, String source, String process, String objectName, String metaDataKey) {
        String finalObjectName = useAccountAsBucketname ?
                ObjectStoreUtil.getName(container, source, process, objectName) :
                ObjectStoreUtil.getName(source, process, objectName);
        String bucketName = useAccountAsBucketname ? account : container;
        bucketName = addBucketPrefix(bucketName).toLowerCase();
        LOGGER.info(SESSIONID, REGISTRATIONID, "Decrementing metadata key: " + metaDataKey + " for object: " + finalObjectName, "");
        try {
            Map<String, Object> metadata = getMetaData(account, container, source, process, objectName);
            if (metadata.get(metaDataKey) != null) {
                Integer updatedValue = Integer.parseInt(metadata.get(metaDataKey).toString()) - 1;
                metadata.put(metaDataKey, updatedValue);
                addObjectMetaData(account, container, source, process, objectName, metadata);
                LOGGER.info(SESSIONID, REGISTRATIONID, "Successfully decremented metadata key: " + metaDataKey + " to " + updatedValue, "");
                return updatedValue;
            }
            LOGGER.info(SESSIONID, REGISTRATIONID, "Metadata key not found: " + metaDataKey, "");
            return null;
        } catch (NumberFormatException e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Invalid metadata value for key: " + metaDataKey, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Invalid metadata value: " + e.getMessage(), e);
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Failed to decrement metadata for key: " + metaDataKey, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Failed to decrement metadata: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean deleteObject(String account, String container, String source, String process, String objectName) {
        String finalObjectName = useAccountAsBucketname ?
                ObjectStoreUtil.getName(container, source, process, objectName) :
                ObjectStoreUtil.getName(source, process, objectName);
        String bucketName = useAccountAsBucketname ? account : container;
        bucketName = addBucketPrefix(bucketName).toLowerCase();
        LOGGER.info(SESSIONID, REGISTRATIONID, "Deleting object: " + finalObjectName + " from bucket: " + bucketName, "");
        if (finalObjectName == null || finalObjectName.isEmpty()) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Object name is null or empty", "");
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Object name is invalid");
        }
        try {
            final String finalBucketName = bucketName;
            return retry(() -> {
                S3Client client = getS3Client();
                client.deleteObject(DeleteObjectRequest.builder().bucket(finalBucketName).key(finalObjectName).build());
                LOGGER.info(SESSIONID, REGISTRATIONID, "Successfully deleted object: " + finalObjectName, "");
                return true;
            }, "deleteObject", false);
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Failed to delete object: " + finalObjectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Failed to delete object: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean removeContainer(String account, String container, String source, String process) {
        String bucketName = useAccountAsBucketname ? addBucketPrefix(account).toLowerCase() : addBucketPrefix(container).toLowerCase();
        LOGGER.info(SESSIONID, REGISTRATIONID, "Removing bucket: " + bucketName, "");
        try {
            return retry(() -> {
                S3Client client = getS3Client();
                client.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build());
                existingBuckets.remove(bucketName);
                LOGGER.info(SESSIONID, REGISTRATIONID, "Successfully removed bucket: " + bucketName, "");
                return true;
            }, "removeContainer", false);
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Failed to remove bucket: " + bucketName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Failed to remove bucket: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean pack(String account, String container, String source, String process, String refId) {
        String bucketName = useAccountAsBucketname ? addBucketPrefix(account).toLowerCase() : addBucketPrefix(container).toLowerCase();
        LOGGER.info(SESSIONID, REGISTRATIONID, "Packing objects for refId: " + refId + " in bucket: " + bucketName, "");
        try {
            List<ObjectDto> objects = getAllObjects(account, container);
            if (objects.isEmpty()) {
                LOGGER.info(SESSIONID, REGISTRATIONID, "No objects found to pack in bucket: " + bucketName, "");
                return false;
            }

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ZipOutputStream zipOut = new ZipOutputStream(baos)) {
                S3Client client = getS3Client();
                for (ObjectDto obj : objects) {
                    String objectName = useAccountAsBucketname ?
                            ObjectStoreUtil.getName(container, obj.getSource(), obj.getProcess(), obj.getObjectName()) :
                            ObjectStoreUtil.getName(obj.getSource(), obj.getProcess(), obj.getObjectName());
                    // Skip tags file
                    if (objectName.endsWith(TAGS_FILENAME)) {
                        continue;
                    }
                    // Filter by refId if provided
                    if (refId != null && !objectName.contains(refId)) {
                        continue;
                    }
                    try (InputStream objectStream = getObject(account, container, obj.getSource(), obj.getProcess(), obj.getObjectName())) {
                        if (objectStream == null) {
                            LOGGER.warn(SESSIONID, REGISTRATIONID, "Object not found during packing: " + objectName, "");
                            continue;
                        }
                        zipOut.putNextEntry(new ZipEntry(objectName));
                        byte[] buffer = new byte[chunkSize];
                        int bytesRead;
                        while ((bytesRead = objectStream.read(buffer)) != -1) {
                            zipOut.write(buffer, 0, bytesRead);
                        }
                        zipOut.closeEntry();
                        LOGGER.debug(SESSIONID, REGISTRATIONID, "Added object to zip: " + objectName, "");
                    }
                }
                zipOut.finish();
            }
            byte[] zipBytes = baos.toByteArray();
            if (zipBytes.length == 0) {
                LOGGER.info(SESSIONID, REGISTRATIONID, "No valid objects packed for refId: " + refId, "");
                return false;
            }
            LOGGER.info(SESSIONID, REGISTRATIONID, "Successfully packed objects for refId: " + refId, "");
            return true;
        } catch (IOException e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "IO error during packing for refId: " + refId, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Failed to pack objects: " + e.getMessage(), e);
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Failed to pack objects for refId: " + refId, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Failed to pack objects: " + e.getMessage(), e);
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
                ListObjectsV2Request request = ListObjectsV2Request.builder()
                        .bucket(bucketName)
                        .continuationToken(continuationToken)
                        .build();
                ListObjectsV2Response response = retry(() -> client.listObjectsV2(request), "getAllObjects", false);
                if (response != null && response.contents() != null) {
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
                } else {
                    continuationToken = null;
                }
            } while (continuationToken != null);
            LOGGER.info(SESSIONID, REGISTRATIONID, "Successfully listed " + objects.size() + " objects in bucket: " + bucketName, "");
            return objects;
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Failed to list objects in bucket: " + bucketName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Failed to list objects: " + e.getMessage(), e);
        }
    }

    @Override
    public Map<String, String> addTags(String account, String container, Map<String, String> tags) {
        String bucketName = useAccountAsBucketname ? addBucketPrefix(account).toLowerCase() : addBucketPrefix(container).toLowerCase();
        String finalObjectName = TAGS_FILENAME;
        LOGGER.info(SESSIONID, REGISTRATIONID, "Adding tags to: " + finalObjectName + " in bucket: " + bucketName, "");
        if (tags == null || tags.isEmpty()) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Tags are null or empty for: " + finalObjectName, "");
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Tags are invalid");
        }
        S3Client client = getS3Client();
        if (!doesBucketExist(bucketName)) {
            LOGGER.info(SESSIONID, REGISTRATIONID, "Creating bucket: " + bucketName, "");
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
                    return new HashMap<>(existingTags);
                }, "addTags", false);
            }
        } catch (IOException e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "IO error during addTags for: " + finalObjectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Failed to add tags: " + e.getMessage(), e);
        } catch (Exception e) {
            if (e.getMessage() != null && (e.getMessage().contains(TAG_BACKWARD_COMPATIBILITY_ERROR) || e.getMessage().contains(TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR))) {
                LOGGER.warn(SESSIONID, REGISTRATIONID, "Backward compatibility error during addTags: " + finalObjectName, ExceptionUtils.getStackTrace(e));
                return tags;
            }
            LOGGER.error(SESSIONID, REGISTRATIONID, "Failed to add tags to: " + finalObjectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Failed to add tags: " + e.getMessage(), e);
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
            }, "getTags", false);
        } catch (NoSuchKeyException e) {
            LOGGER.info(SESSIONID, REGISTRATIONID, "No tags found for: " + finalObjectName, "");
            return new HashMap<>();
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Failed to get tags for: " + finalObjectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Failed to get tags: " + e.getMessage(), e);
        }
    }

    @Override
    public void deleteTags(String account, String container, List<String> tags) {
        String bucketName = useAccountAsBucketname ? addBucketPrefix(account).toLowerCase() : addBucketPrefix(container).toLowerCase();
        String finalObjectName = TAGS_FILENAME;
        LOGGER.info(SESSIONID, REGISTRATIONID, "Deleting tags from: " + finalObjectName + " in bucket: " + bucketName, "");
        if (tags == null || tags.isEmpty()) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Tags list is null or empty for: " + finalObjectName, "");
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Tags list is invalid");
        }
        try {
            Map<String, String> existingTags = getTags(account, container);
            tags.forEach(existingTags::remove);
            S3Client client = getS3Client();
            if (existingTags.isEmpty()) {
                retry(() -> {
                    client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(finalObjectName).build());
                    LOGGER.info(SESSIONID, REGISTRATIONID, "Successfully deleted all tags from: " + finalObjectName, "");
                    return true;
                }, "deleteTags", false);
            } else {
                byte[] tagsBytes = objectMapper.writeValueAsBytes(existingTags);
                try (InputStream tagsStream = new ByteArrayInputStream(tagsBytes)) {
                    retry(() -> {
                        client.putObject(
                                PutObjectRequest.builder().bucket(bucketName).key(finalObjectName).build(),
                                RequestBody.fromInputStream(tagsStream, tagsBytes.length));
                        LOGGER.info(SESSIONID, REGISTRATIONID, "Successfully updated tags for: " + finalObjectName, "");
                        return true;
                    }, "deleteTags", false);
                }
            }
        } catch (IOException e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "IO error during deleteTags for: " + finalObjectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Failed to delete tags: " + e.getMessage(), e);
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Failed to delete tags from: " + finalObjectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Failed to delete tags: " + e.getMessage(), e);
        }
    }

    private <T> T retry(Callable<T> operation, String operationName, boolean skipNonTransient) throws ObjectStoreAdapterException {
        for (int attempt = 1; attempt <= maxRetry; attempt++) {
            try {
                long startTime = System.currentTimeMillis();
                T result = operation.call();
                LOGGER.debug(SESSIONID, REGISTRATIONID, "Operation {} completed in {} ms", operationName, System.currentTimeMillis() - startTime);
                return result;
            } catch (IllegalArgumentException e) {
                if (skipNonTransient) {
                    LOGGER.error(SESSIONID, REGISTRATIONID, "Non-transient error in {}: {}", operationName, ExceptionUtils.getStackTrace(e));
                    throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Non-transient error in " + operationName + ": " + e.getMessage(), e);
                }
                LOGGER.warn(SESSIONID, REGISTRATIONID, "Retry attempt {}/{} for {} failed: {}", attempt, maxRetry, operationName, ExceptionUtils.getStackTrace(e));
                if (attempt == maxRetry) {
                    throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Operation " + operationName + " failed after " + maxRetry + " attempts", e);
                }
            } catch (Exception e) {
                LOGGER.warn(SESSIONID, REGISTRATIONID, "Retry attempt {}/{} for {} failed: {}", attempt, maxRetry, operationName, ExceptionUtils.getStackTrace(e));
                if (attempt == maxRetry) {
                    throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Operation " + operationName + " failed after " + maxRetry + " attempts", e);
                }
            }
            try {
                Thread.sleep(1000L * attempt); // Exponential backoff
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOGGER.error(SESSIONID, REGISTRATIONID, "Interrupted during retry for " + operationName, ExceptionUtils.getStackTrace(ie));
                throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), "Interrupted during retry for " + operationName, ie);
            }
        }
        return null;
    }
}