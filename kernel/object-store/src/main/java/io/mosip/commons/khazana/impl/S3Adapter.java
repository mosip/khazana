package io.mosip.commons.khazana.impl;

import static io.mosip.commons.khazana.config.LoggerConfiguration.REGISTRATIONID;
import static io.mosip.commons.khazana.config.LoggerConfiguration.SESSIONID;
import static io.mosip.commons.khazana.constant.KhazanaConstant.TAGS_FILENAME;
import static io.mosip.commons.khazana.constant.KhazanaErrorCodes.OBJECT_STORE_NOT_ACCESSIBLE;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import io.mosip.commons.khazana.util.SafeS3InputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import io.mosip.commons.khazana.config.LoggerConfiguration;
import io.mosip.commons.khazana.dto.ObjectDto;
import io.mosip.commons.khazana.dto.ObjectStoreReference;
import io.mosip.commons.khazana.exception.ObjectStoreAdapterException;
import io.mosip.commons.khazana.spi.ObjectStoreAdapter;
import io.mosip.commons.khazana.util.ObjectStoreUtil;
import io.mosip.kernel.core.exception.ExceptionUtils;
import io.mosip.kernel.core.logger.spi.Logger;

/**
 * S3 Object Store Adapter with proper stream handling to prevent connection leaks.
 * <p>
 * Primary {@link io.mosip.commons.khazana.spi.ObjectStoreAdapter}, selected with
 * {@code @Qualifier("S3Adapter")}. One shared {@link com.amazonaws.services.s3.AmazonS3}
 * client is reused. A failed call sets that client to {@code null} so the next call
 * reconnects, up to {@code object.store.connection.max.retry} attempts.
 * Bucket names are lower-cased. {@code object.store.s3.bucket-name-prefix} is prepended
 * when the name does not already start with it.
 * When {@code object.store.s3.use.account.as.bucketname} is {@code false} (the default),
 * the bucket is the container and the key is {@code source/process/objectName}.
 * When it is {@code true}, the bucket is the account and the key is
 * {@code container/source/process/objectName}.
 * Tags are objects under {@code Tags/}, not native S3 object tags.
 * {@code removeContainer} and {@code pack} return {@code false}.
 * {@code getObject} returns a {@link io.mosip.commons.khazana.util.SafeS3InputStream}.
 * {@link #moveObject} copies then optionally deletes. {@link #listObjectsByPrefix}
 * returns container-relative keys under the prefix (paginated {@code ListObjectsV2}).
 *
 * Key improvements:
 * - Proper try-with-resources for stream management
 * - Content-Length validation to prevent memory buffering issues
 * - Explicit stream closing to prevent "Not all bytes were read" warnings
 * - Efficient stream-based operations
 * - Connection pooling and reuse optimization
 */
@Service
@Qualifier("S3Adapter")
public class S3Adapter implements ObjectStoreAdapter {

    /**
     * Kernel logger. Calls pass {@code SESSION_ID} and {@code REGISTRATION_ID}.
     */
    private final Logger LOGGER = LoggerConfiguration.logConfig(S3Adapter.class);

    /**
     * S3 access key. Property {@code object.store.s3.accesskey}, default {@code accesskey:accesskey}.
     */
    @Value("${object.store.s3.accesskey:accesskey:accesskey}")
    private String accessKey;
    /**
     * S3 secret key. Property {@code object.store.s3.secretkey}, default {@code secretkey:secretkey}.
     */
    @Value("${object.store.s3.secretkey:secretkey:secretkey}")
    private String secretKey;
    /**
     * S3 endpoint URL. Property {@code object.store.s3.url}, default {@code null}.
     */
    @Value("${object.store.s3.url:null}")
    private String url;

    /**
     * S3 region passed with {@link #url} to the endpoint configuration.
     * Property {@code object.store.s3.region}, default {@code null}.
     */
    @Value("${object.store.s3.region:null}")
    private String region;

    /**
     * Read limit set on metadata rewrite requests, in bytes.
     * Property {@code object.store.s3.readlimit}, default {@code 10000000}.
     */
    @Value("${object.store.s3.readlimit:10000000}")
    private int readlimit;

    /**
     * Maximum connection attempts and SDK error retries.
     * Property {@code object.store.connection.max.retry}, default {@code 20}.
     */
    @Value("${object.store.connection.max.retry:20}")
    private int maxRetry;

    /**
     * Maximum HTTP connections in the S3 client pool.
     * Property {@code object.store.max.connection}, default {@code 200}.
     */
    @Value("${object.store.max.connection:200}")
    private int maxConnection;

    /**
     * Connection timeout in milliseconds.
     * Property {@code object.store.connection.timeout}, default {@code 5000}.
     */
    @Value("${object.store.connection.timeout:5000}")
    private int connectionTimeout;

    /**
     * Socket timeout in milliseconds.
     * Property {@code object.store.socket.timeout}, default {@code 10000}.
     */
    @Value("${object.store.socket.timeout:10000}")
    private int socketTimeout;

    /**
     * Client execution timeout in milliseconds.
     * Property {@code object.store.client.execution.timeout}, default {@code 15000}.
     */
    @Value("${object.store.client.execution.timeout:15000}")
    private int clientExecutionTimeout;

    /**
     * When {@code true}, the account is the bucket and the container is the first key segment.
     * Property {@code object.store.s3.use.account.as.bucketname}, default {@code false}.
     */
    @Value("${object.store.s3.use.account.as.bucketname:false}")
    private boolean useAccountAsBucketname;

    /**
     * Prefix prepended to bucket names that do not already start with it.
     * Property {@code object.store.s3.bucket-name-prefix}, default empty.
     */
    @Value("${object.store.s3.bucket-name-prefix:}")
    private String bucketNamePrefix;

    /**
     * Configured stream buffer size in bytes.
     * Property {@code object.store.s3.stream.buffer.size}, default {@code 8192}.
     */
    @Value("${object.store.s3.stream.buffer.size:8192}")
    private int streamBufferSize;

    /**
     * Retry counter held on the adapter. Connection attempts are counted inside {@link #getConnection(String)}.
     */
    private int retry = 0;

    /**
     * Bucket names already known to exist when {@link #useAccountAsBucketname} is {@code true}.
     */
    private List<String> existingBuckets = new ArrayList<>();

    /**
     * Shared S3 client. Cleared after a failed call so the next operation reconnects.
     */
    private AmazonS3 connection = null;

    /**
     * Separator between key segments and after the tags prefix.
     */
    private static final String SEPARATOR = "/";

    /**
     * S3 error text when a tag prefix collides with an existing object. {@link #addTags} deletes that object and retries.
     */
    private static final String TAG_BACKWARD_COMPATIBILITY_ERROR = "Object-prefix is already an object, please choose a different object-prefix name";

    /**
     * S3 error text treated like {@link #TAG_BACKWARD_COMPATIBILITY_ERROR} during tag writes.
     */
    private static final String TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR = "Access Denied";

    /**
     * Reads an object and wraps the body in {@link io.mosip.commons.khazana.util.SafeS3InputStream}.
     * The caller must close the stream. A null S3 result returns {@code null}.
     *
     * @param account    object-store account; bucket when account-as-bucket mode is on
     * @param container  bucket when account-as-bucket mode is off, otherwise the first key segment
     * @param source     source path segment; skipped when null or empty
     * @param process    process path segment; skipped when null or empty
     * @param objectName object name
     * @return object contents, or {@code null} when S3 returns no object
     * @throws ObjectStoreAdapterException when the object cannot be read
     */
    @Override
    public InputStream getObject(String account, String container, String source, String process, String objectName) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "getObject - method started for account: " + account + ", container: " + container + ", source: " + source + ", process: " + process + ", objectName: " + objectName);

        String finalObjectName=null;
        String bucketName=null;
        if(useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container,source, process, objectName);
            bucketName=account;
        }else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName=container;
        }

        bucketName = addBucketPrefix(bucketName);
        bucketName = bucketName.toLowerCase();
        S3Object s3Object = null;
        try {
            s3Object = getConnection(bucketName).getObject(bucketName, finalObjectName);
            if (s3Object != null) {
                // IMPORTANT: Get content length to prevent "Not all bytes were read" warning
                ObjectMetadata metadata = s3Object.getObjectMetadata();
                long contentLength = metadata != null ? metadata.getContentLength() : -1;

                LOGGER.info(SESSIONID, REGISTRATIONID, "getObject - retrieved object with contentLength: " + contentLength + " bytes for objectName: " + objectName);

                // Return wrapper stream that ensures proper cleanup
                return new SafeS3InputStream(s3Object, contentLength);
            }
        } catch (Exception e) {
            connection = null;
            if (s3Object != null) {
                try {
                    s3Object.close();
                } catch (IOException ioe) {
                    LOGGER.error(SESSIONID, REGISTRATIONID, "Error closing S3Object on exception", ExceptionUtils.getStackTrace(ioe));
                }
            }
            long endTime = System.currentTimeMillis();
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured in getObject for : " + container + " after " + (endTime - startTime) + "ms", ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
        long endTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "getObject - method completed in " + (endTime - startTime) + "ms for objectName: " + objectName);
        return null;
    }

    /**
     * Reports whether the object key exists.
     *
     * @param account    object-store account; bucket when account-as-bucket mode is on
     * @param container  bucket when account-as-bucket mode is off, otherwise the first key segment
     * @param source     source path segment; skipped when null or empty
     * @param process    process path segment; skipped when null or empty
     * @param objectName object name
     * @return {@code true} when the key exists
     * @throws ObjectStoreAdapterException when the existence check fails
     */
    @Override
    public boolean exists(String account, String container, String source, String process, String objectName) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "exists - method started for account: " + account + ", container: " + container + ", source: " + source + ", process: " + process + ", objectName: " + objectName);

        String finalObjectName=null;
        String bucketName=null;
        if(useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container,source, process, objectName);
            bucketName=account;
        }else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName=container;
        }
        bucketName = addBucketPrefix(bucketName);
        bucketName = bucketName.toLowerCase();

        try {
            boolean result = getConnection(bucketName).doesObjectExist(bucketName,finalObjectName);
            long endTime = System.currentTimeMillis();
            LOGGER.info(SESSIONID, REGISTRATIONID, "exists - method completed in " + (endTime - startTime) + "ms, objectExists: " + result + " for objectName: " + objectName);
            return result;
        } catch (Exception e) {
            connection = null;
            long endTime = System.currentTimeMillis();
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured in exists method for : " + container + " after " + (endTime - startTime) + "ms", ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    /**
     * Writes {@code data} to the object key, creating the bucket when it is missing.
     * {@code data} is closed before this method returns.
     *
     * @param account    object-store account; bucket when account-as-bucket mode is on
     * @param container  bucket when account-as-bucket mode is off, otherwise the first key segment
     * @param source     source path segment; skipped when null or empty
     * @param process    process path segment; skipped when null or empty
     * @param objectName object name
     * @param data       object contents; closed by this method
     * @return {@code true} when the put succeeds
     * @throws ObjectStoreAdapterException when the object cannot be written
     */
    @Override
    public boolean putObject(String account, final String container, String source, String process, String objectName, InputStream data) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "putObject - method started for account: " + account + ", container: " + container + ", source: " + source + ", process: " + process + ", objectName: " + objectName);

        String finalObjectName=null;
        String bucketName=null;
        if(useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container,source, process, objectName);
            bucketName=account;
        }else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName=container;
        }
        bucketName = addBucketPrefix(bucketName);
        bucketName = bucketName.toLowerCase();

        try {
            AmazonS3 connection = getConnection(bucketName);
            if (!doesBucketExists(bucketName)) {
                LOGGER.info(SESSIONID, REGISTRATIONID, "putObject - creating bucket: " + bucketName);
                connection.createBucket(bucketName);
                if (useAccountAsBucketname)
                    existingBuckets.add(bucketName);
            }

            // Use try-with-resources to ensure stream is properly closed
            try (InputStream inputStream = data) {
                connection.putObject(bucketName, finalObjectName, inputStream, new ObjectMetadata());
                long endTime = System.currentTimeMillis();
                LOGGER.info(SESSIONID, REGISTRATIONID, "putObject - method completed successfully in " + (endTime - startTime) + "ms for objectName: " + objectName);
                return true;
            }
        } catch (Exception e) {
            connection = null;
            long endTime = System.currentTimeMillis();
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured in putObject for : " + container + " after " + (endTime - startTime) + "ms", ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    /**
     * Merges {@code metadata} into the object's user metadata and rewrites the object.
     * Existing user metadata is kept. The object body is read and written back.
     *
     * @param account    object-store account; bucket when account-as-bucket mode is on
     * @param container  bucket when account-as-bucket mode is off, otherwise the first key segment
     * @param source     source path segment; skipped when null or empty
     * @param process    process path segment; skipped when null or empty
     * @param objectName object name
     * @param metadata   entries to add; values are stored with {@code toString()}
     * @return {@code metadata}
     * @throws ObjectStoreAdapterException when the metadata rewrite fails
     */
    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process,
                                                 String objectName, Map<String, Object> metadata) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "addObjectMetaData - method started for account: " + account + ", container: " + container + ", source: " + source + ", process: " + process + ", objectName: " + objectName + ", metadata keys: " + (metadata != null ? metadata.keySet() : "null"));

        S3Object s3Object = null;
        try {
            String finalObjectName=null;
            String bucketName=null;
            if(useAccountAsBucketname) {
                finalObjectName = ObjectStoreUtil.getName(container,source, process, objectName);
                bucketName=account;
            }else {
                finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
                bucketName=container;
            }
            bucketName = addBucketPrefix(bucketName);
            bucketName = bucketName.toLowerCase();

            s3Object = getConnection(bucketName).getObject(bucketName, finalObjectName);
            ObjectMetadata objectMetadata = new ObjectMetadata();

            if (s3Object.getObjectMetadata() != null && s3Object.getObjectMetadata().getUserMetadata() != null)
                s3Object.getObjectMetadata().getUserMetadata().entrySet().forEach(m -> objectMetadata.addUserMetadata(m.getKey(), m.getValue()));

            metadata.entrySet().stream().forEach(m -> objectMetadata.addUserMetadata(m.getKey(), m.getValue() != null ? m.getValue().toString() : null));

            // IMPORTANT: Use try-with-resources to ensure proper stream cleanup
            try (InputStream content = s3Object.getObjectContent()) {
                PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, finalObjectName, content, objectMetadata);
                putObjectRequest.getRequestClientOptions().setReadLimit(readlimit);
                getConnection(bucketName).putObject(putObjectRequest);
                long endTime = System.currentTimeMillis();
                LOGGER.info(SESSIONID, REGISTRATIONID, "addObjectMetaData - method completed successfully in " + (endTime - startTime) + "ms for objectName: " + objectName);
                return metadata;
            }
        } catch (Exception e) {
            connection = null;
            long endTime = System.currentTimeMillis();
            LOGGER.error(SESSIONID, REGISTRATIONID,"Exception occured to addObjectMetaData for : " + container + " after " + (endTime - startTime) + "ms", ExceptionUtils.getStackTrace(e));
            metadata = null;
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        } finally {
            try {
                if (s3Object != null)
                    s3Object.close();
            } catch (IOException e) {
                LOGGER.error(SESSIONID, REGISTRATIONID,"IO occured : " + container, ExceptionUtils.getStackTrace(e));
            }
        }
    }

    /**
     * Adds one user-metadata entry by delegating to
     * {@link #addObjectMetaData(String, String, String, String, String, Map)}.
     *
     * @param account    object-store account; bucket when account-as-bucket mode is on
     * @param container  bucket when account-as-bucket mode is off, otherwise the first key segment
     * @param source     source path segment; skipped when null or empty
     * @param process    process path segment; skipped when null or empty
     * @param objectName object name
     * @param key        metadata key
     * @param value      metadata value
     * @return metadata map returned by the map overload
     * @throws ObjectStoreAdapterException when the metadata rewrite fails
     */
    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process,
                                                 String objectName, String key, String value) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "addObjectMetaData(key-value) - method started for account: " + account + ", container: " + container + ", objectName: " + objectName + ", key: " + key + ", value: " + value);

        Map<String, Object> meta = new HashMap<>();
        meta.put(key, value);
        String finalObjectName=null;

        if(useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container,source, process, objectName);
        }else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
        }

        Map<String, Object> result = addObjectMetaData(account, container, source, process, finalObjectName, meta);
        long endTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "addObjectMetaData(key-value) - method completed in " + (endTime - startTime) + "ms for objectName: " + objectName);
        return result;
    }

    /**
     * Reads user metadata with a metadata-only request, without downloading the body.
     * HTTP 404 returns an empty map.
     *
     * @param account    object-store account; bucket when account-as-bucket mode is on
     * @param container  bucket when account-as-bucket mode is off, otherwise the first key segment
     * @param source     source path segment; skipped when null or empty
     * @param process    process path segment; skipped when null or empty
     * @param objectName object name
     * @return user metadata; empty when the object is missing or has no user metadata
     * @throws ObjectStoreAdapterException when the metadata read fails for a reason other than HTTP 404
     */
    @Override
    public Map<String, Object> getMetaData(String account, String container, String source, String process,
                                           String objectName) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID,
                "getMetaData started - account: {}, container: {}, source: {}, process: {}, objectName: {}",
                account, container, source, process, objectName);

        String bucketName;
        String finalObjectName;

        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
            bucketName = account;
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName = container;
        }

        bucketName = addBucketPrefix(bucketName).toLowerCase();

        Map<String, Object> metadataResult = new HashMap<>();

        try {
            // Use metadata-only request (HEAD operation + metadata) – no body download
            ObjectMetadata objectMetadata = getConnection(bucketName)
                    .getObjectMetadata(bucketName, finalObjectName);

            if (objectMetadata != null && objectMetadata.getUserMetadata() != null) {
                objectMetadata.getUserMetadata().forEach((key, value) ->
                        metadataResult.put(key, value));
            }

            long endTime = System.currentTimeMillis();
            LOGGER.info(SESSIONID, REGISTRATIONID,
                    "getMetaData completed successfully in {} ms | metadata keys: {} | object: {}",
                    (endTime - startTime), metadataResult.keySet(), finalObjectName);

            return metadataResult;

        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                LOGGER.debug(SESSIONID, REGISTRATIONID,
                        "Object not found in S3: bucket={}, key={}", bucketName, finalObjectName);
                return metadataResult; // return empty map (consistent with "no metadata")
            }

            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "S3 error fetching metadata | bucket: {} | key: {} | status: {}",
                    bucketName, finalObjectName, e.getStatusCode(), e);

            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);

        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Unexpected error in getMetaData | bucket: {} | key: {} after {} ms",
                    bucketName, finalObjectName, (System.currentTimeMillis() - startTime), e);

            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    /**
     * Increments an integer metadata value by one and writes the metadata map back.
     * Returns {@code null} when {@code metaDataKey} is absent.
     *
     * @param account     object-store account
     * @param container   container or bucket, depending on account-as-bucket mode
     * @param source      source path segment; skipped when null or empty
     * @param process     process path segment; skipped when null or empty
     * @param objectName  object name
     * @param metaDataKey metadata key to increment
     * @return the value after increment, or {@code null} when the key is absent
     * @throws ObjectStoreAdapterException when the metadata cannot be read or written
     */
    @Override
    public Integer incMetadata(String account, String container, String source, String process, String objectName, String metaDataKey) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "incMetadata - method started for account: " + account + ", container: " + container + ", objectName: " + objectName + ", metaDataKey: " + metaDataKey);

        try {
            Map<String, Object> metadata = getMetaData(account, container, source, process, objectName);
            if (metadata.get(metaDataKey) != null) {
                metadata.put(metaDataKey, Integer.valueOf(metadata.get(metaDataKey).toString()) + 1);
                addObjectMetaData(account, container, source, process, objectName, metadata);
                long endTime = System.currentTimeMillis();
                int result = Integer.valueOf(metadata.get(metaDataKey).toString());
                LOGGER.info(SESSIONID, REGISTRATIONID, "incMetadata - method completed successfully in " + (endTime - startTime) + "ms, new value: " + result + " for objectName: " + objectName);
                return result;
            }
            long endTime = System.currentTimeMillis();
            LOGGER.info(SESSIONID, REGISTRATIONID, "incMetadata - metaDataKey not found after " + (endTime - startTime) + "ms for objectName: " + objectName);
            return null;
        } catch (Exception e) {
            connection = null;
            long endTime = System.currentTimeMillis();
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured in incMetadata after " + (endTime - startTime) + "ms", ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    /**
     * Decrements an integer metadata value by one and writes the metadata map back.
     * Returns {@code null} when {@code metaDataKey} is absent.
     *
     * @param account     object-store account
     * @param container   container or bucket, depending on account-as-bucket mode
     * @param source      source path segment; skipped when null or empty
     * @param process     process path segment; skipped when null or empty
     * @param objectName  object name
     * @param metaDataKey metadata key to decrement
     * @return the value after decrement, or {@code null} when the key is absent
     * @throws ObjectStoreAdapterException when the metadata cannot be read or written
     */
    @Override
    public Integer decMetadata(String account, String container, String source, String process, String objectName, String metaDataKey) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "decMetadata - method started for account: " + account + ", container: " + container + ", objectName: " + objectName + ", metaDataKey: " + metaDataKey);

        try {
            Map<String, Object> metadata = getMetaData(account, container, source, process, objectName);
            if (metadata.get(metaDataKey) != null) {
                metadata.put(metaDataKey, Integer.valueOf(metadata.get(metaDataKey).toString()) - 1);
                addObjectMetaData(account, container, source, process, objectName, metadata);
                long endTime = System.currentTimeMillis();
                int result = Integer.valueOf(metadata.get(metaDataKey).toString());
                LOGGER.info(SESSIONID, REGISTRATIONID, "decMetadata - method completed successfully in " + (endTime - startTime) + "ms, new value: " + result + " for objectName: " + objectName);
                return result;
            }
            long endTime = System.currentTimeMillis();
            LOGGER.info(SESSIONID, REGISTRATIONID, "decMetadata - metaDataKey not found after " + (endTime - startTime) + "ms for objectName: " + objectName);
            return null;
        } catch (Exception e) {
            connection = null;
            long endTime = System.currentTimeMillis();
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured in decMetadata after " + (endTime - startTime) + "ms", ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    /**
     * Deletes the object key.
     *
     * @param account    object-store account; bucket when account-as-bucket mode is on
     * @param container  bucket when account-as-bucket mode is off, otherwise the first key segment
     * @param source     source path segment; skipped when null or empty
     * @param process    process path segment; skipped when null or empty
     * @param objectName object name
     * @return {@code true} when the delete call returns
     * @throws ObjectStoreAdapterException when the object cannot be deleted
     */
    @Override
    public boolean deleteObject(String account, String container, String source, String process, String objectName) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "deleteObject - method started for account: " + account + ", container: " + container + ", source: " + source + ", process: " + process + ", objectName: " + objectName);

        String finalObjectName=null;
        String bucketName=null;
        if(useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container,source, process, objectName);
            bucketName=account;
        }else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName=container;
        }
        bucketName = addBucketPrefix(bucketName);
        bucketName = bucketName.toLowerCase();

        try {
            getConnection(bucketName).deleteObject(bucketName, finalObjectName);
            long endTime = System.currentTimeMillis();
            LOGGER.info(SESSIONID, REGISTRATIONID, "deleteObject - method completed successfully in " + (endTime - startTime) + "ms for objectName: " + objectName);
            return true;
        } catch (Exception e) {
            connection = null;
            long endTime = System.currentTimeMillis();
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured in deleteObject after " + (endTime - startTime) + "ms", ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    /**
     * Copies {@code src} to {@code dst} with a server-side S3 copy. When
     * {@code deleteSourceAfterCopy} is {@code true}, the source key is deleted after
     * a successful copy (a move). A missing source throws rather than returning
     * {@code false}, so callers can tell a failed copy from a no-op.
     *
     * @param src                   source location; {@code source} and {@code process}
     *                              may be null when {@code objectName} is already a full key
     * @param dst                   destination location, same key rules as {@code src}
     * @param deleteSourceAfterCopy {@code true} to delete the source after copy
     * @return {@code true} when the copy (and optional delete) succeed
     * @throws ObjectStoreAdapterException when S3 cannot copy or delete; a missing
     *         source is wrapped with {@link AmazonS3Exception} as the cause (HTTP 404)
     */
    @Override
    public boolean moveObject(ObjectStoreReference src, ObjectStoreReference dst,
            boolean deleteSourceAfterCopy) {
        String srcBucketName = "";
        String srcObjectName = "";
        String dstBucketName = "";
        String dstObjectName = "";
        try {
            srcBucketName = resolveBucket(src);
            srcObjectName = resolveKey(src);
            dstBucketName = resolveBucket(dst);
            dstObjectName = resolveKey(dst);

            long startTime = System.currentTimeMillis();
            getConnection(srcBucketName).copyObject(srcBucketName, srcObjectName, dstBucketName, dstObjectName);
            LOGGER.debug(SESSIONID, REGISTRATIONID,
                    "moveObject copyObject timeTaken: " + (System.currentTimeMillis() - startTime)
                            + " ms srcBucket: " + srcBucketName + " srcKey: " + srcObjectName
                            + " dstBucket: " + dstBucketName + " dstKey: " + dstObjectName);
            if (deleteSourceAfterCopy) {
                getConnection(srcBucketName).deleteObject(srcBucketName, srcObjectName);
                LOGGER.debug(SESSIONID, REGISTRATIONID,
                        "moveObject deleteObject source timeTaken: " + (System.currentTimeMillis() - startTime)
                                + " ms srcBucket: " + srcBucketName + " srcKey: " + srcObjectName);
            }
            return true;
        } catch (AmazonS3Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "S3 error in moveObject from: " + srcObjectName + " to: " + dstObjectName
                            + " | status: " + e.getStatusCode(),
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        } catch (Exception e) {
            connection = null;
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Unexpected error in moveObject from: " + srcObjectName + " to: " + dstObjectName,
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    /**
     * Not supported. Logs and returns {@code false} without deleting a bucket.
     *
     * @param account   object-store account
     * @param container container name
     * @param source    unused path segment
     * @param process   unused path segment
     * @return {@code false}
     */
    @Override
    public boolean removeContainer(String account, String container, String source, String process) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "removeContainer - method started for account: " + account + ", container: " + container + ", source: " + source + ", process: " + process);
        long endTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "removeContainer - method not supported, completed in " + (endTime - startTime) + "ms");
        return false;
    }

    /**
     * Not supported. Logs and returns {@code false} without encrypting anything.
     *
     * @param account   object-store account
     * @param container container name
     * @param source    unused path segment
     * @param process   unused path segment
     * @param refId     encryption reference id; ignored
     * @return {@code false}
     */
    @Override
    public boolean pack(String account, String container, String source, String process, String refId) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "pack - method started for account: " + account + ", container: " + container + ", source: " + source + ", process: " + process + ", refId: " + refId);
        long endTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "pack - method not supported, completed in " + (endTime - startTime) + "ms");
        return false;
    }

    /**
     * This method will return a singleton connection. It will verify connection for the first time and will reuse same connection in subsequent calls.
     * A new client uses the configured credentials, timeouts, max connections, and retry cap,
     * enables path-style access, and checks that {@code bucketName} exists.
     * Failed attempts sleep with a short backoff. The shared client is returned on later calls.
     *
     * @param bucketName bucket used to verify the new client
     * @return shared {@link AmazonS3} client
     * @throws ObjectStoreAdapterException when every attempt fails or the retry loop exits unexpectedly
     */
    private AmazonS3 getConnection(String bucketName) {
        if (connection != null) {
            LOGGER.info(SESSIONID, REGISTRATIONID, "Reusing existing S3 connection");
            return connection;
        }

        int attempt = 0;
        while (attempt < maxRetry) {
            attempt++;
            try {
                AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
                ClientConfiguration clientConfig = new ClientConfiguration()
                        .withConnectionTimeout(connectionTimeout)
                        .withSocketTimeout(socketTimeout)
                        .withClientExecutionTimeout(clientExecutionTimeout)
                        .withMaxConnections(maxConnection)
                        .withMaxErrorRetry(maxRetry);

                connection = AmazonS3ClientBuilder.standard()
                        .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                        .enablePathStyleAccess()
                        .withClientConfiguration(clientConfig)
                        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(url, region))
                        .build();

                // Verify connection works
                connection.doesBucketExistV2(bucketName);

                LOGGER.info(SESSIONID, REGISTRATIONID,
                        "New S3 connection created successfully after {} attempts", attempt);
                return connection;

            } catch (Exception e) {
                LOGGER.warn(SESSIONID, REGISTRATIONID,
                        "S3 connection attempt {} failed for bucket {}. Retrying...", attempt, bucketName, e);

                if (attempt >= maxRetry) {
                    LOGGER.error(SESSIONID, REGISTRATIONID,
                            "Max retries ({}) reached. Giving up on S3 connection.", maxRetry);
                    throw new ObjectStoreAdapterException(
                            OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                            "Failed to create S3 connection after " + maxRetry + " attempts", e);
                }

                try {
                    Thread.sleep(300 + attempt * 200); // simple exponential backoff ~300–~4s
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                "Unexpected exit from connection retry loop");
    }

    /**
     * Lists objects in the bucket and maps each key to an {@link io.mosip.commons.khazana.dto.ObjectDto}.
     * Tag objects whose segment ends with {@code Tags} are skipped. In account-as-bucket mode
     * the first key segment (the container) is removed before the remaining segments are parsed:
     * one segment is the object name, two are source and object name, three are source, process,
     * and object name.
     *
     * @param account object-store account; bucket when account-as-bucket mode is on
     * @param id      container used as the bucket, or as the key prefix when account-as-bucket mode is on
     * @return parsed objects, or {@code null} when the bucket has no summaries
     * @throws ObjectStoreAdapterException when connecting to the bucket fails
     */
    public List<ObjectDto> getAllObjects(String account, String id) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "getAllObjects - method started for account: " + account + ", id: " + id);

        List<S3ObjectSummary> os = null;
        if(useAccountAsBucketname) {
            String searchPattern = id + SEPARATOR;
            account = addBucketPrefix(account);
            account = account.toLowerCase();
            os = getConnection(account).listObjects(account, searchPattern).getObjectSummaries();
        }
        else {
            id = addBucketPrefix(id);
            id = id.toLowerCase();
            os = getConnection(id).listObjects(id).getObjectSummaries();
        }

        if (os != null && os.size() > 0) {
            List<ObjectDto> objectDtos = new ArrayList<>();
            os.forEach(o -> {
                String[] tempKeys = o.getKey().split("/");
                if (useAccountAsBucketname) {
                    if (tempKeys[1] != null && tempKeys[1].endsWith(TAGS_FILENAME))
                        tempKeys = null;
                } else {
                    if (tempKeys[0] != null && tempKeys[0].endsWith(TAGS_FILENAME))
                        tempKeys = null;
                }

                String[] keys = removeIdFromObjectPath(useAccountAsBucketname, tempKeys);
                if (ArrayUtils.isNotEmpty(keys)) {
                    ObjectDto objectDto = null;
                    switch (keys.length) {
                        case 1:
                            objectDto = new ObjectDto(null, null, keys[0], o.getLastModified());
                            break;
                        case 2:
                            objectDto = new ObjectDto(keys[0], null, keys[1], o.getLastModified());
                            break;
                        case 3:
                            objectDto = new ObjectDto(keys[0], keys[1], keys[2], o.getLastModified());
                            break;
                    }
                    if (objectDto != null)
                        objectDtos.add(objectDto);
                }
            });
            long endTime = System.currentTimeMillis();
            LOGGER.info(SESSIONID, REGISTRATIONID, "getAllObjects - method completed successfully in " + (endTime - startTime) + "ms, found " + objectDtos.size() + " objects for account: " + account);
            return objectDtos;
        }

        long endTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "getAllObjects - method completed in " + (endTime - startTime) + "ms, no objects found for account: " + account);
        return null;
    }

    /**
     * Lists object keys under {@code prefix}. Returned keys are bucket-root keys when
     * the container is the bucket, and container-relative (the {@code container/}
     * segment stripped) when the account is the bucket, so they can be passed as
     * {@code objectName} on {@link ObjectStoreReference} to {@link #moveObject}.
     * An empty match returns an empty list, never {@code null}.
     *
     * @param account   object-store account; bucket when account-as-bucket mode is on
     * @param container bucket when account-as-bucket mode is off, otherwise the first key segment
     * @param prefix    prefix inside the container (for example {@code _draft/ridHash/Biometrics/})
     * @return matching keys; never {@code null}
     * @throws ObjectStoreAdapterException when the list call fails
     */
    @Override
    public List<String> listObjectsByPrefix(String account, String container, String prefix) {
        String bucketName = useAccountAsBucketname
                ? addBucketPrefix(account).toLowerCase()
                : addBucketPrefix(container).toLowerCase();
        String objectPrefix = useAccountAsBucketname ? ObjectStoreUtil.getName(container, prefix) : prefix;
        List<String> keys = new ArrayList<>();
        try {
            ListObjectsV2Request request = new ListObjectsV2Request()
                    .withBucketName(bucketName)
                    .withPrefix(objectPrefix);
            ListObjectsV2Result result;
            do {
                result = getConnection(bucketName).listObjectsV2(request);
                if (result.getObjectSummaries() != null) {
                    for (S3ObjectSummary summary : result.getObjectSummaries()) {
                        String key = summary.getKey();
                        if (useAccountAsBucketname && key.startsWith(container + SEPARATOR)) {
                            key = key.substring(container.length() + 1);
                        }
                        keys.add(key);
                    }
                }
                request.setContinuationToken(result.getNextContinuationToken());
            } while (result.isTruncated());
        } catch (Exception e) {
            connection = null;
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception in listObjectsByPrefix for prefix: " + prefix, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
        return keys;
    }

    /**
     * If account is used as bucket name then first element of array is the packet id.
     * This method removes packet id from array so that path is same irrespective of useAccountAsBucketname is true or false.
     *
     * @param useAccountAsBucketname when {@code true} and {@code keys} is not empty, index 0 is removed
     * @param keys                   key segments; may be {@code null} when the object was a tag
     * @return {@code keys} without the container segment when account-as-bucket mode is on; otherwise {@code keys}
     */
    private String[] removeIdFromObjectPath(boolean useAccountAsBucketname, String[] keys) {
        return (useAccountAsBucketname && ArrayUtils.isNotEmpty(keys)) ?
                (String[]) ArrayUtils.remove(keys, 0) : keys;
    }

    /**
     * Stores each tag as an object under {@code Tags/}, not as a native S3 object tag.
     * The bucket is created when missing. If S3 reports that the tag prefix is already an object,
     * or access is denied, and {@code Tags} itself exists, that object is deleted and the add is retried.
     *
     * @param account   object-store account; bucket when account-as-bucket mode is on
     * @param container bucket when account-as-bucket mode is off, otherwise the key prefix
     * @param tags      tag name to value; each value is stored as the object body
     * @return {@code tags}
     * @throws ObjectStoreAdapterException when a tag object cannot be written
     */
    @Override
    public Map<String, String> addTags(String account, String container, Map<String, String> tags) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "addTags - method started for account: " + account + ", container: " + container + ", tags: " + (tags != null ? tags.keySet() : "null"));

        String bucketName=null;
        String finalObjectName=null;
        try {
            if(useAccountAsBucketname) {
                bucketName=account;
                finalObjectName = ObjectStoreUtil.getName(container,null,TAGS_FILENAME);
            }else {
                bucketName=container;
                finalObjectName = TAGS_FILENAME;
            }
            bucketName = addBucketPrefix(bucketName);
            bucketName = bucketName.toLowerCase();
            AmazonS3 connection = getConnection(bucketName);
            if (!doesBucketExists(bucketName)) {
                LOGGER.info(SESSIONID, REGISTRATIONID, "addTags - creating bucket: " + bucketName);
                connection.createBucket(bucketName);
                if (useAccountAsBucketname)
                    existingBuckets.add(bucketName);
            }
            for(Entry<String, String> entry:tags.entrySet()) {
                String tagName=null;
                InputStream data = IOUtils.toInputStream(entry.getValue(), StandardCharsets.UTF_8);
                tagName = ObjectStoreUtil.getName(finalObjectName, entry.getKey());
                try {
                    LOGGER.info(SESSIONID, REGISTRATIONID, "addTags - adding tag: " + entry.getKey() + " for container: " + container);
                    // Use try-with-resources to ensure stream closure
                    try (InputStream tagData = data) {
                        connection.putObject(bucketName, tagName, tagData, new ObjectMetadata());
                    }
                } catch (Exception e) {
                    if (e instanceof AmazonS3Exception && (e.getMessage().contains(TAG_BACKWARD_COMPATIBILITY_ERROR)
                            || e.getMessage().contains(TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR))) {
                        LOGGER.info(SESSIONID, REGISTRATIONID, "addTags - backward compatibility error detected, attempting recovery for tag: " + entry.getKey());
                        if (connection.doesObjectExist(bucketName, finalObjectName)) {
                            connection.deleteObject(bucketName, finalObjectName);
                            LOGGER.info(SESSIONID, REGISTRATIONID, "addTags - deleted existing object and retrying");
                            addTags(account, container, tags);
                        } else {
                            connection = null;
                            long endTime = System.currentTimeMillis();
                            LOGGER.error(SESSIONID, REGISTRATIONID,
                                    "Exception occured while addTags for : " + container + " after " + (endTime - startTime) + "ms",
                                    ExceptionUtils.getStackTrace(e));
                            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
                        }
                    }else {
                        connection = null;
                        long endTime = System.currentTimeMillis();
                        LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured while addTags for : " + container + " after " + (endTime - startTime) + "ms",
                                ExceptionUtils.getStackTrace(e));
                        throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                                OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
                    }
                }
            }
            long endTime = System.currentTimeMillis();
            LOGGER.info(SESSIONID, REGISTRATIONID, "addTags - method completed successfully in " + (endTime - startTime) + "ms for container: " + container);

        } catch (Exception e) {
            connection = null;
            long endTime = System.currentTimeMillis();
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured while addTags for : " + container + " after " + (endTime - startTime) + "ms",
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
        return tags;
    }

    /**
     * Reads tag objects stored under {@code Tags/} and returns their names and bodies.
     *
     * @param account   object-store account; bucket when account-as-bucket mode is on
     * @param container bucket when account-as-bucket mode is off, otherwise the key prefix
     * @return tag name to object body; empty when no tag objects are found
     * @throws ObjectStoreAdapterException when tag objects cannot be listed or read
     */
    @Override
    public Map<String, String> getTags(String account, String container) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "getTags - method started for account: " + account + ", container: " + container);

        Map<String, String> objectTags = new HashMap<String, String>();
        try {

            String bucketName=null;
            String finalObjectName=null;
            if (useAccountAsBucketname) {
                bucketName=account;
                finalObjectName = ObjectStoreUtil.getName(container, null, TAGS_FILENAME) + SEPARATOR;
            }else {
                bucketName=container;
                finalObjectName = TAGS_FILENAME + SEPARATOR;
            }
            bucketName = addBucketPrefix(bucketName);
            bucketName = bucketName.toLowerCase();
            AmazonS3 connection = getConnection(bucketName);

            List<S3ObjectSummary> objectSummary = null;
            if(useAccountAsBucketname)
                objectSummary = connection.listObjects(bucketName, finalObjectName).getObjectSummaries();
            else
                objectSummary = connection.listObjects(bucketName).getObjectSummaries();

            List<String> tagNames=new ArrayList<String>();
            if (objectSummary != null && objectSummary.size() > 0) {
                LOGGER.info(SESSIONID, REGISTRATIONID, "getTags - found " + objectSummary.size() + " tag objects");

                objectSummary.forEach(o -> {
                    String[] keys = o.getKey().split("/");
                    if (ArrayUtils.isNotEmpty(keys)) {
                        if (useAccountAsBucketname) {
                            if (keys[1] != null && keys[1].endsWith(TAGS_FILENAME))
                                tagNames.add(keys[2]);
                        } else {
                            if (keys[0] != null && keys[0].endsWith(TAGS_FILENAME))
                                tagNames.add(keys[1]);
                        }

                    }
                });

            }
            LOGGER.info(SESSIONID, REGISTRATIONID, "getTags - processing " + tagNames.size() + " tags");
            for(String tagName:tagNames) {
                objectTags.put(tagName, connection.getObjectAsString(bucketName, finalObjectName+tagName));
            }

            long endTime = System.currentTimeMillis();
            LOGGER.info(SESSIONID, REGISTRATIONID, "getTags - method completed successfully in " + (endTime - startTime) + "ms, retrieved " + objectTags.size() + " tags for container: " + container);

            return objectTags;

        }catch(Exception e){
            long endTime = System.currentTimeMillis();
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured while getTags for : " + container + " after " + (endTime - startTime) + "ms",
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }

    }

    /**
     * Deletes the named tag objects under {@code Tags/}.
     * The bucket is created when it is missing.
     *
     * @param account   object-store account; bucket when account-as-bucket mode is on
     * @param container bucket when account-as-bucket mode is off, otherwise the key prefix
     * @param tags      tag names to delete
     * @throws ObjectStoreAdapterException when a tag object cannot be deleted
     */
    @Override
    public void deleteTags(String account, String container, List<String> tags) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "deleteTags - method started for account: " + account + ", container: " + container + ", tags: " + (tags != null ? tags.size() : 0));

        try {
            String bucketName=null;
            String finalObjectName=null;
            if(useAccountAsBucketname) {
                bucketName=account;
                finalObjectName = ObjectStoreUtil.getName(container,null,TAGS_FILENAME);
            }else {
                bucketName=container;
                finalObjectName = TAGS_FILENAME;
            }
            bucketName = addBucketPrefix(bucketName);
            bucketName = bucketName.toLowerCase();
            AmazonS3 connection = getConnection(bucketName);
            if (!doesBucketExists(bucketName)) {
                LOGGER.info(SESSIONID, REGISTRATIONID, "deleteTags - creating bucket: " + container);
                connection.createBucket(bucketName);
                if (useAccountAsBucketname)
                    existingBuckets.add(bucketName);
            }
            for(String tag:tags) {
                String tagName=null;
                tagName=ObjectStoreUtil.getName(finalObjectName, tag);
                LOGGER.info(SESSIONID, REGISTRATIONID, "deleteTags - deleting tag: " + tag + " for container: " + container);
                connection.deleteObject(bucketName, tagName);
            }
            long endTime = System.currentTimeMillis();
            LOGGER.info(SESSIONID, REGISTRATIONID, "deleteTags - method completed successfully in " + (endTime - startTime) + "ms, deleted " + tags.size() + " tags for container: " + container);

        } catch (Exception e) {
            connection = null;
            long endTime = System.currentTimeMillis();
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occurred while deleteTags for : " + container + " after " + (endTime - startTime) + "ms",
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }

    }

    /**
     * Reports whether {@code bucketName} exists.
     * In account-as-bucket mode, names already recorded in {@link #existingBuckets} are not
     * queried again. A bucket found in S3 is added to that list.
     *
     * @param bucketName lower-cased bucket name, including any prefix
     * @return {@code true} when the bucket exists
     */
    private boolean doesBucketExists(String bucketName) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "doesBucketExists - method started for bucketName: " + bucketName);

        boolean result = false;
        if (useAccountAsBucketname && existingBuckets.contains(bucketName)) {
            result = true;
            long endTime = System.currentTimeMillis();
            LOGGER.info(SESSIONID, REGISTRATIONID, "doesBucketExists - bucket found in cache in " + (endTime - startTime) + "ms for bucketName: " + bucketName);
            return result;
        }
        else if (useAccountAsBucketname && !existingBuckets.contains(bucketName)) {
            boolean doesBucketExistsInObjectStore = connection.doesBucketExistV2(bucketName);
            if (doesBucketExistsInObjectStore) {
                existingBuckets.add(bucketName);
                result = true;
            }
            long endTime = System.currentTimeMillis();
            LOGGER.info(SESSIONID, REGISTRATIONID, "doesBucketExists - checked object store in " + (endTime - startTime) + "ms, bucketExists: " + result + " for bucketName: " + bucketName);
            return result;
        } else {
            result = connection.doesBucketExistV2(bucketName);
            long endTime = System.currentTimeMillis();
            LOGGER.info(SESSIONID, REGISTRATIONID, "doesBucketExists - method completed in " + (endTime - startTime) + "ms, bucketExists: " + result + " for bucketName: " + bucketName);
            return result;
        }
    }

    /**
     * Bucket for an {@link ObjectStoreReference}: the account when account-as-bucket
     * mode is on, otherwise the container. Prefix and lower-case are applied.
     *
     * @param ref object location
     * @return normalized bucket name
     */
    private String resolveBucket(ObjectStoreReference ref) {
        String bucketName = useAccountAsBucketname ? ref.getAccount() : ref.getContainer();
        return addBucketPrefix(bucketName).toLowerCase();
    }

    /**
     * Object key for an {@link ObjectStoreReference}. Null source and process are
     * skipped so a full key in {@code objectName} (for example a draft path) is used as-is.
     *
     * @param ref object location
     * @return S3 object key
     */
    private String resolveKey(ObjectStoreReference ref) {
        if (useAccountAsBucketname) {
            return ObjectStoreUtil.getName(ref.getContainer(), ref.getSource(), ref.getProcess(), ref.getObjectName());
        }
        return ObjectStoreUtil.getName(ref.getSource(), ref.getProcess(), ref.getObjectName());
    }

    /**
     * Prepends {@link #bucketNamePrefix} when {@code bucketName} does not already start with it.
     *
     * @param bucketName bucket name before lower-casing
     * @return bucket name including the configured prefix
     */
    private String addBucketPrefix(String bucketName) {
        long startTime = System.currentTimeMillis();
        LOGGER.debug(SESSIONID, REGISTRATIONID, "addBucketPrefix - method started for bucketName: " + bucketName);

        if (bucketName.startsWith(bucketNamePrefix)) {
            long endTime = System.currentTimeMillis();
            LOGGER.debug(SESSIONID, REGISTRATIONID, "addBucketPrefix - prefix already present in " + (endTime - startTime) + "ms, bucketName: " + bucketName);
            return bucketName;
        } else {
            bucketName = bucketNamePrefix + bucketName;
            long endTime = System.currentTimeMillis();
            LOGGER.debug(SESSIONID, REGISTRATIONID, "addBucketPrefix - prefix added in " + (endTime - startTime) + "ms, bucketName: " + bucketName);
            return bucketName;
        }
    }
}