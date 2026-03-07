package io.mosip.commons.khazana.impl;


import static io.mosip.commons.khazana.config.LoggerConfiguration.REGISTRATIONID;
import static io.mosip.commons.khazana.config.LoggerConfiguration.SESSIONID;
import static io.mosip.commons.khazana.constant.KhazanaConstant.TAGS_FILENAME;
import static io.mosip.commons.khazana.constant.KhazanaErrorCodes.OBJECT_STORE_NOT_ACCESSIBLE;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import io.mosip.commons.khazana.util.SafeS3InputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.springframework.beans.factory.DisposableBean;
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
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import io.mosip.commons.khazana.config.LoggerConfiguration;
import io.mosip.commons.khazana.dto.ObjectDto;
import io.mosip.commons.khazana.exception.ObjectStoreAdapterException;
import io.mosip.commons.khazana.spi.ObjectStoreAdapter;
import io.mosip.commons.khazana.util.ObjectStoreUtil;
import io.mosip.kernel.core.exception.ExceptionUtils;
import io.mosip.kernel.core.logger.spi.Logger;

@Service
@Qualifier("S3Adapter")
public class S3Adapter implements ObjectStoreAdapter, DisposableBean {

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
    private int clientExecutionTimeout;

    @Value("${object.store.s3.use.account.as.bucketname:false}")
    private boolean useAccountAsBucketname;

    @Value("${object.store.s3.bucket-name-prefix:}")
    private String bucketNamePrefix;

    /**
     * volatile: writes from one thread are immediately visible to all other threads.
     * Without it, CPUs can cache the value per-thread, so 200 concurrent threads can
     * all see null simultaneously and each create their own AmazonS3 client.
     */
    private volatile AmazonS3 connection = null;

    /**
     * ReentrantLock instead of synchronized: virtual threads blocked on a ReentrantLock
     * can unmount from their carrier thread while waiting, unlike synchronized blocks
     * which pin the virtual thread to the carrier thread and prevent other virtual threads
     * from running on it — causing severe throughput degradation under high concurrency.
     */
    private final ReentrantLock connectionLock = new ReentrantLock();

    /**
     * ConcurrentHashMap-backed Set for O(1) thread-safe contains/add.
     * Tracks which buckets are confirmed to exist so we avoid a doesBucketExistV2()
     * S3 API call on every write operation.
     */
    private final Set<String> existingBuckets = ConcurrentHashMap.newKeySet();

    private static final String SEPARATOR = "/";

    private static final String TAG_BACKWARD_COMPATIBILITY_ERROR = "Object-prefix is already an object, please choose a different object-prefix name";

    private static final String TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR = "Access Denied";

    /**
     * Shuts down the S3 client and clears the connection reference to prevent connection leaks.
     * Uses ReentrantLock so virtual threads can unmount while waiting (unlike synchronized).
     */
    private void shutdownConnection() {
        connectionLock.lock();
        try {
            if (connection != null) {
                try {
                    connection.shutdown();
                } catch (Exception e) {
                    LOGGER.warn(SESSIONID, REGISTRATIONID, "Error shutting down S3 connection", ExceptionUtils.getStackTrace(e));
                } finally {
                    connection = null;
                }
            }
        } finally {
            connectionLock.unlock();
        }
    }

    /**
     * Shutdown S3 client when the adapter bean is destroyed (e.g. application shutdown or context refresh).
     * Without this, the connection pool is never released in the normal "no error" path, causing a connection leak.
     */
    @Override
    public void destroy() {
        shutdownConnection();
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

        bucketName = normalizeBucket(bucketName);

        try {
            S3Object s3Object = getConnection(bucketName).getObject(bucketName, finalObjectName);
            if (s3Object != null) {
                long contentLength = s3Object.getObjectMetadata() != null
                        ? s3Object.getObjectMetadata().getContentLength() : -1;
                return new SafeS3InputStream(s3Object, contentLength);
            }
        } catch (AmazonS3Exception e) {
            // S3 operational error (NoSuchKey, etc.) — connection is healthy, do NOT reset it
            LOGGER.error(SESSIONID, REGISTRATIONID, "S3 error in getObject for: " + objectName + " | status: " + e.getStatusCode(), ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        } catch (Exception e) {
            // Unexpected transport-level error — reset connection so next call reconnects
            shutdownConnection();
            LOGGER.error(SESSIONID, REGISTRATIONID, "Unexpected error in getObject for: " + objectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
        return null;
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
        bucketName = normalizeBucket(bucketName);
        try {
            return getConnection(bucketName).doesObjectExist(bucketName, finalObjectName);
        } catch (AmazonS3Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "S3 error in exists for: " + objectName + " | status: " + e.getStatusCode(), ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        } catch (Exception e) {
            shutdownConnection();
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public boolean putObject(String account, final String container, String source, String process, String objectName, InputStream data) {
        String finalObjectName;
        String bucketName;
        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
            bucketName = account;
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName = container;
        }
        bucketName = normalizeBucket(bucketName);
        AmazonS3 client = getConnection(bucketName);
        try {
            ensureBucketExists(client, bucketName);
            client.putObject(bucketName, finalObjectName, data, new ObjectMetadata());
            return true;
        } catch (AmazonS3Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "S3 error in putObject for: " + objectName + " | status: " + e.getStatusCode(), ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        } catch (Exception e) {
            shutdownConnection();
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process,
                                                 String objectName, Map<String, Object> metadata) {
        String finalObjectName;
        String bucketName;
        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
            bucketName = account;
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName = container;
        }
        bucketName = normalizeBucket(bucketName);
        try {
            ObjectMetadata objectMetadata = new ObjectMetadata();
            // Fetch only the metadata header (HEAD request, no content download) then merge with new metadata
            ObjectMetadata existingMetadata = getConnection(bucketName).getObjectMetadata(bucketName, finalObjectName);
            if (existingMetadata != null && existingMetadata.getUserMetadata() != null)
                existingMetadata.getUserMetadata().forEach((k, v) -> objectMetadata.addUserMetadata(k, v));
            metadata.forEach((k, v) -> objectMetadata.addUserMetadata(k, v != null ? v.toString() : null));
            // Server-side copy with new metadata — no content is transferred to/from the client
            CopyObjectRequest copyRequest = new CopyObjectRequest(bucketName, finalObjectName, bucketName, finalObjectName)
                    .withNewObjectMetadata(objectMetadata);
            getConnection(bucketName).copyObject(copyRequest);
            return metadata;
        } catch (AmazonS3Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "S3 error in addObjectMetaData for: " + objectName + " | status: " + e.getStatusCode(), ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        } catch (Exception e) {
            shutdownConnection();
            LOGGER.error(SESSIONID, REGISTRATIONID, "Unexpected error in addObjectMetaData for: " + objectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process,
                                                 String objectName, String key, String value) {
        Map<String, Object> meta = new HashMap<>();
        meta.put(key, value);
        String finalObjectName = useAccountAsBucketname
                ? ObjectStoreUtil.getName(container, source, process, objectName)
                : ObjectStoreUtil.getName(source, process, objectName);
        return addObjectMetaData(account, container, source, process, finalObjectName, meta);
    }

    @Override
    public Map<String, Object> getMetaData(String account, String container, String source, String process,
                                           String objectName) {
        String finalObjectName;
        String bucketName;
        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
            bucketName = account;
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName = container;
        }
        bucketName = normalizeBucket(bucketName);
        Map<String, Object> metaData = new HashMap<>();
        try {
            ObjectMetadata objectMetadata = getConnection(bucketName).getObjectMetadata(bucketName, finalObjectName);
            if (objectMetadata != null && objectMetadata.getUserMetadata() != null)
                objectMetadata.getUserMetadata().forEach((k, v) -> metaData.put(k, v));
            return metaData;
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                LOGGER.debug(SESSIONID, REGISTRATIONID, "Object not found in getMetaData for: " + objectName);
                return metaData; // normal "not found" — connection is healthy
            }
            // Non-404 S3 error — connection is still healthy, do NOT reset
            LOGGER.error(SESSIONID, REGISTRATIONID, "S3 error in getMetaData for: " + objectName + " | status: " + e.getStatusCode(), ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        } catch (Exception e) {
            shutdownConnection();
            LOGGER.error(SESSIONID, REGISTRATIONID, "Unexpected error in getMetaData for: " + objectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public Integer incMetadata(String account, String container, String source, String process, String objectName, String metaDataKey) {
        Map<String, Object> metadata = getMetaData(account, container, source, process, objectName);
        if (metadata.get(metaDataKey) != null) {
            int newVal = Integer.parseInt(metadata.get(metaDataKey).toString()) + 1;
            metadata.put(metaDataKey, newVal);
            addObjectMetaData(account, container, source, process, objectName, metadata);
            return newVal;
        }
        return null;
    }

    @Override
    public Integer decMetadata(String account, String container, String source, String process, String objectName, String metaDataKey) {
        Map<String, Object> metadata = getMetaData(account, container, source, process, objectName);
        if (metadata.get(metaDataKey) != null) {
            int newVal = Integer.parseInt(metadata.get(metaDataKey).toString()) - 1;
            metadata.put(metaDataKey, newVal);
            addObjectMetaData(account, container, source, process, objectName, metadata);
            return newVal;
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
        bucketName = normalizeBucket(bucketName);
        try {
            getConnection(bucketName).deleteObject(bucketName, finalObjectName);
            return true;
        } catch (AmazonS3Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "S3 error in deleteObject for: " + objectName + " | status: " + e.getStatusCode(), ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        } catch (Exception e) {
            shutdownConnection();
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public boolean removeContainer(String account, String container, String source, String process) {
        return false;
    }

    @Override
    public boolean pack(String account, String container, String source, String process, String refId) {
        return false;
    }

    /**
     * Double-checked locking with ReentrantLock for thread-safe singleton connection.
     * ReentrantLock is used instead of synchronized so virtual threads can unmount
     * while waiting for the lock — synchronized would pin them to carrier threads.
     */
    private AmazonS3 getConnection(String bucketName) {
        if (connection != null)
            return connection;

        connectionLock.lock();
        try {
            if (connection != null)
                return connection;

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

                    // Test connection once before returning it
                    connection.doesBucketExistV2(bucketName);
                    return connection;

                } catch (Exception e) {
                    shutdownConnection();
                    LOGGER.error(SESSIONID, REGISTRATIONID,
                            "Exception occured while obtaining connection for " + bucketName
                                    + ". Will try again. Retry count : " + attempt,
                            ExceptionUtils.getStackTrace(e));

                    if (attempt >= maxRetry) {
                        LOGGER.error(SESSIONID, REGISTRATIONID,
                                "Maximum retry limit exceeded. Could not obtain connection for " + bucketName
                                        + ". Retry count :" + attempt);
                        throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                                OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
                    }
                }
            }

            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage());
        } finally {
            connectionLock.unlock();
        }
    }

    public List<ObjectDto> getAllObjects(String account, String id) {
        List<S3ObjectSummary> os = new ArrayList<>();
        if (useAccountAsBucketname) {
            String searchPattern = id + SEPARATOR;
            account = normalizeBucket(account);
            // listObjectsV2 with pagination to avoid silent 1000-object truncation
            ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(account).withPrefix(searchPattern);
            ListObjectsV2Result result;
            do {
                result = getConnection(account).listObjectsV2(request);
                os.addAll(result.getObjectSummaries());
                request.setContinuationToken(result.getNextContinuationToken());
            } while (result.isTruncated());
        } else {
            id = normalizeBucket(id);
            ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(id);
            ListObjectsV2Result result;
            do {
                result = getConnection(id).listObjectsV2(request);
                os.addAll(result.getObjectSummaries());
                request.setContinuationToken(result.getNextContinuationToken());
            } while (result.isTruncated());
        }

        if (!os.isEmpty()) {
            List<ObjectDto> objectDtos = new ArrayList<>();
            os.forEach(o -> {
                String[] tempKeys = o.getKey().split("/");
                if (useAccountAsBucketname) {
                    if (tempKeys.length > 1 && tempKeys[1] != null && tempKeys[1].endsWith(TAGS_FILENAME))
                        tempKeys = null;
                } else {
                    if (tempKeys.length > 0 && tempKeys[0] != null && tempKeys[0].endsWith(TAGS_FILENAME))
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
            return objectDtos;
        }

        return null;
    }

    private String[] removeIdFromObjectPath(boolean useAccountAsBucketname, String[] keys) {
        return (useAccountAsBucketname && ArrayUtils.isNotEmpty(keys)) ?
                (String[]) ArrayUtils.remove(keys, 0) : keys;
    }

    @Override
    public Map<String, String> addTags(String account, String container, Map<String, String> tags) {
        String bucketName;
        String finalObjectName;
        if (useAccountAsBucketname) {
            bucketName = account;
            finalObjectName = ObjectStoreUtil.getName(container, null, TAGS_FILENAME);
        } else {
            bucketName = container;
            finalObjectName = TAGS_FILENAME;
        }
        bucketName = normalizeBucket(bucketName);
        AmazonS3 client = getConnection(bucketName);
        try {
            ensureBucketExists(client, bucketName);
            for (Entry<String, String> entry : tags.entrySet()) {
                String tagName = ObjectStoreUtil.getName(finalObjectName, entry.getKey());
                try (InputStream data = IOUtils.toInputStream(entry.getValue(), StandardCharsets.UTF_8)) {
                    try {
                        client.putObject(bucketName, tagName, data, new ObjectMetadata());
                    } catch (AmazonS3Exception e) {
                        if (e.getMessage().contains(TAG_BACKWARD_COMPATIBILITY_ERROR)
                                || e.getMessage().contains(TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR)) {
                            if (client.doesObjectExist(bucketName, finalObjectName)) {
                                client.deleteObject(bucketName, finalObjectName);
                                return addTags(account, container, tags);
                            }
                        }
                        // S3 error — connection healthy, re-throw without resetting
                        LOGGER.error(SESSIONID, REGISTRATIONID, "S3 error in addTags for: " + container + " | status: " + e.getStatusCode(), ExceptionUtils.getStackTrace(e));
                        throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
                    }
                }
            }
        } catch (ObjectStoreAdapterException e) {
            throw e; // already handled above
        } catch (Exception e) {
            shutdownConnection();
            LOGGER.error(SESSIONID, REGISTRATIONID, "Unexpected error in addTags for: " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
        return tags;
    }

    @Override
    public Map<String, String> getTags(String account, String container) {
        Map<String, String> objectTags = new ConcurrentHashMap<>();
        String bucketName;
        String finalObjectName;
        if (useAccountAsBucketname) {
            bucketName = account;
            finalObjectName = ObjectStoreUtil.getName(container, null, TAGS_FILENAME) + SEPARATOR;
        } else {
            bucketName = container;
            finalObjectName = TAGS_FILENAME + SEPARATOR;
        }
        bucketName = normalizeBucket(bucketName);

        try {
            AmazonS3 client = getConnection(bucketName);
            List<S3ObjectSummary> objectSummary;
            if (useAccountAsBucketname)
                objectSummary = client.listObjects(bucketName, finalObjectName).getObjectSummaries();
            else
                objectSummary = client.listObjects(bucketName).getObjectSummaries();

            List<String> tagNames = new ArrayList<>();
            if (objectSummary != null && !objectSummary.isEmpty()) {
                objectSummary.forEach(o -> {
                    String[] keys = o.getKey().split("/");
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

            // Capture effectively-final copies for use in lambda
            final String effectiveBucketName = bucketName;
            final String effectiveFinalObjectName = finalObjectName;
            // Fetch all tag values concurrently to avoid N+1 serial API calls
            tagNames.parallelStream().forEach(tagName ->
                    objectTags.put(tagName, client.getObjectAsString(effectiveBucketName, effectiveFinalObjectName + tagName)));

            return objectTags;

        } catch (AmazonS3Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "S3 error in getTags for: " + container + " | status: " + e.getStatusCode(), ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        } catch (Exception e) {
            shutdownConnection();
            LOGGER.error(SESSIONID, REGISTRATIONID, "Unexpected error in getTags for: " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public void deleteTags(String account, String container, List<String> tags) {
        String bucketName;
        String finalObjectName;
        if (useAccountAsBucketname) {
            bucketName = account;
            finalObjectName = ObjectStoreUtil.getName(container, null, TAGS_FILENAME);
        } else {
            bucketName = container;
            finalObjectName = TAGS_FILENAME;
        }
        bucketName = normalizeBucket(bucketName); // Bug fix: was using raw 'container' below
        AmazonS3 client = getConnection(bucketName);
        try {
            ensureBucketExists(client, bucketName);
            for (String tag : tags) {
                client.deleteObject(bucketName, ObjectStoreUtil.getName(finalObjectName, tag));
            }
        } catch (AmazonS3Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "S3 error in deleteTags for: " + container + " | status: " + e.getStatusCode(), ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        } catch (Exception e) {
            shutdownConnection();
            LOGGER.error(SESSIONID, REGISTRATIONID, "Unexpected error in deleteTags for: " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    /**
     * Ensures a bucket exists, using a ConcurrentHashMap-backed Set to avoid
     * a doesBucketExistV2() S3 API call on every write operation.
     * Works for both useAccountAsBucketname=true and false.
     */
    private void ensureBucketExists(AmazonS3 client, String bucketName) {
        if (existingBuckets.contains(bucketName))
            return;
        if (!client.doesBucketExistV2(bucketName))
            client.createBucket(bucketName);
        existingBuckets.add(bucketName);
    }

    /**
     * Applies bucket prefix and lowercases per S3 naming rules.
     */
    private String normalizeBucket(String bucketName) {
        if (!bucketName.startsWith(bucketNamePrefix))
            bucketName = bucketNamePrefix + bucketName;
        return bucketName.toLowerCase();
    }
}
