package io.mosip.commons.khazana.impl;

import static io.mosip.commons.khazana.config.LoggerConfiguration.REGISTRATIONID;
import static io.mosip.commons.khazana.config.LoggerConfiguration.SESSIONID;
import static io.mosip.commons.khazana.constant.KhazanaConstant.TAGS_FILENAME;
import static io.mosip.commons.khazana.constant.KhazanaErrorCodes.OBJECT_STORE_NOT_ACCESSIBLE;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantLock;

import io.mosip.commons.khazana.util.SafeS3InputStream;
import jakarta.annotation.PostConstruct;
import org.apache.commons.lang.ArrayUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

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

    @Value("${object.store.s3.region:}")
    private String region;

    @Value("${object.store.connection.max.retry:20}")
    private int maxRetry;

    /**
     * Max SDK-level retries per S3 API call (separate from connection-establishment retries).
     * Keep LOW (default 3). Under high load S3 returns 503 SlowDown; aggressive SDK retries
     * cause exponential backoff storms that double response times.
     */
    @Value("${object.store.sdk.max.error.retry:3}")
    private int sdkMaxErrorRetry;

    @Value("${object.store.max.connection:200}")
    private int maxConnection;

    @Value("${object.store.connection.timeout:5000}")
    private int connectionTimeout;

    /**
     * Max time to wait for a free connection from the pool when all
     * {@link #maxConnection} leases are in use. Without this, Apache HttpClient blocks
     * indefinitely and can exhaust threads under load.
     */
    @Value("${object.store.connection.acquisition.timeout:10000}")
    private int connectionAcquisitionTimeout;

    /**
     * Socket (read) timeout per TCP read. Kept at 8s so a stalled S3 connection
     * fails fast rather than tying up a thread for the full apiCallTimeout budget.
     */
    @Value("${object.store.socket.timeout:8000}")
    private int socketTimeout;

    /**
     * Total per-request budget including SDK retries (maps to apiCallTimeout in SDK v2).
     * With sdkMaxErrorRetry=3 and socketTimeout=8s worst case ≈ 24s; keep this lower
     * so threads are released before upstream timeouts fire.
     */
    @Value("${object.store.client.execution.timeout:10000}")
    private int clientExecutionTimeout;

    @Value("${object.store.s3.use.account.as.bucketname:false}")
    private boolean useAccountAsBucketname;

    @Value("${object.store.s3.bucket-name-prefix:}")
    private String bucketNamePrefix;

    /**
     * volatile: writes from one thread are immediately visible to all others.
     * Without it, CPUs can cache the reference per-thread so 200 concurrent threads
     * all see null simultaneously and each create their own S3Client.
     */
    private volatile S3Client connection = null;

    /**
     * ReentrantLock instead of synchronized: virtual threads blocked here can unmount
     * from their carrier thread while waiting. synchronized pins virtual threads to
     * carrier threads, causing severe throughput degradation under high concurrency.
     */
    private final ReentrantLock connectionLock = new ReentrantLock();

    /**
     * ConcurrentHashMap-backed Set for O(1) thread-safe bucket existence cache.
     * Avoids a headBucket() S3 API call on every write after the first confirmation.
     */
    private final Set<String> existingBuckets = ConcurrentHashMap.newKeySet();

    private static final String SEPARATOR = "/";

    private static final String TAG_BACKWARD_COMPATIBILITY_ERROR = "Object-prefix is already an object, please choose a different object-prefix name";

    private static final String TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR = "Access Denied";

    private static final String DEFAULT_S3_REGION = "DEFAULT";

    /**
     * {@link Region} for {@link S3Client}, computed once at bean init from
     * {@code object.store.s3.region} (unchanged at runtime).
     */
    private Region resolvedS3Region;

    @PostConstruct
    void initResolvedS3Region() {
        resolvedS3Region = Region.of(resolveConfiguredRegionId());
    }

    /**
     * Region id from configuration, before {@link Region#of(String)}. AWS SDK for Java v2
     * does not allow a null or empty region id when building the client. When
     * {@code object.store.s3.region} is unset (empty default), blank after trimming,
     * Java {@code null}, or the literal string {@code "null"} (case-insensitive), returns
     * {@link #DEFAULT_S3_REGION} so S3-compatible endpoints still receive a valid id.
     */
    private String resolveConfiguredRegionId() {
        if (region == null) {
            return DEFAULT_S3_REGION;
        }
        String effectiveRegion = region.trim();
        if (effectiveRegion.isEmpty() || "null".equalsIgnoreCase(effectiveRegion)) {
            return DEFAULT_S3_REGION;
        }
        return effectiveRegion;
    }

    /**
     * Closes the S3Client and releases the underlying Apache HTTP connection pool.
     * Uses ReentrantLock so virtual threads can unmount while waiting.
     */
    private void shutdownConnection() {
        connectionLock.lock();
        try {
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                    LOGGER.warn(SESSIONID, REGISTRATIONID,
                            "Error closing S3 connection", ExceptionUtils.getStackTrace(e));
                } finally {
                    connection = null;
                    // Clear bucket cache — state may be stale after reconnect
                    existingBuckets.clear();
                }
            }
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void destroy() {
        shutdownConnection();
    }

    @Override
    public InputStream getObject(String account, String container, String source,
                                 String process, String objectName) {
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
            var response = getConnection(bucketName).getObject(
                    GetObjectRequest.builder().bucket(bucketName).key(finalObjectName).build());
            return new SafeS3InputStream(response);
        } catch (NoSuchKeyException e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Object not found in getObject for: " + objectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(),
                    e);
        } catch (S3Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "S3 error in getObject for: " + objectName + " | status: " + e.statusCode(),
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        } catch (Exception e) {
            shutdownConnection();
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Unexpected error in getObject for: " + objectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public boolean exists(String account, String container, String source,
                          String process, String objectName) {
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
            getConnection(bucketName).headObject(
                    HeadObjectRequest.builder().bucket(bucketName).key(finalObjectName).build());
            return true;
        } catch (NoSuchKeyException e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Object not found in exists for: " + objectName, ExceptionUtils.getStackTrace(e));
            return false;
        } catch (S3Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "S3 error in exists for: " + objectName + " | status: " + e.statusCode(),
                    ExceptionUtils.getStackTrace(e));
            throw e;
        } catch (Exception e) {
            shutdownConnection();
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Unexpected error in exists for: " + objectName, ExceptionUtils.getStackTrace(e));
            throw e;
        }
    }

    @Override
    public boolean putObject(String account, final String container, String source,
                             String process, String objectName, InputStream data) {
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
        S3Client client = getConnection(bucketName);
        try {
            ensureBucketExists(client, bucketName);
            client.putObject(
                    PutObjectRequest.builder().bucket(bucketName).key(finalObjectName).build(),
                    toRequestBody(data));
            return true;
        } catch (S3Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "S3 error in putObject for: " + objectName + " | status: " + e.statusCode(),
                    ExceptionUtils.getStackTrace(e));
            throw e;
        } catch (Exception e) {
            shutdownConnection();
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Unexpected error in putObject for: " + objectName, ExceptionUtils.getStackTrace(e));
            throw e;
        }
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source,
                                                 String process, String objectName,
                                                 Map<String, Object> metadata) {
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
            // Single client reference — prevents using two different instances if a
            // reconnect happens between the HEAD and COPY calls.
            S3Client client = getConnection(bucketName);

            // HEAD only — no content download (replaces the old GET + re-PUT approach
            // which downloaded and re-uploaded the full object body for metadata updates).
            HeadObjectResponse headResponse = client.headObject(
                    HeadObjectRequest.builder().bucket(bucketName).key(finalObjectName).build());

            Map<String, String> merged = new HashMap<>(headResponse.metadata());
            metadata.forEach((k, v) -> merged.put(k, v != null ? v.toString() : null));

            // Server-side copy with REPLACE directive — zero bytes transferred to/from client
            client.copyObject(CopyObjectRequest.builder()
                    .sourceBucket(bucketName).sourceKey(finalObjectName)
                    .destinationBucket(bucketName).destinationKey(finalObjectName)
                    .metadataDirective(MetadataDirective.REPLACE)
                    .metadata(merged)
                    .build());
            return metadata;
        } catch (S3Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "S3 error in addObjectMetaData for: " + objectName + " | status: " + e.statusCode(),
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        } catch (Exception e) {
            shutdownConnection();
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Unexpected error in addObjectMetaData for: " + objectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source,
                                                 String process, String objectName,
                                                 String key, String value) {
        Map<String, Object> meta = new HashMap<>();
        meta.put(key, value);
        String finalObjectName = useAccountAsBucketname
                ? ObjectStoreUtil.getName(container, source, process, objectName)
                : ObjectStoreUtil.getName(source, process, objectName);
        return addObjectMetaData(account, container, source, process, finalObjectName, meta);
    }

    @Override
    public Map<String, Object> getMetaData(String account, String container, String source,
                                           String process, String objectName) {
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
            HeadObjectResponse headResponse = getConnection(bucketName).headObject(
                    HeadObjectRequest.builder().bucket(bucketName).key(finalObjectName).build());
            if (headResponse.metadata() != null)
                headResponse.metadata().forEach(metaData::put);
            return metaData;
        } catch (NoSuchKeyException e) {
            // Normal "object not found" — connection is healthy, return empty map
            LOGGER.debug(SESSIONID, REGISTRATIONID, "Object not found in getMetaData for: " + objectName);
            return metaData;
        } catch (S3Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "S3 error in getMetaData for: " + objectName + " | status: " + e.statusCode(),
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        } catch (Exception e) {
            shutdownConnection();
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Unexpected error in getMetaData for: " + objectName, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    /**
     * WARNING — NOT safe for concurrent callers on the same objectName at high RPS.
     * S3 has no atomic increment: two threads reading the same value simultaneously
     * will both write n+1, silently losing one increment.
     * Callers must ensure single-writer per objectName at the application level.
     */
    @Override
    public Integer incMetadata(String account, String container, String source,
                               String process, String objectName, String metaDataKey) {
        Map<String, Object> metadata = getMetaData(account, container, source, process, objectName);
        if (metadata.get(metaDataKey) != null) {
            int newVal = Integer.parseInt(metadata.get(metaDataKey).toString()) + 1;
            metadata.put(metaDataKey, newVal);
            addObjectMetaData(account, container, source, process, objectName, metadata);
            return newVal;
        }
        return null;
    }

    /** WARNING — NOT safe for concurrent callers on the same objectName. See incMetadata. */
    @Override
    public Integer decMetadata(String account, String container, String source,
                               String process, String objectName, String metaDataKey) {
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
    public boolean deleteObject(String account, String container, String source,
                                String process, String objectName) {
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
            getConnection(bucketName).deleteObject(
                    DeleteObjectRequest.builder().bucket(bucketName).key(finalObjectName).build());
            return true;
        } catch (S3Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "S3 error in deleteObject for: " + objectName + " | status: " + e.statusCode(),
                    ExceptionUtils.getStackTrace(e));
            throw e;
        } catch (Exception e) {
            shutdownConnection();
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occured to deleteObject for : " + container,
                    ExceptionUtils.getStackTrace(e));
            throw e;
        }
    }

    @Override
    public boolean removeContainer(String account, String container,
                                   String source, String process) {
        return false;
    }

    @Override
    public boolean pack(String account, String container,
                        String source, String process, String refId) {
        return false;
    }

    /**
     * Double-checked locking with ReentrantLock for a thread-safe singleton S3Client.
     *
     * Why ReentrantLock instead of synchronized:
     *   Virtual threads blocked on a ReentrantLock unmount from their carrier thread
     *   while waiting. synchronized pins virtual threads to carriers, which serialises
     *   all other virtual threads on that carrier — catastrophic at 400 RPS.
     *
     * Why volatile on the connection field:
     *   Without it, CPUs may cache the reference per-thread. Two hundred concurrent
     *   threads could all read null and each begin building an S3Client.
     */
    private S3Client getConnection(String bucketName) {
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
                    // Assign to connection immediately so shutdownConnection() can close it
                    // if the connectivity test below throws a non-transient exception.
                    connection = S3Client.builder()
                            .credentialsProvider(StaticCredentialsProvider.create(
                                    AwsBasicCredentials.create(accessKey, secretKey)))
                            .endpointOverride(URI.create(url))
                            .region(resolvedS3Region)
                            .serviceConfiguration(S3Configuration.builder()
                                    .pathStyleAccessEnabled(true)
                                    .build())
                            .httpClientBuilder(ApacheHttpClient.builder()
                                    .maxConnections(maxConnection)
                                    .connectionTimeout(Duration.ofMillis(connectionTimeout))
                                    .connectionAcquisitionTimeout(
                                            Duration.ofMillis(connectionAcquisitionTimeout))
                                    .socketTimeout(Duration.ofMillis(socketTimeout)))
                            .overrideConfiguration(ClientOverrideConfiguration.builder()
                                    .apiCallTimeout(Duration.ofMillis(clientExecutionTimeout))
                                    .retryPolicy(RetryPolicy.builder()
                                            .numRetries(sdkMaxErrorRetry)
                                            .build())
                                    .build())
                            .build();

                    // Connectivity test — NoSuchBucketException means S3 is reachable but
                    // bucket doesn't exist yet, which is fine at startup.
                    try {
                        connection.headBucket(
                                HeadBucketRequest.builder().bucket(bucketName).build());
                    } catch (NoSuchBucketException ignored) {
                        // Bucket not created yet — connection is healthy
                    }

                    return connection;

                } catch (Exception e) {
                    shutdownConnection();
                    LOGGER.error(SESSIONID, REGISTRATIONID,
                            "Exception occurred while obtaining connection for " + bucketName
                                    + ". Will try again. Retry count : " + attempt,
                            ExceptionUtils.getStackTrace(e));

                    if (attempt >= maxRetry) {
                        LOGGER.error(SESSIONID, REGISTRATIONID,
                                "Maximum retry limit exceeded. Could not obtain connection for "
                                        + bucketName + ". Retry count: " + attempt);
                        throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                                OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
                    }

                    // Exponential backoff with jitter: 200ms, 400ms, 800ms … capped at 5s.
                    // Without backoff, 200 threads × 20 retries = 4000 rapid S3 requests,
                    // triggering 503 SlowDown which causes SDK retries that double response times.
                    try {
                        long backoffMs = Math.min(200L * (1L << (attempt - 1)), 5000L);
                        backoffMs += ThreadLocalRandom.current().nextLong(100); // jitter
                        Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                                OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), ie);
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
        // Process each page immediately (via paginator) instead of accumulating all summaries.
        // Collecting all pages first holds every S3Object in heap simultaneously:
        // 1M objects × ~300B per summary ≈ 300 MB per concurrent call at 400 RPS.
        List<ObjectDto> objectDtos = new ArrayList<>();

        try {
            if (useAccountAsBucketname) {
                String bucketName = normalizeBucket(account);
                String searchPattern = id + SEPARATOR;
                getConnection(bucketName)
                        .listObjectsV2Paginator(ListObjectsV2Request.builder()
                                .bucket(bucketName).prefix(searchPattern).build())
                        .forEach(page -> collectObjectDtos(page.contents(), objectDtos));
            } else {
                String bucketName = normalizeBucket(id);
                getConnection(bucketName)
                        .listObjectsV2Paginator(ListObjectsV2Request.builder()
                                .bucket(bucketName).build())
                        .forEach(page -> collectObjectDtos(page.contents(), objectDtos));
            }

            return objectDtos.isEmpty() ? null : objectDtos;

        } catch (Exception e) {

            shutdownConnection();
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occurred in getAllObjects for: " + id,
                    ExceptionUtils.getStackTrace(e));
            throw e;
        }
    }

    /**
     * Converts one page of SDK v2 S3Object summaries into ObjectDtos and appends them.
     * Called per page so the previous page's summaries are GC-eligible immediately.
     *
     * Note: SDK v2 S3Object.lastModified() returns Instant; converted to Date for ObjectDto.
     */
    private void collectObjectDtos(List<S3Object> summaries, List<ObjectDto> objectDtos) {
        for (S3Object s3Obj : summaries) {
            String[] tempKeys = s3Obj.key().split("/");
            if (useAccountAsBucketname) {
                if (tempKeys.length > 1 && tempKeys[1] != null && tempKeys[1].endsWith(TAGS_FILENAME))
                    continue;
            } else {
                if (tempKeys.length > 0 && tempKeys[0] != null && tempKeys[0].endsWith(TAGS_FILENAME))
                    continue;
            }
            String[] keys = removeIdFromObjectPath(useAccountAsBucketname, tempKeys);
            if (ArrayUtils.isNotEmpty(keys)) {
                ObjectDto dto = null;
                Date lastModified = Date.from(s3Obj.lastModified());
                switch (keys.length) {
                    case 1: dto = new ObjectDto(null, null, keys[0], lastModified); break;
                    case 2: dto = new ObjectDto(keys[0], null, keys[1], lastModified); break;
                    case 3: dto = new ObjectDto(keys[0], keys[1], keys[2], lastModified); break;
                }
                if (dto != null)
                    objectDtos.add(dto);
            }
        }
    }

    private String[] removeIdFromObjectPath(boolean useAccountAsBucketname, String[] keys) {
        return (useAccountAsBucketname && ArrayUtils.isNotEmpty(keys))
                ? (String[]) ArrayUtils.remove(keys, 0) : keys;
    }

    @Override
    public Map<String, String> addTags(String account, String container, Map<String, String> tags) {
        return addTagsInternal(account, container, tags, false);
    }

    /**
     * @param backwardCompatRetry true after the first backward-compat cleanup attempt.
     *   Prevents unbounded recursion if the error recurs after the legacy object is deleted
     *   (would indicate a permissions issue, not stale data).
     */
    private Map<String, String> addTagsInternal(String account, String container,
                                                Map<String, String> tags,
                                                boolean backwardCompatRetry) {
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
        S3Client client = getConnection(bucketName);
        try {
            ensureBucketExists(client, bucketName);
            for (Entry<String, String> entry : tags.entrySet()) {
                String tagName = ObjectStoreUtil.getName(finalObjectName, entry.getKey());
                // getBytes(UTF_8) is exact — contentLength is known, no SDK buffering occurs
                byte[] tagBytes = entry.getValue().getBytes(StandardCharsets.UTF_8);
                try {
                    client.putObject(
                            PutObjectRequest.builder()
                                    .bucket(bucketName).key(tagName)
                                    .contentLength((long) tagBytes.length)
                                    .build(),
                            RequestBody.fromBytes(tagBytes));
                } catch (S3Exception e) {
                    if (!backwardCompatRetry
                            && (e.getMessage().contains(TAG_BACKWARD_COMPATIBILITY_ERROR)
                                || e.getMessage().contains(TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR))) {
                        // Legacy: a plain object exists at the prefix key. Delete it and retry once.
                        try {
                            client.headObject(HeadObjectRequest.builder()
                                    .bucket(bucketName).key(finalObjectName).build());
                            client.deleteObject(DeleteObjectRequest.builder()
                                    .bucket(bucketName).key(finalObjectName).build());
                            return addTagsInternal(account, container, tags, true);
                        } catch (NoSuchKeyException ignored) {
                            // Legacy prefix object not found — fall through to re-throw original error
                        }
                    }
                    LOGGER.error(SESSIONID, REGISTRATIONID,
                            "S3 error in addTags for: " + container + " | status: " + e.statusCode(),
                            ExceptionUtils.getStackTrace(e));
                    throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                            OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
                }
            }
        } catch (ObjectStoreAdapterException e) {
            throw e;
        } catch (Exception e) {
            shutdownConnection();
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Unexpected error in addTags for: " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
        return tags;
    }

    @Override
    public Map<String, String> getTags(String account, String container) {
        Map<String, String> objectTags = new HashMap<>();
        String bucketName;
        String prefix;
        if (useAccountAsBucketname) {
            bucketName = account;
            prefix = ObjectStoreUtil.getName(container, null, TAGS_FILENAME) + SEPARATOR;
        } else {
            bucketName = container;
            prefix = TAGS_FILENAME + SEPARATOR;
        }
        bucketName = normalizeBucket(bucketName);
        try {
            S3Client client = getConnection(bucketName);
            List<String> tagNames = new ArrayList<>();

            // Paginator handles continuation automatically — no manual token management.
            // Prefix filter ensures we only scan tag objects, not the entire bucket.
            // Serial fetch (not parallelStream) avoids ForkJoinPool thread starvation at 400 RPS.
            ListObjectsV2Request.Builder listReq = ListObjectsV2Request.builder().bucket(bucketName);
            if (useAccountAsBucketname)
                listReq.prefix(prefix);
            client.listObjectsV2Paginator(listReq.build())
                    .forEach(page -> page.contents().forEach(s3Obj -> {
                        String[] keys = s3Obj.key().split("/");
                        if (ArrayUtils.isNotEmpty(keys)) {
                            if (useAccountAsBucketname) {
                                if (keys.length > 1 && keys[1] != null
                                        && keys[1].endsWith(TAGS_FILENAME)
                                        && keys.length > 2)
                                    tagNames.add(keys[2]);
                            } else {
                                if (keys[0] != null && keys[0].endsWith(TAGS_FILENAME)
                                        && keys.length > 1)
                                    tagNames.add(keys[1]);
                            }
                        }
                    }));

            for (String tagName : tagNames) {
                objectTags.put(tagName,
                        client.getObjectAsBytes(GetObjectRequest.builder()
                                        .bucket(bucketName).key(prefix + tagName).build())
                                .asUtf8String());
            }
            return objectTags;
        } catch (S3Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "S3 error in getTags for: " + container + " | status: " + e.statusCode(),
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        } catch (Exception e) {
            shutdownConnection();
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Unexpected error in getTags for: " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
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
        bucketName = normalizeBucket(bucketName);
        S3Client client = getConnection(bucketName);
        try {
            ensureBucketExists(client, bucketName);
            for (String tag : tags) {
                client.deleteObject(DeleteObjectRequest.builder()
                        .bucket(bucketName)
                        .key(ObjectStoreUtil.getName(finalObjectName, tag))
                        .build());
            }
        } catch (S3Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "S3 error in deleteTags for: " + container + " | status: " + e.statusCode(),
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        } catch (Exception e) {
            shutdownConnection();
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Unexpected error in deleteTags for: " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    /**
     * Ensures a bucket exists:
     * <ul>
     *   <li>{@code useAccountAsBucketname == true}: in-memory set skips repeat {@code headBucket}
     *       after the first successful check/create for that bucket name (cleared on reconnect).</li>
     *   <li>{@code useAccountAsBucketname == false}: no cache — every call performs {@code headBucket}
     *       (same as upstream always calling {@code doesBucketExistV2}).</li>
     * </ul>
     * Concurrent first-writers may both attempt {@code createBucket}; the loser receives
     * {@link BucketAlreadyOwnedByYouException}, which is treated as success.
     */
    private void ensureBucketExists(S3Client client, String bucketName) {
        if (useAccountAsBucketname && existingBuckets.contains(bucketName))
            return;

        try {
            client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
        } catch (NoSuchBucketException e) {
            try {
                client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
            } catch (BucketAlreadyOwnedByYouException race) {
                LOGGER.debug(SESSIONID, REGISTRATIONID,
                        "createBucket race resolved: bucket already owned, bucket=" + bucketName);
            }
        }

        if (useAccountAsBucketname)
            existingBuckets.add(bucketName);
    }

    /**
     * Converts an InputStream to an AWS SDK v2 {@link RequestBody} by reading the stream to completion
     * via {@link InputStream#readAllBytes()} and wrapping the result with {@link RequestBody#fromBytes(byte[])}.
     *
     * <p>This materializes the full payload in memory so the SDK receives a known content length.
     * {@link IOException} from reading the stream is wrapped in {@link ObjectStoreAdapterException}.
     */
    private RequestBody toRequestBody(InputStream data) {
        try {
            // Fast path: ByteArrayInputStream and most MOSIP streams report exact size.
            // fromInputStream() streams directly without a full heap copy.
            int available = data.available();
            if (available > 0)
                return RequestBody.fromInputStream(data, available);
            // Fallback: unknown-length streams must be buffered; SDK v2 requires content-length.
            return RequestBody.fromBytes(data.readAllBytes());
        } catch (IOException e) {
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }
    /**
     * Applies bucket prefix and lowercases per S3 naming rules.
     */
    private String normalizeBucket(String bucketName) {
        if (bucketName.startsWith(bucketNamePrefix)) {
            LOGGER.debug("Already bucketName with prefix is present" + bucketName);
        } else {
            bucketName = bucketNamePrefix + bucketName;
            LOGGER.debug("Adding  Prefix to bucketName" + bucketName);
        }
        return bucketName.toLowerCase();
    }
}
