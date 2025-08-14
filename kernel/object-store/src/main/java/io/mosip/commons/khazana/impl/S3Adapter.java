package io.mosip.commons.khazana.impl;

import static io.mosip.commons.khazana.config.LoggerConfiguration.REGISTRATIONID;
import static io.mosip.commons.khazana.config.LoggerConfiguration.SESSIONID;
import static io.mosip.commons.khazana.constant.KhazanaConstant.TAGS_FILENAME;
import static io.mosip.commons.khazana.constant.KhazanaErrorCodes.OBJECT_STORE_NOT_ACCESSIBLE;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import org.apache.commons.lang.ArrayUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
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

    // Keeping existing config fields for compatibility (even if not all are used directly now)
    @Value("${object.store.s3.readlimit:10000000}")
    private int readlimit;
    @Value("${object.store.connection.max.retry:20}")
    private int maxRetry; // SDK-level retry used instead of manual sleeps
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

    private static final String SEPARATOR = "/";
    private static final String TAG_BACKWARD_COMPATIBILITY_ERROR = "Object-prefix is already an object, please choose a different object-prefix name";
    private static final String TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR = "Access Denied";

    private final Set<String> existingBuckets = ConcurrentHashMap.newKeySet();
    private final ConcurrentMap<String, Boolean> bucketExistCache = new ConcurrentHashMap<>();
    private volatile S3Client s3Client;

    /* ======================= Client ======================= */

    private S3Client getS3Client() {
        if (s3Client == null) {
            synchronized (this) {
                if (s3Client == null) {
                    s3Client = S3Client.builder()
                            .endpointOverride(java.net.URI.create(url))
                            .region(Region.of(region))
                            .credentialsProvider(StaticCredentialsProvider.create(
                                    AwsBasicCredentials.create(accessKey, secretKey)))
                            .httpClientBuilder(ApacheHttpClient.builder()
                                    .maxConnections(Math.max(maxConnection, 500)) // boost pool
                                    .connectionTimeout(Duration.ofMillis(connectionTimeout))
                                    .socketTimeout(Duration.ofMillis(socketTimeout))
                                    .tcpKeepAlive(true)) // keep TCP connections alive
                            .build();
                }
            }
        }
        return s3Client;
    }

    private String addBucketPrefix(String bucketName) {
        if (bucketNamePrefix != null && !bucketNamePrefix.isEmpty() && !bucketName.startsWith(bucketNamePrefix)) {
            LOGGER.debug("Adding Prefix to bucketName " + bucketNamePrefix + bucketName);
            return bucketNamePrefix + bucketName;
        }
        return bucketName;
    }

    private boolean doesBucketExist(String bucketName) {
        // keep behavior but add strong caching to avoid repeated HEAD calls
        if (existingBuckets.contains(bucketName)) return true;
        return bucketExistCache.computeIfAbsent(bucketName, bn -> {
            try {
                getS3Client().headBucket(HeadBucketRequest.builder().bucket(bn).build());
                existingBuckets.add(bn);
                return true;
            } catch (S3Exception e) {
                return false;
            }
        });
    }

    private void ensureBucket(String bucketName) {
        if (!doesBucketExist(bucketName)) {
            getS3Client().createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
            existingBuckets.add(bucketName);
            bucketExistCache.put(bucketName, true);
        }
    }

    private String resolveBucketName(String account, String container) {
        return addBucketPrefix(useAccountAsBucketname ? account : container).toLowerCase();
    }

    private String resolveObjectKey(String account, String container, String source, String process, String objectName) {
        return useAccountAsBucketname
                ? ObjectStoreUtil.getName(container, source, process, objectName)
                : ObjectStoreUtil.getName(source, process, objectName);
    }

    /* ======================= Core Ops ======================= */

    @Override
    public InputStream getObject(String account, String container, String source, String process, String objectName) {
        String bucketName = resolveBucketName(account, container);
        String finalObjectName = resolveObjectKey(account, container, source, process, objectName);
        try {
            GetObjectRequest req = GetObjectRequest.builder().bucket(bucketName).key(finalObjectName).build();
            return getS3Client().getObject(req); // streaming; caller must close
        } catch (S3Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "S3Exception during getObject for: " + container, ExceptionUtils.getStackTrace(e));
            return null;
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception during getObject for: " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public boolean exists(String account, String container, String source, String process, String objectName) {
        String bucketName = resolveBucketName(account, container);
        String finalObjectName = resolveObjectKey(account, container, source, process, objectName);
        try {
            getS3Client().headObject(HeadObjectRequest.builder().bucket(bucketName).key(finalObjectName).build());
            return true;
        } catch (S3Exception e) {
            return false;
        }
    }

    @Override
    public boolean putObject(String account, String container, String source, String process,
                             String objectName, InputStream data) {
        String bucketName = resolveBucketName(account, container);
        String finalObjectName = resolveObjectKey(account, container, source, process, objectName);

        final int partSize = 5 * 1024 * 1024; // 5MB minimum for S3
        final int parallelism = Math.min(Runtime.getRuntime().availableProcessors(), 8); // cap at 8 threads

        ExecutorService executor = Executors.newFixedThreadPool(parallelism);
        List<Future<CompletedPart>> futures = new ArrayList<>();
        S3Client client = getS3Client();

        try {
            ensureBucket(bucketName);

            // 1. Initiate multipart upload
            CreateMultipartUploadResponse createResp = client.createMultipartUpload(
                    CreateMultipartUploadRequest.builder()
                            .bucket(bucketName)
                            .key(finalObjectName)
                            .build());
            String uploadId = createResp.uploadId();

            try {
                byte[] buffer = new byte[partSize];
                int bytesRead;
                int partNumber = 1;

                while ((bytesRead = readFully(data, buffer)) > 0) {
                    final int currentPart = partNumber;
                    final byte[] partBytes = Arrays.copyOf(buffer, bytesRead);

                    // Submit upload task in parallel
                    futures.add(executor.submit(() -> {
                        UploadPartResponse uploadPartResponse = client.uploadPart(
                                UploadPartRequest.builder()
                                        .bucket(bucketName)
                                        .key(finalObjectName)
                                        .uploadId(uploadId)
                                        .partNumber(currentPart)
                                        .contentLength((long) partBytes.length)
                                        .build(),
                                RequestBody.fromBytes(partBytes)
                        );
                        return CompletedPart.builder()
                                .partNumber(currentPart)
                                .eTag(uploadPartResponse.eTag())
                                .build();
                    }));

                    partNumber++;
                }

                // Wait for all uploads to finish
                List<CompletedPart> completedParts = new ArrayList<>();
                for (Future<CompletedPart> f : futures) {
                    completedParts.add(f.get());
                }

                // Sort parts (S3 requires ascending part numbers)
                completedParts.sort(Comparator.comparingInt(CompletedPart::partNumber));

                // 3. Complete upload
                client.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                        .bucket(bucketName)
                        .key(finalObjectName)
                        .uploadId(uploadId)
                        .multipartUpload(CompletedMultipartUpload.builder()
                                .parts(completedParts)
                                .build())
                        .build());

            } catch (Exception ex) {
                // Abort upload if anything fails
                client.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                        .bucket(bucketName)
                        .key(finalObjectName)
                        .uploadId(uploadId)
                        .build());
                throw ex;
            }

            return true;

        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception during putObject for: " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        } finally {
            executor.shutdown();
        }
    }

    /*   @Override
    public boolean putObject(String account, String container, String source, String process,
                             String objectName, InputStream data) {
        String bucketName = resolveBucketName(account, container);
        String finalObjectName = resolveObjectKey(account, container, source, process, objectName);

        try {
            ensureBucket(bucketName);

            final int partSize = 5 * 1024 * 1024; // 5MB min for S3 multipart
            S3Client client = getS3Client();

            // 1. Initiate multipart upload
            CreateMultipartUploadResponse createResp = client.createMultipartUpload(
                    CreateMultipartUploadRequest.builder()
                            .bucket(bucketName)
                            .key(finalObjectName)
                            .build());
            String uploadId = createResp.uploadId();
            List<CompletedPart> completedParts = new ArrayList<>();

            try {
                byte[] buffer = new byte[partSize];
                int bytesRead;
                int partNumber = 1;

                while ((bytesRead = readFully(data, buffer)) > 0) {
                    byte[] partBytes = (bytesRead == buffer.length) ? buffer : Arrays.copyOf(buffer, bytesRead);

                    UploadPartResponse uploadPartResponse = client.uploadPart(
                            UploadPartRequest.builder()
                                    .bucket(bucketName)
                                    .key(finalObjectName)
                                    .uploadId(uploadId)
                                    .partNumber(partNumber)
                                    .contentLength((long) bytesRead)
                                    .build(),
                            RequestBody.fromBytes(partBytes)
                    );

                    completedParts.add(CompletedPart.builder()
                            .partNumber(partNumber)
                            .eTag(uploadPartResponse.eTag())
                            .build());

                    partNumber++;
                }

                // 3. Complete multipart upload
                client.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                        .bucket(bucketName)
                        .key(finalObjectName)
                        .uploadId(uploadId)
                        .multipartUpload(CompletedMultipartUpload.builder()
                                .parts(completedParts)
                                .build())
                        .build());

            } catch (Exception ex) {
                // Abort upload on any error
                client.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                        .bucket(bucketName)
                        .key(finalObjectName)
                        .uploadId(uploadId)
                        .build());
                throw ex;
            }

            return true;

        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception during putObject for: " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }*/

    private int readFully(InputStream in, byte[] buffer) throws IOException {
        int totalRead = 0;
        while (totalRead < buffer.length) {
            int bytesRead = in.read(buffer, totalRead, buffer.length - totalRead);
            if (bytesRead == -1) break;
            totalRead += bytesRead;
        }
        return totalRead;
    }


    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process, String objectName, Map<String, Object> metadata) {
        String bucketName = resolveBucketName(account, container);
        String finalObjectName = resolveObjectKey(account, container, source, process, objectName);
        S3Client client = getS3Client();
        try {
            // Get current metadata then REPLACE via in-place copy
            HeadObjectResponse head = client.headObject(HeadObjectRequest.builder().bucket(bucketName).key(finalObjectName).build());
            Map<String, String> userMeta = new HashMap<>();
            if (head.metadata() != null) userMeta.putAll(head.metadata());
            metadata.forEach((k, v) -> userMeta.put(k, v != null ? v.toString() : null));

            client.copyObject(CopyObjectRequest.builder()
                    .sourceBucket(bucketName)
                    .sourceKey(finalObjectName)
                    .destinationBucket(bucketName)
                    .destinationKey(finalObjectName)
                    .metadata(userMeta)
                    .metadataDirective(MetadataDirective.REPLACE)
                    .build());
            return metadata;
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured to addObjectMetaData for : " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process, String objectName, String key, String value) {
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
    public Map<String, Object> getMetaData(String account, String container, String source, String process, String objectName) {
        String bucketName = resolveBucketName(account, container);
        String finalObjectName = resolveObjectKey(account, container, source, process, objectName);
        try {
            HeadObjectResponse head = getS3Client().headObject(HeadObjectRequest.builder().bucket(bucketName).key(finalObjectName).build());
            Map<String, Object> metaData = new HashMap<>();
            if (head.metadata() != null) metaData.putAll(head.metadata());
            return metaData;
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,"Exception occured to getMetaData for : " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public Integer incMetadata(String account, String container, String source, String process, String objectName, String metaDataKey) {
        Map<String, Object> metadata = getMetaData(account, container, source, process, objectName);
        if (metadata.get(metaDataKey) != null) {
            metadata.put(metaDataKey, Integer.valueOf(metadata.get(metaDataKey).toString()) + 1);
            addObjectMetaData(account, container, source, process, objectName, metadata);
            return Integer.valueOf(metadata.get(metaDataKey).toString());
        }
        return null;
    }

    @Override
    public Integer decMetadata(String account, String container, String source, String process, String objectName, String metaDataKey) {
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
        String bucketName = resolveBucketName(account, container);
        String finalObjectName = resolveObjectKey(account, container, source, process, objectName);
        try {
            getS3Client().deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(finalObjectName).build());
            return true;
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured to deleteObject for : " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public boolean removeContainer(String account, String container, String source, String process) {
        String bucketName = useAccountAsBucketname ? addBucketPrefix(account).toLowerCase() : addBucketPrefix(container).toLowerCase();
        try {
            getS3Client().deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build());
            existingBuckets.remove(bucketName);
            bucketExistCache.remove(bucketName);
            return true;
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured to removeContainer for : " + container, ExceptionUtils.getStackTrace(e));
            return false;
        }
    }

    @Override
    public boolean pack(String account, String container, String source, String process, String refId) {
        // Packing logic if required (not implemented)
        return false;
    }

    @Override
    public List<ObjectDto> getAllObjects(String account, String id) {
        List<S3Object> os;
        if (useAccountAsBucketname) {
            String searchPattern = id + SEPARATOR;
            account = addBucketPrefix(account).toLowerCase();
            os = getS3Client()
                    .listObjectsV2(ListObjectsV2Request.builder().bucket(account).prefix(searchPattern).build())
                    .contents();
        } else {
            id = addBucketPrefix(id).toLowerCase();
            os = getS3Client()
                    .listObjectsV2(ListObjectsV2Request.builder().bucket(id).build())
                    .contents();
        }
        if (os == null || os.isEmpty()) {
            return null;
        }

        List<ObjectDto> objectDtos = new ArrayList<>();
        for (S3Object o : os) {
            String[] tempKeys = o.key().split("/");
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
                if (objectDto != null) objectDtos.add(objectDto);
            }
        }
        return objectDtos;
    }

    private String[] removeIdFromObjectPath(boolean useAccountAsBucketname, String[] keys) {
        return (useAccountAsBucketname && ArrayUtils.isNotEmpty(keys)) ?
                (String[]) ArrayUtils.remove(keys, 0) : keys;
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

            ensureBucket(bucketName);

            for (Map.Entry<String, String> entry : tags.entrySet()) {
                String tagName = ObjectStoreUtil.getName(finalObjectName, entry.getKey());
                try {
                    getS3Client().putObject(
                            PutObjectRequest.builder().bucket(bucketName).key(tagName).build(),
                            RequestBody.fromString(entry.getValue(), StandardCharsets.UTF_8)
                    );
                } catch (S3Exception e) {
                    // Backward compatibility handling
                    String msg = e.getMessage() != null ? e.getMessage() : "";
                    if (msg.contains(TAG_BACKWARD_COMPATIBILITY_ERROR) || msg.contains(TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR)) {
                        ListObjectsV2Response res = getS3Client().listObjectsV2(
                                ListObjectsV2Request.builder().bucket(bucketName).prefix(finalObjectName).build());
                        if (res != null && res.keyCount() > 0) {
                            getS3Client().deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(finalObjectName).build());
                            addTags(account, container, tags); // retry once via recursion (same signature)
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

            List<S3Object> objectSummary;
            if (useAccountAsBucketname)
                objectSummary = getS3Client().listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).prefix(finalObjectName).build()).contents();
            else
                objectSummary = getS3Client().listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).build()).contents();

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
                objectTags.put(tagName, getS3Client()
                        .getObjectAsBytes(GetObjectRequest.builder().bucket(bucketName).key(finalObjectName + tagName).build())
                        .asUtf8String());
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

            ensureBucket(bucketName);

            for (String tag : tags) {
                String tagName = ObjectStoreUtil.getName(finalObjectName, tag);
                getS3Client().deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(tagName).build());
            }
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured while deleteTags for : " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    /* ======================= Helpers ======================= */

    /**
     * Streams the incoming data into a temporary file to avoid loading the entire content into heap.
     * This gives us a reliable content length and enables zero-copy file upload via SDK.
     */
    private Path bufferToTempFile(InputStream in) throws IOException {
        Path tmp = Files.createTempFile("s3adapter-upload-", ".bin");
        try (InputStream src = in) {
            Files.copy(src, tmp, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }
        return tmp;
    }

    private void safeDelete(Path p) {
        if (p == null) return;
        try {
            Files.deleteIfExists(p);
        } catch (IOException ignore) { /* best-effort */ }
    }
}
