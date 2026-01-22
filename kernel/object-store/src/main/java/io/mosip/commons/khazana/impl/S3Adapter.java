package io.mosip.commons.khazana.impl;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import io.mosip.commons.khazana.config.LoggerConfiguration;
import io.mosip.commons.khazana.dto.ObjectDto;
import io.mosip.commons.khazana.exception.ObjectStoreAdapterException;
import io.mosip.commons.khazana.spi.ObjectStoreAdapter;
import io.mosip.commons.khazana.util.ObjectStoreUtil;
import io.mosip.kernel.core.logger.spi.Logger;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Component("S3Adapter")
public class S3Adapter implements ObjectStoreAdapter {

    private final Logger LOGGER = LoggerConfiguration.logConfig(S3Adapter.class);

    @Value("${object.store.s3.accesskey:accesskey:accesskey}")
    private String accessKey;

    @Value("${object.store.s3.secretkey:secretkey:secretkey}")
    private String secretKey;

    @Value("${object.store.s3.region:ap-south-1}")
    private String region;

    @Value("${object.store.s3.connection.max:250}")
    private int maxConnections;

    @Value("${object.store.s3.multipart.threshold:5242880}") // 5MB
    private long multipartThreshold;

    @Value("${object.store.s3.multipart.partsizemb:5}")
    private int partSizeMb;

    @Value("${object.store.s3.use.account.as.bucketname:false}")
    private boolean useAccountAsBucketname;

    @Value("${object.store.s3.bucket-name-prefix:}")
    private String bucketNamePrefix;

    private AmazonS3 s3Client;
    private TransferManager transferManager;

    private static final String TAGS_FILENAME = "tags";
    private static final String SEPARATOR = "/";
    // Optional for backward compat. If you had error string constants, define as needed.

    @PostConstruct
    public void init() {
        try {
            AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
            ClientConfiguration clientConfig = new ClientConfiguration()
                    .withMaxConnections(maxConnections)
                    .withConnectionTimeout(10000)
                    .withSocketTimeout(60000)
                    .withTcpKeepAlive(true)
                    .withConnectionTTL(60000)
                    .withConnectionMaxIdleMillis(60000);

            s3Client = AmazonS3ClientBuilder.standard()
                    .withClientConfiguration(clientConfig)
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .withRegion(region)
                    .build();

            transferManager = TransferManagerBuilder.standard()
                    .withS3Client(s3Client)
                    .withMultipartUploadThreshold(multipartThreshold)
                    .withMinimumUploadPartSize(partSizeMb * 1024L * 1024L)
                    .build();

            LOGGER.info("FastS3Adapter initialized | region: {} | max connections: {}", region, maxConnections);
        } catch (Exception e) {
            LOGGER.error("Failed to initialize S3 client", e);
            throw new RuntimeException("S3 Adapter initialization failed", e);
        }
    }

    @PreDestroy
    public void shutdown() {
        if (transferManager != null) {
            transferManager.shutdownNow(false);
        }
        if (s3Client != null) {
            s3Client.shutdown();
        }
    }


    // ───────────── Classic MOSIP key logic ─────────────

    private String addBucketPrefix(String bucketName) {
        if (bucketNamePrefix == null || bucketNamePrefix.isEmpty() || bucketName.startsWith(bucketNamePrefix)) {
            return bucketName;
        }
        return bucketNamePrefix + bucketName;
    }

    private String getBucket(String account, String container) {
        if (useAccountAsBucketname) {
            return addBucketPrefix(account).toLowerCase();
        } else {
            return addBucketPrefix(container).toLowerCase();
        }
    }

    /**
     * Builds the full S3 object key as per MOSIP style.
     * Use ObjectStoreUtil.getName(...) as in your original adapter.
     * If you need a custom implementation, provide it.
     */
    private String getS3ObjectKey(String container, String source, String process, String objectName) {
        if (useAccountAsBucketname) {
            return ObjectStoreUtil.getName(container, source, process, objectName);
        } else {
            return ObjectStoreUtil.getName(source, process, objectName);
        }
    }

    // ───────────── Core Adapter API ─────────────

    @Override
    public InputStream getObject(String account, String container, String source, String process, String objectName) {
        String bucket = getBucket(account, container);
        String key = getS3ObjectKey(container, source, process, objectName);
        try {
            S3Object s3Object = s3Client.getObject(bucket, key);
            return s3Object.getObjectContent();
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) return null;
            LOGGER.error("getObject failed for bucket/key: {}/{}", bucket, key, e);
            throw new ObjectStoreAdapterException("Failed to get object", e.getMessage());
        }
    }

    @Override
    public boolean exists(String account, String container, String source, String process, String objectName) {
        String bucket = getBucket(account, container);
        String key = getS3ObjectKey(container, source, process, objectName);
        try {
            s3Client.getObjectMetadata(bucket, key);
            return true;
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) return false;
            throw new ObjectStoreAdapterException("Failed to check existence", e.getErrorMessage());
        }
    }

    @Override
    public boolean putObject(String account, String container, String source, String process, String objectName, InputStream data) {
        String bucket = getBucket(account, container);
        String key = getS3ObjectKey(container, source, process, objectName);
        try {
            // For maximum compatibility we do not attempt to fetch content length from InputStream. You may extend.
            PutObjectRequest req = new PutObjectRequest(bucket, key, data, new ObjectMetadata())
                    .withCannedAcl(CannedAccessControlList.Private);

            Upload upload = transferManager.upload(req);
            upload.waitForCompletion();
            return true;
        } catch (Exception e) {
            LOGGER.error("putObject failed for bucket/key: {}/{}", bucket, key, e);
            throw new ObjectStoreAdapterException("Failed to store object", e.getMessage());
        }
    }

    @Override
    public boolean deleteObject(String account, String container, String source, String process, String objectName) {
        String bucket = getBucket(account, container);
        String key = getS3ObjectKey(container, source, process, objectName);
        try {
            s3Client.deleteObject(bucket, key);
            return true;
        } catch (Exception e) {
            LOGGER.error("deleteObject failed for bucket/key: {}/{}", bucket, key, e);
            return false;
        }
    }

    // ■■■ Classic metadata API with full user-meta copying ■■■

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process,
                                                 String objectName, Map<String, Object> metadata) {
        String bucket = getBucket(account, container);
        String key = getS3ObjectKey(container, source, process, objectName);
        try {
            ObjectMetadata orig = s3Client.getObjectMetadata(bucket, key);
            Map<String, String> merged = orig.getUserMetadata() == null ? new HashMap<>() : new HashMap<>(orig.getUserMetadata());
            for (Map.Entry<String, Object> e : metadata.entrySet()) {
                merged.put(e.getKey(), Objects.toString(e.getValue(), null));
            }

            ObjectMetadata newMeta = new ObjectMetadata();
            newMeta.setUserMetadata(merged);
            // preserve content type etc as needed (optional)

            CopyObjectRequest copyReq = new CopyObjectRequest(bucket, key, bucket, key).withNewObjectMetadata(newMeta);
            s3Client.copyObject(copyReq);
            return metadata;
        } catch (Exception e) {
            LOGGER.error("addObjectMetaData failed for bucket/key: {}/{}", bucket, key, e);
            throw new ObjectStoreAdapterException("Failed to add metadata", e.getMessage());
        }
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process,
                                                 String objectName, String key, String value) {
        Map<String, Object> meta = new HashMap<>();
        meta.put(key, value);
        return addObjectMetaData(account, container, source, process, objectName, meta);
    }

    @Override
    public Map<String, Object> getMetaData(String account, String container, String source, String process, String objectName) {
        String bucket = getBucket(account, container);
        String key = getS3ObjectKey(container, source, process, objectName);
        try {
            ObjectMetadata meta = s3Client.getObjectMetadata(bucket, key);
            Map<String, Object> out = new HashMap<>();
            if (meta.getUserMetadata() != null) {
                out.putAll(meta.getUserMetadata());
            }
            return out;
        } catch (Exception e) {
            LOGGER.error("getMetaData failed for bucket/key: {}/{}", bucket, key, e);
            throw new ObjectStoreAdapterException("Failed to get metadata", e.getMessage());
        }
    }

    // ────────────��� Tag API (classic MOSIP tags-as-objects logic) ─────────────

    @Override
    public Map<String, String> addTags(String account, String container, Map<String, String> tags) {
        String bucket = getBucket(account, container);
        String tagDir;
        if (useAccountAsBucketname) {
            tagDir = ObjectStoreUtil.getName(container, null, TAGS_FILENAME);
        } else {
            tagDir = TAGS_FILENAME;
        }
        for (Map.Entry<String, String> tag : tags.entrySet()) {
            String tagObjKey = ObjectStoreUtil.getName(tagDir, tag.getKey()); // classic MOSIP tag key
            try (InputStream data = IOUtils.toInputStream(tag.getValue(), StandardCharsets.UTF_8)) {
                PutObjectRequest req = new PutObjectRequest(bucket, tagObjKey, data, new ObjectMetadata())
                        .withCannedAcl(CannedAccessControlList.Private);
                s3Client.putObject(req);
            } catch (Exception e) {
                LOGGER.error("addTags failed for tag {} on bucket {}", tag.getKey(), bucket, e);
                throw new ObjectStoreAdapterException("Failed to add tag", e.getMessage());
            }
        }
        return tags;
    }

    @Override
    public Map<String, String> getTags(String account, String container) {
        String bucket = getBucket(account, container);
        String tagDir;
        if (useAccountAsBucketname) {
            tagDir = ObjectStoreUtil.getName(container, null, TAGS_FILENAME) + SEPARATOR;
        } else {
            tagDir = TAGS_FILENAME + SEPARATOR;
        }
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket).withPrefix(tagDir);
        ListObjectsV2Result result;
        Map<String, String> out = new HashMap<>();
        do {
            result = s3Client.listObjectsV2(req);
            for (S3ObjectSummary summary : result.getObjectSummaries()) {
                String key = summary.getKey();
                // Tag key is always tagDir + tagName
                if (key.startsWith(tagDir)) {
                    String tagName = key.substring(tagDir.length());
                    String value = s3Client.getObjectAsString(bucket, key);
                    out.put(tagName, value);
                }
            }
            req.setContinuationToken(result.getNextContinuationToken());
        } while (result.isTruncated());
        return out;
    }

    @Override
    public void deleteTags(String account, String container, List<String> tags) {
        String bucket = getBucket(account, container);
        String tagDir = useAccountAsBucketname
                ? ObjectStoreUtil.getName(container, null, TAGS_FILENAME)
                : TAGS_FILENAME;
        for (String tag : tags) {
            String tagObjKey = ObjectStoreUtil.getName(tagDir, tag);
            try {
                s3Client.deleteObject(bucket, tagObjKey);
            } catch (Exception e) {
                LOGGER.error("deleteTag failed for tag {} on bucket {}", tag, bucket, e);
                throw new ObjectStoreAdapterException("Failed to delete tag", e.getMessage());
            }
        }
    }

    @Override
    public List<ObjectDto> getAllObjects(String account, String container) {
        List<S3ObjectSummary> objectSummaries = new ArrayList<>();
        String bucket = getBucket(account, container);

        String prefix;
        if (useAccountAsBucketname) {
            prefix = container + SEPARATOR;
        } else {
            prefix = ""; // root
        }

        ListObjectsV2Request req = new ListObjectsV2Request()
                .withBucketName(bucket)
                .withPrefix(prefix);
        ListObjectsV2Result result;
        do {
            result = s3Client.listObjectsV2(req);
            objectSummaries.addAll(result.getObjectSummaries());
            req.setContinuationToken(result.getNextContinuationToken());
        } while (result.isTruncated());

        List<ObjectDto> dtos = new ArrayList<>();
        for (S3ObjectSummary summary : objectSummaries) {
            String key = summary.getKey();
            String[] tempKeys = key.split(SEPARATOR);

            // Skip tag marker objects
            if (useAccountAsBucketname) {
                if (tempKeys.length > 1 && tempKeys[1] != null && tempKeys[1].endsWith(TAGS_FILENAME)) continue;
            } else {
                if (tempKeys.length > 0 && tempKeys[0] != null && tempKeys[0].endsWith(TAGS_FILENAME)) continue;
            }

            String[] keys = removeIdFromObjectPath(useAccountAsBucketname, tempKeys);
            if (keys.length == 0) continue;

            String source = null, process = null, objectName = null;
            if (keys.length >= 3) {
                source = keys[keys.length - 3];
                process = keys[keys.length - 2];
                objectName = keys[keys.length - 1];
            } else if (keys.length == 2) {
                process = keys[0];
                objectName = keys[1];
            } else if (keys.length == 1) {
                objectName = keys[0];
            }
            dtos.add(new ObjectDto(source, process, objectName, summary.getLastModified()));
        }
        return dtos;
    }

    private String[] removeIdFromObjectPath(boolean useAccountAsBucketname, String[] keys) {
        if (useAccountAsBucketname && keys.length > 0) {
            return Arrays.copyOfRange(keys, 1, keys.length);
        }
        return keys;
    }

    /**
     * Removing container not supported in S3Adapter
     *
     * @param account
     * @param container
     * @param source
     * @param process
     * @return
     */
    @Override
    public boolean removeContainer(String account, String container, String source, String process) {
        return false;
    }

    /**
     * Not Supported in S3Adapter
     *
     * @param account
     * @param container
     * @param source
     * @param process
     * @return
     */
    @Override
    public boolean pack(String account, String container, String source, String process, String refId) {
        return false;
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

}