package io.mosip.commons.khazana.impl;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.*;
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

    private static final String TAGS_FILENAME = "tags";
    private static final String SEPARATOR = "/";
    private static final String PLACEHOLDER = "_";

    private final Logger LOGGER = LoggerConfiguration.logConfig(S3Adapter.class);

    @Value("${object.store.s3.accesskey}")
    private String accessKey;

    @Value("${object.store.s3.secretkey}")
    private String secretKey;

    @Value("${object.store.s3.region:ap-south-1}")
    private String region;

    @Value("${object.store.s3.max.connections:250}")
    private int maxConnections;

    @Value("${object.store.s3.multipart.threshold:5242880}")
    private long multipartThreshold;

    @Value("${object.store.s3.multipart.part.size.mb:5}")
    private int partSizeMb;

    @Value("${object.store.s3.use.account.as.bucketname:false}")
    private boolean useAccountAsBucketname;

    @Value("${object.store.s3.bucket-name-prefix:}")
    private String bucketPrefix;

    private AmazonS3 s3;
    private TransferManager tm;

    /* ───────────── Lifecycle ───────────── */

    @PostConstruct
    public void init() {
        ClientConfiguration cfg = new ClientConfiguration()
                .withMaxConnections(maxConnections)
                .withTcpKeepAlive(true);

        AWSCredentials creds = new BasicAWSCredentials(accessKey, secretKey);

        s3 = AmazonS3ClientBuilder.standard()
                .withClientConfiguration(cfg)
                .withCredentials(new AWSStaticCredentialsProvider(creds))
                .withRegion(region)
                .build();

        tm = TransferManagerBuilder.standard()
                .withS3Client(s3)
                .withMultipartUploadThreshold(multipartThreshold)
                .withMinimumUploadPartSize(partSizeMb * 1024L * 1024L)
                .build();
    }

    @PreDestroy
    public void shutdown() {
        if (tm != null) tm.shutdownNow(false);
        if (s3 != null) s3.shutdown();
    }

    /* ───────────── Key helpers ───────────── */

    private String bucket(String account, String container) {
        String b = useAccountAsBucketname ? account : container;
        if (!bucketPrefix.isBlank() && !b.startsWith(bucketPrefix)) {
            b = bucketPrefix + b;
        }
        return b.toLowerCase();
    }

    private String key(String container, String source, String process, String object) {
        String s = (source == null || source.isBlank()) ? PLACEHOLDER : source;
        String p = (process == null || process.isBlank()) ? PLACEHOLDER : process;

        return useAccountAsBucketname
                ? ObjectStoreUtil.getName(container, s, p, object)
                : ObjectStoreUtil.getName(s, p, object);
    }

    /* ───────────── Core API ───────────── */

    @Override
    public boolean putObject(String account, String container, String source,
                             String process, String objectName, InputStream data) {
        try {
            Upload u = tm.upload(new PutObjectRequest(
                    bucket(account, container),
                    key(container, source, process, objectName),
                    data,
                    new ObjectMetadata()
            ));
            u.waitForCompletion();
            return true;
        } catch (Exception e) {
            throw new ObjectStoreAdapterException("PUT_FAILED", e.getMessage(), e);
        }
    }

    @Override
    public InputStream getObject(String account, String container, String source,
                                 String process, String objectName) {
        try {
            return s3.getObject(
                    bucket(account, container),
                    key(container, source, process, objectName)
            ).getObjectContent();
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) return null;
            throw e;
        }
    }

    @Override
    public boolean exists(String account, String container, String source,
                          String process, String objectName) {
        try {
            s3.getObjectMetadata(
                    bucket(account, container),
                    key(container, source, process, objectName)
            );
            return true;
        } catch (AmazonS3Exception e) {
            return e.getStatusCode() != 404;
        }
    }

    @Override
    public boolean deleteObject(String account, String container, String source,
                                String process, String objectName) {
        s3.deleteObject(bucket(account, container),
                key(container, source, process, objectName));
        return true;
    }

    /* ───────────── Metadata (FULLY SAFE) ───────────── */

    @Override
    public Map<String, Object> getMetaData(String account, String container,
                                           String source, String process, String objectName) {
        ObjectMetadata m = s3.getObjectMetadata(
                bucket(account, container),
                key(container, source, process, objectName)
        );

        Map<String, Object> out = new HashMap<>();
        if (m.getUserMetadata() != null) {
            out.putAll(m.getUserMetadata());
        }
        return out;
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container,
                                                 String source, String process,
                                                 String objectName, Map<String, Object> meta) {

        String b = bucket(account, container);
        String k = key(container, source, process, objectName);

        try {
            ObjectMetadata old = s3.getObjectMetadata(b, k);

            Map<String, String> merged = new HashMap<>();
            if (old.getUserMetadata() != null) {
                merged.putAll(old.getUserMetadata());
            }
            meta.forEach((x, y) -> merged.put(x, Objects.toString(y, null)));

            ObjectMetadata n = new ObjectMetadata();
            n.setUserMetadata(merged);

            // 🔒 Preserve ALL system metadata
            n.setContentType(old.getContentType());
            n.setContentEncoding(old.getContentEncoding());
            n.setCacheControl(old.getCacheControl());
            n.setContentDisposition(old.getContentDisposition());
            n.setContentLanguage(old.getContentLanguage());
            n.setContentLength(old.getContentLength());

            s3.copyObject(new CopyObjectRequest(b, k, b, k)
                    .withNewObjectMetadata(n));

            return meta;

        } catch (Exception e) {
            throw new ObjectStoreAdapterException(
                    "OBJECT_STORE_NOT_ACCESSIBLE",
                    "Failed to update object metadata",
                    e
            );
        }
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container,
                                                 String source, String process,
                                                 String objectName, String key, String value) {
        Map<String, Object> meta = new HashMap<>();
        meta.put(key, value);
        return addObjectMetaData(account, container, source, process, objectName, meta);
    }

    /* ───────────── Tags (ISOLATED & SAFE) ───────────── */

    @Override
    public Map<String, String> addTags(String account, String container,
                                       Map<String, String> tags) {
        String b = bucket(account, container);
        String dir = useAccountAsBucketname
                ? ObjectStoreUtil.getName(container, null, TAGS_FILENAME)
                : TAGS_FILENAME;

        tags.forEach((k, v) -> {
            try (InputStream in = IOUtils.toInputStream(v, StandardCharsets.UTF_8)) {
                s3.putObject(b, ObjectStoreUtil.getName(dir, k), in, new ObjectMetadata());
            } catch (Exception e) {
                throw new ObjectStoreAdapterException("TAG_ADD_FAILED", e.getMessage(), e);
            }
        });
        return tags;
    }

    @Override
    public Map<String, String> getTags(String account, String container) {
        Map<String, String> out = new HashMap<>();
        String b = bucket(account, container);
        String prefix = (useAccountAsBucketname
                ? ObjectStoreUtil.getName(container, null, TAGS_FILENAME)
                : TAGS_FILENAME) + SEPARATOR;

        ListObjectsV2Request r = new ListObjectsV2Request()
                .withBucketName(b)
                .withPrefix(prefix);

        ListObjectsV2Result res;
        do {
            res = s3.listObjectsV2(r);
            res.getObjectSummaries().forEach(o ->
                    out.put(o.getKey().substring(prefix.length()),
                            s3.getObjectAsString(b, o.getKey())));
            r.setContinuationToken(res.getNextContinuationToken());
        } while (res.isTruncated());

        return out;
    }

    @Override
    public void deleteTags(String account, String container, List<String> tags) {
        String b = bucket(account, container);
        String dir = useAccountAsBucketname
                ? ObjectStoreUtil.getName(container, null, TAGS_FILENAME)
                : TAGS_FILENAME;

        tags.forEach(t -> s3.deleteObject(b, ObjectStoreUtil.getName(dir, t)));
    }

    /* ───────────── Listing (STRICT & SAFE) ───────────── */

    @Override
    public List<ObjectDto> getAllObjects(String account, String container) {

        List<ObjectDto> out = new ArrayList<>();
        String b = bucket(account, container);
        String prefix = useAccountAsBucketname ? container + SEPARATOR : "";

        ListObjectsV2Request r = new ListObjectsV2Request()
                .withBucketName(b)
                .withPrefix(prefix);

        ListObjectsV2Result res;
        do {
            res = s3.listObjectsV2(r);
            for (S3ObjectSummary o : res.getObjectSummaries()) {

                String[] parts = o.getKey().split(SEPARATOR);

                if (useAccountAsBucketname && parts.length > 0) {
                    parts = Arrays.copyOfRange(parts, 1, parts.length);
                }

                // 🔒 Skip empty / folder marker objects
                if (parts.length == 0 || parts[parts.length - 1].isBlank()) {
                    continue;
                }

                // 🔒 Skip ALL tag objects (root cause fix)
                if (parts.length > 0 && TAGS_FILENAME.equalsIgnoreCase(parts[0])) {
                    continue;
                }

                if (parts.length != 1 && parts.length != 2 && parts.length != 3) {
                    LOGGER.warn("Invalid MOSIP object key: {}", o.getKey());
                    continue;
                }

                String src = parts.length >= 2 ? parts[0] : null;
                String proc = parts.length == 3 ? parts[1] : null;
                String obj = parts[parts.length - 1];

                // 🔒 Final defensive guard
                if ("Tags".equalsIgnoreCase(src)) {
                    LOGGER.warn("Skipping tag-like object key: {}", o.getKey());
                    continue;
                }

                out.add(new ObjectDto(src, proc, obj, o.getLastModified()));
            }
            r.setContinuationToken(res.getNextContinuationToken());
        } while (res.isTruncated());

        return out;
    }


    /* ───────────── Unsupported APIs ───────────── */

    @Override public boolean removeContainer(String a, String c, String s, String p) { return false; }
    @Override public boolean pack(String a, String c, String s, String p, String r) { return false; }

    @Override
    public Integer incMetadata(String account, String container, String source,
                               String process, String objectName, String metaDataKey) {
        Map<String, Object> metadata = getMetaData(account, container, source, process, objectName);
        if (metadata.get(metaDataKey) != null) {
            metadata.put(metaDataKey, Integer.valueOf(metadata.get(metaDataKey).toString()) + 1);
            addObjectMetaData(account, container, source, process, objectName, metadata);
            return Integer.valueOf(metadata.get(metaDataKey).toString());
        }
        return null;
    }

    @Override
    public Integer decMetadata(String account, String container, String source,
                               String process, String objectName, String metaDataKey) {
        Map<String, Object> metadata = getMetaData(account, container, source, process, objectName);
        if (metadata.get(metaDataKey) != null) {
            metadata.put(metaDataKey, Integer.valueOf(metadata.get(metaDataKey).toString()) - 1);
            addObjectMetaData(account, container, source, process, objectName, metadata);
            return Integer.valueOf(metadata.get(metaDataKey).toString());
        }
        return null;
    }
}
