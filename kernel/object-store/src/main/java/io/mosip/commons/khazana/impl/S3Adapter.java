package io.mosip.commons.khazana.impl;

import io.mosip.commons.khazana.config.LoggerConfiguration;
import io.mosip.commons.khazana.dto.ObjectDto;
import io.mosip.commons.khazana.exception.ObjectStoreAdapterException;
import io.mosip.commons.khazana.spi.ObjectStoreAdapter;
import io.mosip.commons.khazana.util.ObjectStoreUtil;
import io.mosip.kernel.core.exception.ExceptionUtils;
import io.mosip.kernel.core.logger.spi.Logger;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static io.mosip.commons.khazana.config.LoggerConfiguration.REGISTRATIONID;
import static io.mosip.commons.khazana.config.LoggerConfiguration.SESSIONID;
import static io.mosip.commons.khazana.constant.KhazanaErrorCodes.OBJECT_STORE_NOT_ACCESSIBLE;

@Service
@Qualifier("S3Adapter")
public class S3Adapter implements ObjectStoreAdapter {

    private static final Logger LOGGER = LoggerConfiguration.logConfig(S3Adapter.class);

    @Value("${object.store.s3.accesskey}")
    private String accessKey;

    @Value("${object.store.s3.secretkey}")
    private String secretKey;

    @Value("${object.store.s3.url:}")
    private String url;

    @Value("${object.store.s3.region:us-east-1}")
    private String region;

    @Value("${object.store.max.connection:200}")
    private int maxConnection;

    @Value("${object.store.connection.timeout:5000}")
    private int connectionTimeout;

    @Value("${object.store.socket.timeout:60000}")
    private int socketTimeout;

    @Value("${object.store.s3.use.account.as.bucketname:false}")
    private boolean useAccountAsBucketname;

    @Value("${object.store.s3.bucket-name-prefix:}")
    private String bucketNamePrefix;

    private static final String TAGS_DIR = "tags/";
    private volatile S3Client s3Client;

    /* ==========================================================================================
       CORE FIXED S3 CLIENT (Ultra Fast, Connection Pooled, MinIO compatible)
       ========================================================================================== */
    private S3Client getClient() {
        if (s3Client != null) return s3Client;

        synchronized (this) {
            if (s3Client != null) return s3Client;

            AwsBasicCredentials credentials =
                    AwsBasicCredentials.create(accessKey, secretKey);

            ApacheHttpClient.Builder http = ApacheHttpClient.builder()
                    .maxConnections(maxConnection)
                    .connectionTimeout(Duration.ofMillis(connectionTimeout))
                    .socketTimeout(Duration.ofMillis(socketTimeout))
                    .tcpKeepAlive(true);

            S3Configuration s3cfg = S3Configuration.builder()
                    .pathStyleAccessEnabled(true)     // Important for MinIO
                    .build();

            S3ClientBuilder builder = S3Client.builder()
                    .credentialsProvider(StaticCredentialsProvider.create(credentials))
                    .httpClientBuilder(http)
                    .serviceConfiguration(s3cfg)
                    .region(Region.of(region));

            if (url != null && !url.isBlank()) {
                builder = builder.endpointOverride(URI.create(url));
            }

            s3Client = builder.build();
            return s3Client;
        }
    }

    /* ==========================================================================================
       MOSIP-COMPATIBLE PATH HANDLING (Critical Fix)
       ========================================================================================== */
    private String resolveKey(String container, String source, String process, String objectName) {

        // Restore MOSIP Path Structure
        // container/source/process/objectName

        if (useAccountAsBucketname) {
            return ObjectStoreUtil.getName(container, source, process, objectName);
        } else {
            return ObjectStoreUtil.getName(container, source, process, objectName);
        }
    }

    private String resolveBucket(String account, String container) {

        String bucket = useAccountAsBucketname ? account : container;

        if (bucket == null) {
            throw new IllegalArgumentException("Bucket name cannot be null");
        }

        if (bucketNamePrefix != null && !bucketNamePrefix.isBlank()) {
            if (!bucket.startsWith(bucketNamePrefix)) {
                bucket = bucketNamePrefix + bucket;
            }
        }

        return bucket.toLowerCase();
    }

    private void ensureBucket(String bucket) {
        try {
            getClient().headBucket(HeadBucketRequest.builder().bucket(bucket).build());
        } catch (Exception e) {
            getClient().createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        }
    }

    /* ==========================================================================================
       GET (Fully streaming)
       ========================================================================================== */
    @Override
    public InputStream getObject(String account, String container,
                                 String source, String process, String objectName) {

        String bucket = resolveBucket(account, container);
        String key = resolveKey(container, source, process, objectName);

        try {
            GetObjectRequest req = GetObjectRequest.builder()
                    .bucket(bucket).key(key).build();

            return getClient().getObject(req); // streaming

        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "getObject failed: " + key, ExceptionUtils.getStackTrace(e));

            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    /* ==========================================================================================
       PUT (Ultra-Fast, Always Known Content-Length, No Heap Blow-Up)
       ========================================================================================== */
    @Override
    public boolean putObject(String account, String container,
                             String source, String process,
                             String objectName, InputStream data) {

        String bucket = resolveBucket(account, container);
        String key = resolveKey(container, source, process, objectName);

        ensureBucket(bucket);

        Path tempFile = null;
        try {
            RequestBody body;

            // FAST PATH: file input stream
            if (data instanceof FileInputStream fis) {
                long size = fis.getChannel().size();
                body = RequestBody.fromInputStream(data, size);
            }
            // FAST PATH: byte array
            else if (data instanceof ByteArrayInputStream bais) {
                byte[] bytes = bais.readAllBytes();
                body = RequestBody.fromBytes(bytes);
            }
            // STREAM UNKNOWN LENGTH → spool to a temp file (Least overhead)
            else {
                tempFile = Files.createTempFile("khazana-put-", ".tmp");
                try (OutputStream out = Files.newOutputStream(tempFile)) {
                    byte[] buf = new byte[64 * 1024];
                    int n;
                    while ((n = data.read(buf)) != -1) {
                        out.write(buf, 0, n);
                    }
                }
                body = RequestBody.fromFile(tempFile.toFile());
            }

            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .metadata(Map.of(
                            "source", source == null ? "" : source,
                            "process", process == null ? "" : process,
                            "objectName", objectName
                    ))
                    .contentType("application/octet-stream")
                    .build();

            getClient().putObject(request, body);
            return true;

        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "putObject failed: " + key, ExceptionUtils.getStackTrace(e));

            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        } finally {
            if (tempFile != null) {
                try { Files.deleteIfExists(tempFile); } catch (Exception ignore) {}
            }
        }
    }

    /* ==========================================================================================
       EXISTS
       ========================================================================================== */
    @Override
    public boolean exists(String account, String container,
                          String source, String process, String objectName) {

        String bucket = resolveBucket(account, container);
        String key = resolveKey(container, source, process, objectName);

        try {
            getClient().headObject(HeadObjectRequest.builder()
                    .bucket(bucket).key(key).build());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /* ==========================================================================================
       DELETE
       ========================================================================================== */
    @Override
    public boolean deleteObject(String account, String container,
                                String source, String process, String objectName) {

        String bucket = resolveBucket(account, container);
        String key = resolveKey(container, source, process, objectName);

        try {
            getClient().deleteObject(
                    DeleteObjectRequest.builder().bucket(bucket).key(key).build()
            );
            return true;

        } catch (Exception e) {
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    /* ==========================================================================================
       TAGS (Restored MOSIP Format: tags/<tagName>)
       ========================================================================================== */
    @Override
    public Map<String, String> addTags(String account, String container, Map<String, String> tags) {

        String bucket = resolveBucket(account, container);
        ensureBucket(bucket);

        for (Map.Entry<String, String> e : tags.entrySet()) {

            String key = TAGS_DIR + e.getKey();  // FIXED

            getClient().putObject(
                    PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .contentType("text/plain")
                            .build(),
                    RequestBody.fromString(e.getValue())
            );
        }
        return tags;
    }

    @Override
    public Map<String, String> getTags(String account, String container) {

        Map<String, String> tags = new HashMap<>();
        String bucket = resolveBucket(account, container);

        ListObjectsV2Response resp = getClient().listObjectsV2(
                ListObjectsV2Request.builder()
                        .bucket(bucket)
                        .prefix(TAGS_DIR)  // FIXED
                        .build()
        );

        if (resp == null || resp.contents().isEmpty()) return tags;

        for (S3Object obj : resp.contents()) {

            String key = obj.key(); // tags/name
            String tagName = key.substring(TAGS_DIR.length());

            try (InputStream is = getClient().getObject(
                    GetObjectRequest.builder().bucket(bucket).key(key).build()
            )) {
                tags.put(tagName, new String(is.readAllBytes()));
            } catch (Exception e) {
                throw new ObjectStoreAdapterException(
                        OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                        OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
            }
        }

        return tags;
    }

    @Override
    public void deleteTags(String account, String container, List<String> tags) {

        String bucket = resolveBucket(account, container);

        for (String t : tags) {
            String key = TAGS_DIR + t;
            getClient().deleteObject(
                    DeleteObjectRequest.builder().bucket(bucket).key(key).build()
            );
        }
    }

    /* ==========================================================================================
       LIST (Compatible with MOSIP’s expectations)
       ========================================================================================== */
    @Override
    public List<ObjectDto> getAllObjects(String account, String container) {

        String bucket = resolveBucket(account, container);

        ListObjectsV2Response resp = getClient().listObjectsV2(
                ListObjectsV2Request.builder().bucket(bucket).build()
        );

        List<ObjectDto> list = new ArrayList<>();

        if (resp == null || resp.contents().isEmpty()) return list;

        for (S3Object obj : resp.contents()) {

            String[] parts = obj.key().split("/");

            // Skip tags folder
            if (parts.length > 0 && parts[0].equals("tags")) {
                continue;
            }

            if (parts.length == 4) {
                list.add(new ObjectDto(parts[1], parts[2], parts[3], Date.from(obj.lastModified())));
            }
        }

        return list;
    }

    @Override
    public boolean removeContainer(String account, String container, String source, String process) {
        return false;
    }

    @Override
    public boolean pack(String account, String container, String source, String process, String refId) {
        return false;
    }
    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source,
                                                 String process, String objectName,
                                                 Map<String, Object> metadata) {
        // No-op for now – v1 compatibility
        return metadata;
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source,
                                                 String process, String objectName,
                                                 String key, String value) {
        Map<String, Object> m = new HashMap<>();
        m.put(key, value);
        return m;
    }

    @Override
    public Integer incMetadata(String account, String container, String source,
                               String process, String objectName, String metaDataKey) {
        return null;
    }

    @Override
    public Integer decMetadata(String account, String container, String source,
                               String process, String objectName, String metaDataKey) {
        return null;
    }

    @Override
    public Map<String, Object> getMetaData(String account,
                                           String container,
                                           String source,
                                           String process,
                                           String objectName) {

        String bucket = resolveBucket(account, container);
        String key = resolveKey(container, source, process, objectName);

        try {
            HeadObjectResponse response =
                    getClient().headObject(HeadObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .build());

            return response.metadata()
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue
                    ));

        } catch (Exception e) {
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

}
