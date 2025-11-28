package io.mosip.commons.khazana.impl;

import io.mosip.commons.khazana.config.LoggerConfiguration;
import io.mosip.commons.khazana.dto.ObjectDto;
import io.mosip.commons.khazana.exception.ObjectStoreAdapterException;
import io.mosip.commons.khazana.spi.ObjectStoreAdapter;
import io.mosip.commons.khazana.util.ObjectStoreUtil;
import io.mosip.kernel.core.exception.ExceptionUtils;
import io.mosip.kernel.core.logger.spi.Logger;
import org.apache.commons.io.IOUtils;
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

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;

import static io.mosip.commons.khazana.config.LoggerConfiguration.REGISTRATIONID;
import static io.mosip.commons.khazana.config.LoggerConfiguration.SESSIONID;
import static io.mosip.commons.khazana.constant.KhazanaErrorCodes.OBJECT_STORE_NOT_ACCESSIBLE;

@Service
@Qualifier("S3Adapter")
public class S3Adapter implements ObjectStoreAdapter {

    private static final Logger LOGGER = LoggerConfiguration.logConfig(S3Adapter.class);
    private static final String TAGS_DIR = "tags/";

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

    private volatile S3Client s3Client;

    public void S3Adapter() {
        System.out.println(">>> USING AWS-SDK-V2 S3Adapter (Ultra Fast) <<<");
    }

    /* ===================================================================
       CLIENT INITIALIZATION
       =================================================================== */
    private S3Client getClient() {
        if (s3Client != null) return s3Client;

        synchronized (this) {
            if (s3Client != null) return s3Client;

            AwsBasicCredentials creds = AwsBasicCredentials.create(accessKey, secretKey);

            ApacheHttpClient.Builder http = ApacheHttpClient.builder()
                    .maxConnections(maxConnection)
                    .connectionTimeout(Duration.ofMillis(connectionTimeout))
                    .socketTimeout(Duration.ofMillis(socketTimeout))
                    .tcpKeepAlive(true);

            S3Configuration s3Config = S3Configuration.builder()
                    .pathStyleAccessEnabled(true)   // Important for MinIO + MOSIP
                    .build();

            S3ClientBuilder builder = S3Client.builder()
                    .credentialsProvider(StaticCredentialsProvider.create(creds))
                    .httpClientBuilder(http)
                    .serviceConfiguration(s3Config)
                    .region(Region.of(region));

            if (url != null && !url.isBlank()) {
                builder = builder.endpointOverride(URI.create(url));
            }

            s3Client = builder.build();
            return s3Client;
        }
    }

    /* ===================================================================
       PATH HELPERS
       =================================================================== */
    private String resolveBucket(String account, String container) {
        String bucket = useAccountAsBucketname ? account : container;
        bucket = (bucketNamePrefix + bucket).toLowerCase();
        return bucket;
    }

    private String resolveKey(String container, String source, String process, String objectName) {
        return ObjectStoreUtil.getName(container, source, process, objectName);
    }

    private void ensureBucket(String bucket) {
        try {
            getClient().headBucket(HeadBucketRequest.builder().bucket(bucket).build());
        } catch (Exception ex) {
            getClient().createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        }
    }

    /* ===================================================================
       GET (Streaming)
       =================================================================== */
    @Override
    public InputStream getObject(String account, String container, String source, String process, String objectName) {
        String bucket = resolveBucket(account, container);
        String key = resolveKey(container, source, process, objectName);

        try {
            return getClient().getObject(
                    GetObjectRequest.builder().bucket(bucket).key(key).build()
            );
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "getObject failed for: " + key,
                    ExceptionUtils.getStackTrace(e));

            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    /* ===================================================================
       PUT (Streaming, optimized)
       =================================================================== */
    @Override
    public boolean putObject(String account, String container,
                             String source, String process,
                             String objectName, InputStream data) {

        String bucket = resolveBucket(account, container);
        ensureBucket(bucket);

        String key = resolveKey(container, source, process, objectName);

        Path tempFile = null;
        RequestBody body;

        try {
            if (data instanceof FileInputStream fis) {
                long size = fis.getChannel().size();
                body = RequestBody.fromInputStream(data, size);
            } else {
                tempFile = Files.createTempFile("mosip-s3-", ".tmp");
                try (OutputStream out = Files.newOutputStream(tempFile)) {
                    IOUtils.copy(data, out);
                }
                body = RequestBody.fromFile(tempFile);
            }

            PutObjectRequest req = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .contentType("application/octet-stream")
                    .build();

            getClient().putObject(req, body);
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

    /* ===================================================================
       EXISTS
       =================================================================== */
    @Override
    public boolean exists(String account, String container, String source, String process, String objectName) {
        String bucket = resolveBucket(account, container);
        String key = resolveKey(container, source, process, objectName);

        try {
            getClient().headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    /* ===================================================================
       DELETE
       =================================================================== */
    @Override
    public boolean deleteObject(String account, String container, String source, String process, String objectName) {
        String bucket = resolveBucket(account, container);
        String key = resolveKey(container, source, process, objectName);

        try {
            getClient().deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());
            return true;
        } catch (Exception e) {
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    /* ===================================================================
       METADATA (User Metadata)
       =================================================================== */
    @Override
    public Map<String, Object> getMetaData(String account, String container,
                                           String source, String process,
                                           String objectName) {

        String bucket = resolveBucket(account, container);
        String key = resolveKey(container, source, process, objectName);

        try {
            HeadObjectResponse resp = getClient().headObject(
                    HeadObjectRequest.builder().bucket(bucket).key(key).build()
            );

            return new HashMap<>(resp.metadata());

        } catch (Exception e) {
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container,
                                                 String source, String process,
                                                 String objectName,
                                                 Map<String, Object> metadata) {
        // Not supported by AWS SDK v2 without re-upload → ignore but return input
        return metadata;
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source,
                                                 String process, String objectName,
                                                 String key, String value) {
        return Map.of(key, value);
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

    /* ===================================================================
       TAGS (Stored under tags/)
       =================================================================== */
    @Override
    public Map<String, String> addTags(String account, String container, Map<String, String> tags) {
        String bucket = resolveBucket(account, container);
        ensureBucket(bucket);

        for (Map.Entry<String, String> e : tags.entrySet()) {
            String key = TAGS_DIR + e.getKey();
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
        String bucket = resolveBucket(account, container);

        ListObjectsV2Response resp = getClient().listObjectsV2(
                ListObjectsV2Request.builder().bucket(bucket).prefix(TAGS_DIR).build()
        );

        Map<String, String> tags = new HashMap<>();

        for (S3Object obj : resp.contents()) {
            String key = obj.key();
            String tagName = key.substring(TAGS_DIR.length());

            try (InputStream is = getClient().getObject(
                    GetObjectRequest.builder().bucket(bucket).key(key).build()
            )) {
                tags.put(tagName, new String(is.readAllBytes(), StandardCharsets.UTF_8));
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

    /* ===================================================================
       LIST
       =================================================================== */
    @Override
    public List<ObjectDto> getAllObjects(String account, String container) {

        String bucket = resolveBucket(account, container);

        ListObjectsV2Response resp =
                getClient().listObjectsV2(ListObjectsV2Request.builder().bucket(bucket).build());

        List<ObjectDto> list = new ArrayList<>();

        for (S3Object obj : resp.contents()) {

            String[] parts = obj.key().split("/");
            if (parts.length == 4 && !parts[0].equals("tags")) {
                list.add(new ObjectDto(parts[1], parts[2], parts[3], Date.from(obj.lastModified())));
            }
        }

        return list;
    }

    /* ===================================================================
       UNSUPPORTED (Same as old adapter)
       =================================================================== */
    @Override
    public boolean removeContainer(String account, String container, String source, String process) {
        return false;
    }

    @Override
    public boolean pack(String account, String container, String source, String process, String refId) {
        return false;
    }
}
