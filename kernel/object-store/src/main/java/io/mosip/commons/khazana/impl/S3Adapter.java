package io.mosip.commons.khazana.impl;

import io.mosip.commons.khazana.config.LoggerConfiguration;
import io.mosip.commons.khazana.dto.ObjectDto;
import io.mosip.commons.khazana.exception.ObjectStoreAdapterException;
import io.mosip.commons.khazana.spi.ObjectStoreAdapter;
import io.mosip.commons.khazana.util.ObjectStoreUtil;
import io.mosip.kernel.core.exception.ExceptionUtils;
import io.mosip.kernel.core.logger.spi.Logger;
import org.apache.commons.lang.ArrayUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static io.mosip.commons.khazana.config.LoggerConfiguration.REGISTRATIONID;
import static io.mosip.commons.khazana.config.LoggerConfiguration.SESSIONID;
import static io.mosip.commons.khazana.constant.KhazanaConstant.TAGS_FILENAME;
import static io.mosip.commons.khazana.constant.KhazanaErrorCodes.OBJECT_STORE_NOT_ACCESSIBLE;

@Service
@Qualifier("S3Adapter")
public class S3Adapter implements ObjectStoreAdapter {

    private final Logger LOGGER = LoggerConfiguration.logConfig(S3Adapter.class);

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

    private static final String SEPARATOR = "/";
    private static final int IO_BUFFER = 64 * 1024;

    /* ============================================================= */
    /* ====================== CORE OPERATIONS ======================= */
    /* ============================================================= */

    @Override
    public InputStream getObject(String account, String container,
                                 String source, String process, String objectName) {

        String bucket = resolveBucket(account, container);
        String key = resolveKey(container, source, process, objectName);

        try {
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();

            // TRUE STREAMING: caller must close this stream
            ResponseInputStream<GetObjectResponse> rawStream = getClient().getObject(request);
            return rawStream;

        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "getObject failed: " + key, ExceptionUtils.getStackTrace(e));

            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public boolean putObject(String account, String container,
                             String source, String process,
                             String objectName, InputStream data) {

        String bucket = resolveBucket(account, container);
        String key = resolveKey(container, source, process, objectName);

        Path tempFile = null;
        try {
            createBucketIfMissing(bucket);

            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .contentType("application/octet-stream")
                    .build();

            RequestBody body;

            // STREAMING, no crypto
            if (data instanceof java.io.FileInputStream fis) {
                long size = fis.getChannel().size();
                body = RequestBody.fromInputStream(data, size);
            } else if (data instanceof ByteArrayInputStream bais) {
                int size = bais.available();
                body = RequestBody.fromInputStream(data, size);
            } else {
                // Unknown length -> spool to temp file to avoid heap blow-up
                tempFile = Files.createTempFile("khazana-s3-", ".bin");
                try (OutputStream out = Files.newOutputStream(tempFile)) {
                    byte[] buf = new byte[IO_BUFFER];
                    int r;
                    while ((r = data.read(buf)) != -1) {
                        out.write(buf, 0, r);
                    }
                }
                body = RequestBody.fromFile(tempFile);
            }

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
                try {
                    Files.deleteIfExists(tempFile);
                } catch (IOException ignored) {}
            }
        }
    }

    @Override
    public boolean exists(String account, String container,
                          String source, String process, String objectName) {

        String bucket = resolveBucket(account, container);
        String key = resolveKey(container, source, process, objectName);

        try {
            getClient().headObject(HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean deleteObject(String account, String container,
                                String source, String process,
                                String objectName) {

        String bucket = resolveBucket(account, container);
        String key = resolveKey(container, source, process, objectName);

        try {
            getClient().deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build());

            return true;

        } catch (Exception e) {
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    /* ============================================================= */
    /* ====================== METADATA (LIGHT) ====================== */
    /* ============================================================= */

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

    /* ============================================================= */
    /* ============================ TAGS ============================ */
    /* ============================================================= */

    @Override
    public Map<String, String> addTags(String account,
                                       String container,
                                       Map<String, String> tags) {

        String bucket = resolveBucket(account, container);
        createBucketIfMissing(bucket);

        String baseKey;
        if (useAccountAsBucketname) {
            baseKey = ObjectStoreUtil.getName(container, null, TAGS_FILENAME);
        } else {
            baseKey = TAGS_FILENAME;
        }

        for (Map.Entry<String, String> e : tags.entrySet()) {
            String tagKey = ObjectStoreUtil.getName(baseKey, e.getKey());

            getClient().putObject(
                    PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(tagKey)
                            .contentType("text/plain")
                            .build(),
                    RequestBody.fromString(e.getValue(), StandardCharsets.UTF_8)
            );
        }
        return tags;
    }

    @Override
    public Map<String, String> getTags(String account, String container) {

        Map<String, String> objectTags = new HashMap<>();

        String bucket;
        String finalObjectName;
        if (useAccountAsBucketname) {
            bucket = resolveBucket(account, container);
            finalObjectName = ObjectStoreUtil.getName(container, null, TAGS_FILENAME) + SEPARATOR;
        } else {
            bucket = resolveBucket(account, container);
            finalObjectName = TAGS_FILENAME + SEPARATOR;
        }

        ListObjectsV2Request.Builder listReq = ListObjectsV2Request.builder()
                .bucket(bucket);

        if (useAccountAsBucketname) {
            listReq.prefix(finalObjectName);
        }

        ListObjectsV2Response objectSummary = getClient().listObjectsV2(listReq.build());

        List<String> tagNames = new ArrayList<>();
        if (objectSummary != null && !objectSummary.contents().isEmpty()) {
            objectSummary.contents().forEach(o -> {
                String[] keys = o.key().split(SEPARATOR);
                if (ArrayUtils.isNotEmpty(keys)) {
                    if (useAccountAsBucketname) {
                        if (keys.length > 1 && keys[1] != null && keys[1].endsWith(TAGS_FILENAME)) {
                            if (keys.length > 2) {
                                tagNames.add(keys[2]);
                            }
                        }
                    } else {
                        if (keys.length > 0 && keys[0] != null && keys[0].endsWith(TAGS_FILENAME)) {
                            if (keys.length > 1) {
                                tagNames.add(keys[1]);
                            }
                        }
                    }
                }
            });
        }

        for (String tagName : tagNames) {
            String key = finalObjectName + tagName;
            try (InputStream is = getClient().getObject(
                    GetObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .build())) {
                objectTags.put(tagName, new String(is.readAllBytes(), StandardCharsets.UTF_8));
            } catch (Exception e) {
                LOGGER.error(SESSIONID, REGISTRATIONID,
                        "Exception occurred while getTags for : " + container,
                        ExceptionUtils.getStackTrace(e));
                throw new ObjectStoreAdapterException(
                        OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                        OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
            }
        }

        return objectTags;
    }

    @Override
    public void deleteTags(String account, String container, List<String> tags) {

        String bucket;
        String baseName;
        if (useAccountAsBucketname) {
            bucket = resolveBucket(account, container);
            baseName = ObjectStoreUtil.getName(container, null, TAGS_FILENAME);
        } else {
            bucket = resolveBucket(account, container);
            baseName = TAGS_FILENAME;
        }

        for (String tag : tags) {
            String tagKey = ObjectStoreUtil.getName(baseName, tag);

            getClient().deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucket)
                    .key(tagKey)
                    .build());
        }
    }

    /* ============================================================= */
    /* =========================== LIST ============================= */
    /* ============================================================= */

    @Override
    public List<ObjectDto> getAllObjects(String account, String container) {

        List<ObjectDto> objectDtos = new ArrayList<>();

        String bucket;
        String prefix = null;

        if (useAccountAsBucketname) {
            bucket = resolveBucket(account, container);
            prefix = container + SEPARATOR;
        } else {
            bucket = resolveBucket(account, container);
        }

        ListObjectsV2Request.Builder listReq = ListObjectsV2Request.builder()
                .bucket(bucket);

        if (prefix != null) {
            listReq.prefix(prefix);
        }

        ListObjectsV2Response os = getClient().listObjectsV2(listReq.build());

        if (os != null && !os.contents().isEmpty()) {
            os.contents().forEach(o -> {
                String[] tempKeys = o.key().split(SEPARATOR);

                // ignore Tag file entries
                if (useAccountAsBucketname) {
                    if (tempKeys.length > 1 && tempKeys[1] != null && tempKeys[1].endsWith(TAGS_FILENAME)) {
                        return;
                    }
                } else {
                    if (tempKeys.length > 0 && tempKeys[0] != null && tempKeys[0].endsWith(TAGS_FILENAME)) {
                        return;
                    }
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
                    if (objectDto != null) {
                        objectDtos.add(objectDto);
                    }
                }
            });
        }

        return objectDtos;
    }

    private String[] removeIdFromObjectPath(boolean useAccountAsBucketname, String[] keys) {
        return (useAccountAsBucketname && ArrayUtils.isNotEmpty(keys))
                ? (String[]) ArrayUtils.remove(keys, 0)
                : keys;
    }

    /* ============================================================= */
    /* ====================== REACTIVE WRAPPERS ===================== */
    /* ============================================================= */

    public Mono<Void> putObjectReactive(String account, String container,
                                        String source, String process,
                                        String objectName, InputStream data) {

        return Mono.fromCallable(() -> {
                    try (InputStream in = data) {
                        putObject(account, container, source, process, objectName, in);
                    }
                    return (Void) null;
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    public Mono<InputStream> getObjectReactive(String account, String container,
                                               String source, String process,
                                               String objectName) {

        return Mono.fromCallable(() ->
                        getObject(account, container, source, process, objectName))
                .subscribeOn(Schedulers.boundedElastic());
    }

    /* ============================================================= */
    /* ========================= MISC HELPERS ======================= */
    /* ============================================================= */

    @Override
    public boolean removeContainer(String account, String container, String source, String process) {
        return false;
    }

    @Override
    public boolean pack(String account, String container, String source, String process, String refId) {
        return false;
    }

    private String resolveBucket(String account, String container) {
        String bucket = useAccountAsBucketname ? account : container;
        if (bucket == null) {
            throw new IllegalArgumentException("Bucket name cannot be null");
        }

        if (!bucket.startsWith(bucketNamePrefix)) {
            bucket = bucketNamePrefix + bucket;
        }

        return bucket.toLowerCase();
    }

    private String resolveKey(String container,
                              String source,
                              String process,
                              String object) {

        if (useAccountAsBucketname) {
            return ObjectStoreUtil.getName(container, source, process, object);
        }
        return ObjectStoreUtil.getName(source, process, object);
    }

    private void createBucketIfMissing(String bucket) {
        try {
            getClient().headBucket(
                    HeadBucketRequest.builder().bucket(bucket).build());
        } catch (Exception e) {
            getClient().createBucket(
                    CreateBucketRequest.builder().bucket(bucket).build());
        }
    }

    private S3Client getClient() {

        if (s3Client != null) return s3Client;

        synchronized (this) {
            if (s3Client != null) return s3Client;

            AwsBasicCredentials credentials =
                    AwsBasicCredentials.create(accessKey, secretKey);

            ApacheHttpClient.Builder httpBuilder = ApacheHttpClient.builder()
                    .maxConnections(maxConnection)
                    .connectionTimeout(Duration.ofMillis(connectionTimeout))
                    .socketTimeout(Duration.ofMillis(socketTimeout))
                    .tcpKeepAlive(true);

            S3Configuration serviceConfig = S3Configuration.builder()
                    .pathStyleAccessEnabled(true)   // MinIO-friendly
                    .build();

            S3ClientBuilder builder = S3Client.builder()
                    .credentialsProvider(StaticCredentialsProvider.create(credentials))
                    .region(Region.of(region))
                    .httpClientBuilder(httpBuilder)
                    .serviceConfiguration(serviceConfig);

            if (url != null && !url.isBlank()) {
                builder = builder.endpointOverride(URI.create(url));
            }

            s3Client = builder.build();
            return s3Client;
        }
    }
}
