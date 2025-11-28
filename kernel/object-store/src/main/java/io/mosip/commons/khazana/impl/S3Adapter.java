package io.mosip.commons.khazana.impl;

import io.mosip.commons.khazana.config.LoggerConfiguration;
import io.mosip.commons.khazana.dto.ObjectDto;
import io.mosip.commons.khazana.exception.ObjectStoreAdapterException;
import io.mosip.commons.khazana.spi.ObjectStoreAdapter;
import io.mosip.commons.khazana.util.ObjectStoreUtil;
import io.mosip.kernel.core.logger.spi.Logger;

import org.apache.commons.lang3.ArrayUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

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

import java.io.*;
import java.net.URI;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;

import java.time.Duration;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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

    @Value("${object.store.connection.timeout:10000}")
    private int connectionTimeout;

    @Value("${object.store.socket.timeout:30000}")
    private int socketTimeout;

    @Value("${object.store.s3.use.account.as.bucketname:false}")
    private boolean useAccountAsBucketName;

    @Value("${object.store.s3.bucket-name-prefix:}")
    private String bucketPrefix;

    private static final String TAG_PREFIX = "tags/";
    private static final String SEPARATOR = "/";

    private volatile S3Client s3Client;
    private final Set<String> existingBuckets = ConcurrentHashMap.newKeySet();


    // =====================================================================================
    // CLIENT
    // =====================================================================================
    private S3Client getClient() {

        if (s3Client != null) return s3Client;

        synchronized (this) {

            if (s3Client != null) return s3Client;

            AwsBasicCredentials creds =
                    AwsBasicCredentials.create(accessKey, secretKey);

            ApacheHttpClient.Builder http = ApacheHttpClient.builder()
                    .maxConnections(maxConnection)
                    .connectionTimeout(Duration.ofMillis(connectionTimeout))
                    .socketTimeout(Duration.ofMillis(socketTimeout))
                    .tcpKeepAlive(true);

            S3Configuration cfg = S3Configuration.builder()
                    .pathStyleAccessEnabled(true)
                    .build();

            S3ClientBuilder builder = S3Client.builder()
                    .credentialsProvider(StaticCredentialsProvider.create(creds))
                    .httpClientBuilder(http)
                    .serviceConfiguration(cfg)
                    .region(Region.of(region));

            if (url != null && !url.isBlank()) {
                builder = builder.endpointOverride(URI.create(url));
            }

            s3Client = builder.build();
            return s3Client;
        }
    }

    // =====================================================================================
    // BUCKET HELPERS
    // =====================================================================================
    private String resolveBucket(String account, String container) {

        String b = useAccountAsBucketName ? account : container;

        if (bucketPrefix != null && !bucketPrefix.isBlank() && !b.startsWith(bucketPrefix)) {
            b = bucketPrefix + b;
        }

        return b.toLowerCase();
    }

    private boolean ensureBucket(S3Client client, String bucket) {

        if (existingBuckets.contains(bucket)) return true;

        try {
            client.headBucket(HeadBucketRequest.builder().bucket(bucket).build());
            existingBuckets.add(bucket);
            return true;
        } catch (Exception e) {
            client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
            existingBuckets.add(bucket);
            return true;
        }
    }


    // =====================================================================================
    // OBJECT NAME (Same as MOSIP logic)
    // =====================================================================================
    private String buildKey(String container, String source, String process, String object) {

        if (useAccountAsBucketName) {
            return ObjectStoreUtil.getName(container, source, process, object);
        }
        return ObjectStoreUtil.getName(source, process, object);
    }


    // =====================================================================================
    // GET
    // =====================================================================================
    @Override
    public InputStream getObject(String account, String container,
                                 String source, String process, String objectName) {

        String bucket = resolveBucket(account, container);
        String key = buildKey(container, source, process, objectName);

        try {
            ResponseInputStream<GetObjectResponse> res =
                    getClient().getObject(
                            GetObjectRequest.builder().bucket(bucket).key(key).build()
                    );

            return new BufferedInputStream(res, 64 * 1024);

        } catch (Exception e) {
            s3Client = null;
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    // =====================================================================================
    // PUT
    // =====================================================================================
    @Override
    public boolean putObject(String account, String container,
                             String source, String process,
                             String objectName, InputStream data) {

        String bucket = resolveBucket(account, container);
        S3Client client = getClient();
        ensureBucket(client, bucket);

        String key = buildKey(container, source, process, objectName);

        try {
            RequestBody body;

            if (data instanceof FileInputStream fis) {
                body = RequestBody.fromInputStream(fis, fis.getChannel().size());
            } else if (data instanceof ByteArrayInputStream bais) {
                body = RequestBody.fromBytes(bais.readAllBytes());
            } else {
                Path tmp = Files.createTempFile("put-", ".tmp");
                Files.copy(data, tmp, StandardCopyOption.REPLACE_EXISTING);
                body = RequestBody.fromFile(tmp.toFile());
                Files.deleteIfExists(tmp);
            }

            client.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .contentType("application/octet-stream")
                            .build(),
                    body
            );

            return true;

        } catch (Exception e) {
            s3Client = null;
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }


    // =====================================================================================
    // EXISTS
    // =====================================================================================
    @Override
    public boolean exists(String account, String container,
                          String source, String process, String objectName) {

        String bucket = resolveBucket(account, container);
        String key = buildKey(container, source, process, objectName);

        try {
            getClient().headObject(
                    HeadObjectRequest.builder().bucket(bucket).key(key).build()
            );
            return true;
        } catch (Exception e) {
            return false;
        }
    }


    // =====================================================================================
    // DELETE
    // =====================================================================================
    @Override
    public boolean deleteObject(String account, String container,
                                String source, String process, String objectName) {

        String bucket = resolveBucket(account, container);
        String key = buildKey(container, source, process, objectName);

        getClient().deleteObject(
                DeleteObjectRequest.builder().bucket(bucket).key(key).build()
        );

        return true;
    }


    // =====================================================================================
    // TAGS — *** FINAL FIXED MOSIP-COMPATIBLE VERSION ***
    // =====================================================================================
    private String tagKey(String tagName) {
        return TAG_PREFIX + tagName;
    }

    @Override
    public Map<String, String> addTags(String account, String container, Map<String, String> tags) {

        String bucket = resolveBucket(account, container);
        S3Client client = getClient();
        ensureBucket(client, bucket);

        try {
            for (var e : tags.entrySet()) {

                client.putObject(
                        PutObjectRequest.builder()
                                .bucket(bucket)
                                .key(tagKey(e.getKey()))
                                .contentType("text/plain")
                                .build(),
                        RequestBody.fromBytes(e.getValue().getBytes(StandardCharsets.UTF_8))
                );
            }
            return tags;

        } catch (Exception e) {
            s3Client = null;
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }


    @Override
    public Map<String, String> getTags(String account, String container) {

        Map<String, String> out = new HashMap<>();

        String bucket = resolveBucket(account, container);
        S3Client client = getClient();

        try {
            ListObjectsV2Response list =
                    client.listObjectsV2(
                            ListObjectsV2Request.builder()
                                    .bucket(bucket)
                                    .prefix(TAG_PREFIX)
                                    .build()
                    );

            if (list.contents() == null) return out;

            for (S3Object o : list.contents()) {

                String key = o.key(); // tags/<tagName>
                String tagName = key.substring(TAG_PREFIX.length());

                try (InputStream is = client.getObject(
                        GetObjectRequest.builder().bucket(bucket).key(key).build()
                )) {
                    out.put(tagName, new String(is.readAllBytes(), StandardCharsets.UTF_8));
                }
            }

            return out;

        } catch (Exception e) {
            s3Client = null;
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }


    @Override
    public void deleteTags(String account, String container, List<String> tags) {

        String bucket = resolveBucket(account, container);
        S3Client client = getClient();
        ensureBucket(client, bucket);

        try {
            for (String t : tags) {
                client.deleteObject(
                        DeleteObjectRequest.builder()
                                .bucket(bucket)
                                .key(tagKey(t))
                                .build()
                );
            }

        } catch (Exception e) {
            s3Client = null;
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }


    // =====================================================================================
    // METADATA
    // =====================================================================================
    @Override
    public Map<String, Object> getMetaData(String account, String container,
                                           String source, String process, String objectName) {

        String bucket = resolveBucket(account, container);
        String key = buildKey(container, source, process, objectName);

        try {
            HeadObjectResponse head =
                    getClient().headObject(
                            HeadObjectRequest.builder().bucket(bucket).key(key).build()
                    );

            Map<String, Object> out = new HashMap<>();
            head.metadata().forEach(out::put);
            return out;

        } catch (Exception e) {
            s3Client = null;
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }


    @Override
    public Map<String, Object> addObjectMetaData(String account, String container,
                                                 String source, String process,
                                                 String objectName, Map<String, Object> metadata) {

        String bucket = resolveBucket(account, container);
        String key = buildKey(container, source, process, objectName);

        try (ResponseInputStream<GetObjectResponse> res =
                     getClient().getObject(
                             GetObjectRequest.builder().bucket(bucket).key(key).build()
                     )) {

            byte[] data = res.readAllBytes();

            Map<String, String> meta = new HashMap<>(res.response().metadata());
            metadata.forEach((k, v) -> meta.put(k, Objects.toString(v, null)));

            getClient().putObject(
                    PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .metadata(meta)
                            .build(),
                    RequestBody.fromBytes(data)
            );

            return metadata;

        } catch (Exception e) {
            s3Client = null;
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }


    @Override
    public Map<String, Object> addObjectMetaData(String account, String container,
                                                 String source, String process,
                                                 String objectName, String key, String value) {

        return addObjectMetaData(account, container, source, process, objectName,
                Map.of(key, value));
    }


    @Override
    public Integer incMetadata(String account, String container,
                               String source, String process,
                               String objectName, String metaKey) {

        Map<String, Object> m = getMetaData(account, container, source, process, objectName);
        if (!m.containsKey(metaKey)) return null;

        int v = Integer.parseInt(m.get(metaKey).toString()) + 1;
        m.put(metaKey, v);

        addObjectMetaData(account, container, source, process, objectName, m);
        return v;
    }


    @Override
    public Integer decMetadata(String account, String container,
                               String source, String process,
                               String objectName, String metaKey) {

        Map<String, Object> m = getMetaData(account, container, source, process, objectName);
        if (!m.containsKey(metaKey)) return null;

        int v = Integer.parseInt(m.get(metaKey).toString()) - 1;
        m.put(metaKey, v);

        addObjectMetaData(account, container, source, process, objectName, m);
        return v;
    }


    // =====================================================================================
    // LIST
    // =====================================================================================
    @Override
    public List<ObjectDto> getAllObjects(String account, String id) {

        List<S3Object> objects;

        if (useAccountAsBucketName) {
            String bucket = resolveBucket(account, account);
            objects = getClient().listObjectsV2(
                    ListObjectsV2Request.builder()
                            .bucket(bucket)
                            .prefix(id + SEPARATOR)
                            .build()
            ).contents();
        } else {
            String bucket = resolveBucket(id, id);
            objects = getClient().listObjectsV2(
                    ListObjectsV2Request.builder().bucket(bucket).build()
            ).contents();
        }

        if (objects == null || objects.isEmpty()) return null;

        List<ObjectDto> out = new ArrayList<>();

        for (S3Object o : objects) {

            String[] parts = o.key().split(SEPARATOR);

            if (parts.length > 0 && parts[0].equals("tags")) continue;

            if (useAccountAsBucketName && parts.length > 1) {
                parts = ArrayUtils.remove(parts, 0);
            }

            ObjectDto dto = null;

            switch (parts.length) {
                case 1 -> dto = new ObjectDto(null, null, parts[0], Date.from(o.lastModified()));
                case 2 -> dto = new ObjectDto(parts[0], null, parts[1], Date.from(o.lastModified()));
                case 3 -> dto = new ObjectDto(parts[0], parts[1], parts[2], Date.from(o.lastModified()));
            }

            if (dto != null) out.add(dto);
        }

        return out;
    }


    // =====================================================================================
    // UNUSED OPS (MOSIP interface requires)
    // =====================================================================================
    @Override public boolean removeContainer(String a, String c, String s, String p) { return false; }
    @Override public boolean pack(String a, String c, String s, String p, String r) { return false; }

}
