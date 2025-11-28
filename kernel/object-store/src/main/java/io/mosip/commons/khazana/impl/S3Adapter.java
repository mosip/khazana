package io.mosip.commons.khazana.impl;

import io.mosip.commons.khazana.config.LoggerConfiguration;
import io.mosip.commons.khazana.dto.ObjectDto;
import io.mosip.commons.khazana.exception.ObjectStoreAdapterException;
import io.mosip.commons.khazana.spi.ObjectStoreAdapter;
import io.mosip.commons.khazana.util.ObjectStoreUtil;
import io.mosip.kernel.core.logger.spi.Logger;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Qualifier;
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
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static io.mosip.commons.khazana.config.LoggerConfiguration.*;
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
    private String endpoint;

    @Value("${object.store.s3.region:us-east-1}")
    private String region;

    @Value("${object.store.connection.max.retry:20}")
    private int maxRetry;

    @Value("${object.store.max.connection:200}")
    private int maxConnection;

    @Value("${object.store.connection.timeout:8000}")
    private int connectionTimeout;

    @Value("${object.store.socket.timeout:60000}")
    private int socketTimeout;

    @Value("${object.store.s3.use.account.as.bucketname:false}")
    private boolean useAccountAsBucketname;

    @Value("${object.store.s3.bucket-name-prefix:}")
    private String bucketNamePrefix;

    private volatile S3Client s3Client;

    /** Thread-safe cache of buckets already created */
    private final Set<String> existingBuckets = ConcurrentHashMap.newKeySet();


    /* =====================================================================================
       CLIENT CREATION — FINAL & CORRECT FOR AWS SDK v2
       ===================================================================================== */

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

            S3Configuration s3cfg = S3Configuration.builder()
                    .pathStyleAccessEnabled(true) // REQUIRED FOR MINIO
                    .build();

            S3ClientBuilder builder = S3Client.builder()
                    .credentialsProvider(StaticCredentialsProvider.create(creds))
                    .httpClientBuilder(http)
                    .serviceConfiguration(s3cfg)
                    .region(Region.of(region));

            if (endpoint != null && !endpoint.isBlank()) {
                builder = builder.endpointOverride(URI.create(endpoint));
            }

            // retry mechanism
            for (int i = 0; i < maxRetry; i++) {
                try {
                    s3Client = builder.build();
                    return s3Client;
                } catch (Exception e) {
                    if (i == maxRetry - 1)
                        throw new ObjectStoreAdapterException(
                                OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                                OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);

                    try { Thread.sleep(100); } catch (InterruptedException ignored) {}
                }
            }
        }

        return s3Client;
    }


    /* =====================================================================================
       BUCKET HANDLING
       ===================================================================================== */

    private String resolveBucket(String account, String container) {
        String bucket = useAccountAsBucketname ? account : container;

        if (bucketNamePrefix != null && !bucketNamePrefix.isBlank()
                && !bucket.startsWith(bucketNamePrefix)) {
            bucket = bucketNamePrefix + bucket;
        }

        return bucket.toLowerCase();
    }

    private void ensureBucket(String bucket) {
        S3Client client = getClient();

        if (existingBuckets.contains(bucket)) return;

        try {
            client.headBucket(HeadBucketRequest.builder().bucket(bucket).build());
        } catch (Exception e) {
            client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        }

        existingBuckets.add(bucket);
    }


    /* =====================================================================================
       KEY BUILDER
       ===================================================================================== */

    private String key(String container, String source, String process, String objectName) {
        return useAccountAsBucketname
                ? ObjectStoreUtil.getName(container, source, process, objectName)
                : ObjectStoreUtil.getName(source, process, objectName);
    }


    /* =====================================================================================
       GET (Streaming)
       ===================================================================================== */

    @Override
    public InputStream getObject(String account, String container,
                                 String source, String process, String objectName) {

        String bucket = resolveBucket(account, container);
        String k = key(container, source, process, objectName);

        try {
            ResponseInputStream<GetObjectResponse> r =
                    getClient().getObject(
                            GetObjectRequest.builder().bucket(bucket).key(k).build()
                    );

            return new BufferedInputStream(r, 64 * 1024);

        } catch (Exception e) {
            s3Client = null;
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    "Failed to read object " + k, e);
        }
    }


    /* =====================================================================================
       EXISTS
       ===================================================================================== */

    @Override
    public boolean exists(String account, String container,
                          String source, String process, String objectName) {

        String bucket = resolveBucket(account, container);
        String k = key(container, source, process, objectName);

        try {
            getClient().headObject(
                    HeadObjectRequest.builder().bucket(bucket).key(k).build()
            );
            return true;
        } catch (S3Exception e) {
            return false;
        }
    }


    /* =====================================================================================
       PUT (streaming + zero-copy if possible)
       ===================================================================================== */

    @Override
    public boolean putObject(String account, String container,
                             String source, String process,
                             String objectName, InputStream in) {

        String bucket = resolveBucket(account, container);
        String k = key(container, source, process, objectName);

        ensureBucket(bucket);

        try {
            RequestBody body;

            if (in instanceof FileInputStream fis) {
                body = RequestBody.fromInputStream(in, fis.getChannel().size());
            } else if (in instanceof ByteArrayInputStream bais) {
                body = RequestBody.fromBytes(bais.readAllBytes());
            } else {
                Path tmp = Files.createTempFile("put-", ".tmp");
                Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
                body = RequestBody.fromFile(tmp);
                Files.deleteIfExists(tmp);
            }

            getClient().putObject(
                    PutObjectRequest.builder().bucket(bucket).key(k).build(),
                    body
            );

            return true;

        } catch (Exception e) {
            s3Client = null;
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    "Failed to put object " + k, e);
        }
    }


    /* =====================================================================================
       DELETE
       ===================================================================================== */

    @Override
    public boolean deleteObject(String account, String container,
                                String source, String process, String objectName) {

        String bucket = resolveBucket(account, container);
        String k = key(container, source, process, objectName);

        getClient().deleteObject(
                DeleteObjectRequest.builder().bucket(bucket).key(k).build()
        );

        return true;
    }


    /* =====================================================================================
       METADATA
       ===================================================================================== */

    @Override
    public Map<String, Object> getMetaData(String account, String container,
                                           String source, String process, String objectName) {

        String bucket = resolveBucket(account, container);
        String k = key(container, source, process, objectName);

        try {
            HeadObjectResponse r =
                    getClient().headObject(
                            HeadObjectRequest.builder().bucket(bucket).key(k).build()
                    );

            Map<String, Object> out = new HashMap<>();
            r.metadata().forEach(out::put);

            return out;

        } catch (Exception e) {
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    "Failed to read metadata for " + k, e);
        }
    }


    @Override
    public Map<String, Object> addObjectMetaData(String account, String container,
                                                 String source, String process,
                                                 String objectName,
                                                 Map<String, Object> metadata) {

        String bucket = resolveBucket(account, container);
        String k = key(container, source, process, objectName);

        try (ResponseInputStream<GetObjectResponse> obj =
                     getClient().getObject(
                             GetObjectRequest.builder()
                                     .bucket(bucket).key(k).build()
                     )) {

            byte[] data = obj.readAllBytes();

            Map<String, String> meta = new HashMap<>(obj.response().metadata());
            metadata.forEach((kk, vv) -> meta.put(kk, Objects.toString(vv, "")));

            getClient().putObject(
                    PutObjectRequest.builder().bucket(bucket).key(k).metadata(meta).build(),
                    RequestBody.fromBytes(data)
            );

            return metadata;

        } catch (Exception e) {
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    "Failed to update metadata " + k, e);
        }
    }


    /* =====================================================================================
       LIST OBJECTS
       ===================================================================================== */

    @Override
    public List<ObjectDto> getAllObjects(String account, String container) {

        String bucket = resolveBucket(account, container);

        List<S3Object> objs = getClient().listObjectsV2(
                ListObjectsV2Request.builder().bucket(bucket).build()
        ).contents();

        List<ObjectDto> out = new ArrayList<>();

        for (S3Object o : objs) {
            String[] parts = o.key().split("/");
            if (parts.length == 3) {
                out.add(new ObjectDto(parts[0], parts[1], parts[2], Date.from(o.lastModified())));
            }
        }

        return out;
    }


    /* =====================================================================================
       TAGS (simple version)
       ===================================================================================== */

    @Override
    public Map<String, String> addTags(String account, String container,
                                       Map<String, String> tags) {
        String bucket = resolveBucket(account, container);

        ensureBucket(bucket);

        for (var e : tags.entrySet()) {
            String k = "tags/" + e.getKey();

            getClient().putObject(
                    PutObjectRequest.builder().bucket(bucket).key(k).build(),
                    RequestBody.fromBytes(e.getValue().getBytes())
            );
        }

        return tags;
    }


    @Override
    public Map<String, String> getTags(String account, String container) {

        String bucket = resolveBucket(account, container);
        Map<String, String> out = new HashMap<>();

        List<S3Object> objs =
                getClient().listObjectsV2(
                        ListObjectsV2Request.builder()
                                .bucket(bucket)
                                .prefix("tags/")
                                .build()
                ).contents();

        for (S3Object o : objs) {
            String name = o.key().substring("tags/".length());
            try (InputStream is = getClient().getObject(
                    GetObjectRequest.builder().bucket(bucket).key(o.key()).build()
            )) {
                out.put(name, new String(is.readAllBytes()));
            } catch (IOException ignored) {}
        }

        return out;
    }


    @Override
    public void deleteTags(String account, String container, List<String> tags) {
        String bucket = resolveBucket(account, container);

        for (String t : tags) {
            getClient().deleteObject(
                    DeleteObjectRequest.builder()
                            .bucket(bucket)
                            .key("tags/" + t)
                            .build()
            );
        }
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source,
                                                 String process, String objectName,
                                                 String key, String value) {
        Map<String, Object> meta = new HashMap<>();
        meta.put(key, value);
        // In v1 they recomputed finalObjectName then passed it back;
        // here we keep it logically correct: objectName is still the actual objectName.
        return addObjectMetaData(account, container, source, process, objectName, meta);
    }
    @Override
    public Integer incMetadata(String account, String container, String source,
                               String process, String objectName, String metaDataKey) {
        Map<String, Object> metadata = getMetaData(account, container, source, process, objectName);
        if (metadata.get(metaDataKey) != null) {
            int newValue = Integer.parseInt(metadata.get(metaDataKey).toString()) + 1;
            metadata.put(metaDataKey, newValue);
            addObjectMetaData(account, container, source, process, objectName, metadata);
            return newValue;
        }
        return null;
    }

    @Override
    public Integer decMetadata(String account, String container, String source,
                               String process, String objectName, String metaDataKey) {
        Map<String, Object> metadata = getMetaData(account, container, source, process, objectName);
        if (metadata.get(metaDataKey) != null) {
            int newValue = Integer.parseInt(metadata.get(metaDataKey).toString()) - 1;
            metadata.put(metaDataKey, newValue);
            addObjectMetaData(account, container, source, process, objectName, metadata);
            return newValue;
        }
        return null;
    }

    @Override public boolean removeContainer(String account, String container, String source, String process) { return false; }
    @Override public boolean pack(String account, String container, String source, String process, String refId) { return false; }
}
