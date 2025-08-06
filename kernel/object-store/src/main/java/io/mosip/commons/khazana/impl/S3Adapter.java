package io.mosip.commons.khazana.impl;

import static io.mosip.commons.khazana.config.LoggerConfiguration.REGISTRATIONID;
import static io.mosip.commons.khazana.config.LoggerConfiguration.SESSIONID;
import static io.mosip.commons.khazana.constant.KhazanaConstant.TAGS_FILENAME;
import static io.mosip.commons.khazana.constant.KhazanaErrorCodes.OBJECT_STORE_NOT_ACCESSIBLE;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.Protocol;
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

    private static final String SEPARATOR = "/";
    private static final String TAG_BACKWARD_COMPATIBILITY_ERROR = "Object-prefix is already an object, please choose a different object-prefix name";
    private static final String TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR = "Access Denied";

    private final Set<String> existingBuckets = ConcurrentHashMap.newKeySet();
    private volatile S3Client s3Client;

    private S3Client getS3Client() {
        if (s3Client == null) {
            synchronized (this) {
                if (s3Client == null) {
                    s3Client = S3Client.builder()
                            .endpointOverride(java.net.URI.create(url))
                            .region(Region.of(region))
                            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                            .httpClientBuilder(ApacheHttpClient.builder()
                                    .maxConnections(maxConnection)
                                    .connectionTimeout(Duration.ofMillis(connectionTimeout))
                                    .socketTimeout(Duration.ofMillis(socketTimeout))
                            )
                            .build();
                }
            }
        }
        return s3Client;
    }

    private boolean doesBucketExist(String bucketName) {
        if (existingBuckets.contains(bucketName)) return true;
        try {
            getS3Client().headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
            existingBuckets.add(bucketName);
            return true;
        } catch (S3Exception e) {
            return false;
        }
    }

    private String addBucketPrefix(String bucketName) {
        if (bucketNamePrefix != null && !bucketNamePrefix.isEmpty() && !bucketName.startsWith(bucketNamePrefix)) {
            LOGGER.debug("Adding Prefix to bucketName " + bucketNamePrefix + bucketName);
            return bucketNamePrefix + bucketName;
        }
        return bucketName;
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
        bucketName = addBucketPrefix(bucketName).toLowerCase();
        try {
            S3Client client = getS3Client();
            GetObjectRequest req = GetObjectRequest.builder().bucket(bucketName).key(finalObjectName).build();
            return client.getObject(req);
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
        String finalObjectName;
        String bucketName;
        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
            bucketName = account;
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName = container;
        }
        bucketName = addBucketPrefix(bucketName).toLowerCase();
        try {
            S3Client client = getS3Client();
            HeadObjectRequest req = HeadObjectRequest.builder().bucket(bucketName).key(finalObjectName).build();
            client.headObject(req);
            return true;
        } catch (S3Exception e) {
            return false;
        }
    }

    @Override
    public boolean putObject(String account, String container, String source, String process, String objectName, InputStream data) {
        String finalObjectName;
        String bucketName;
        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
            bucketName = account;
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName = container;
        }
        bucketName = addBucketPrefix(bucketName).toLowerCase();

        S3Client client = getS3Client();

        if (!doesBucketExist(bucketName)) {
            client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
            existingBuckets.add(bucketName);
        }

        try {
            byte[] contentBytes = IOUtils.toByteArray(data);
            for (int attempt = 1; attempt <= maxRetry; attempt++) {
                try {
                    client.putObject(
                            PutObjectRequest.builder().bucket(bucketName).key(finalObjectName).build(),
                            RequestBody.fromBytes(contentBytes)
                    );
                    return true;
                } catch (SdkClientException | S3Exception e) {
                    LOGGER.error(SESSIONID, REGISTRATIONID, "S3 PutObject failed, attempt " + attempt, ExceptionUtils.getStackTrace(e));
                    if (attempt == maxRetry) throw e;
                    try { Thread.sleep(1000L * attempt); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                }
            }
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception during putObject for: " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
        return false;
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process, String objectName, Map<String, Object> metadata) {
        String finalObjectName;
        String bucketName;
        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
            bucketName = account;
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName = container;
        }
        bucketName = addBucketPrefix(bucketName).toLowerCase();
        S3Client client = getS3Client();
        try {
            GetObjectRequest getReq = GetObjectRequest.builder().bucket(bucketName).key(finalObjectName).build();
            try (ResponseInputStream<GetObjectResponse> s3Object = client.getObject(getReq)) {
                byte[] contentBytes = IOUtils.toByteArray(s3Object);
                HeadObjectResponse head = client.headObject(HeadObjectRequest.builder().bucket(bucketName).key(finalObjectName).build());
                Map<String, String> userMeta = new HashMap<>();
                if (head.metadata() != null) userMeta.putAll(head.metadata());
                metadata.forEach((k, v) -> userMeta.put(k, v != null ? v.toString() : null));

                client.putObject(
                        PutObjectRequest.builder()
                                .bucket(bucketName)
                                .key(finalObjectName)
                                .metadata(userMeta)
                                .build(),
                        RequestBody.fromBytes(contentBytes)
                );
            }
            return metadata;
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID,"Exception occured to addObjectMetaData for : " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
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
        String finalObjectName;
        String bucketName;
        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
            bucketName = account;
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName = container;
        }
        bucketName = addBucketPrefix(bucketName).toLowerCase();
        try {
            S3Client client = getS3Client();
            HeadObjectResponse head = client.headObject(HeadObjectRequest.builder().bucket(bucketName).key(finalObjectName).build());
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
        String finalObjectName;
        String bucketName;
        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
            bucketName = account;
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName = container;
        }
        bucketName = addBucketPrefix(bucketName).toLowerCase();
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
        List<S3Object> os = null;
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
        if (os != null && !os.isEmpty()) {
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
                    }
                    if (objectDto != null)
                        objectDtos.add(objectDto);
                }
            }
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
        try {
            if (useAccountAsBucketname) {
                bucketName = account;
                finalObjectName = ObjectStoreUtil.getName(container, null, TAGS_FILENAME);
            } else {
                bucketName = container;
                finalObjectName = TAGS_FILENAME;
            }
            bucketName = addBucketPrefix(bucketName).toLowerCase();
            S3Client client = getS3Client();
            if (!doesBucketExist(bucketName)) {
                client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
                existingBuckets.add(bucketName);
            }
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                String tagName = ObjectStoreUtil.getName(finalObjectName, entry.getKey());
                InputStream data = IOUtils.toInputStream(entry.getValue(), StandardCharsets.UTF_8);
                try {
                    client.putObject(PutObjectRequest.builder().bucket(bucketName).key(tagName).build(),
                            RequestBody.fromInputStream(data, entry.getValue().getBytes(StandardCharsets.UTF_8).length));
                } catch (S3Exception e) {
                    if (e.getMessage().contains(TAG_BACKWARD_COMPATIBILITY_ERROR)
                            || e.getMessage().contains(TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR)) {
                        if (client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).prefix(finalObjectName).build()).keyCount() > 0) {
                            client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(finalObjectName).build());
                            addTags(account, container, tags);
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
            S3Client client = getS3Client();

            List<S3Object> objectSummary;
            if (useAccountAsBucketname)
                objectSummary = client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).prefix(finalObjectName).build()).contents();
            else
                objectSummary = client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).build()).contents();

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
                objectTags.put(tagName, client.getObjectAsBytes(GetObjectRequest.builder().bucket(bucketName).key(finalObjectName + tagName).build()).asUtf8String());
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
            S3Client client = getS3Client();
            if (!doesBucketExist(bucketName)) {
                client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
                existingBuckets.add(bucketName);
            }
            for (String tag : tags) {
                String tagName = ObjectStoreUtil.getName(finalObjectName, tag);
                client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(tagName).build());
            }
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured while deleteTags for : " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }
}