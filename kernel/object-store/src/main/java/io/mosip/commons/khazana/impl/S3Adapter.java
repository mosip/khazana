package io.mosip.commons.khazana.impl;

import static io.mosip.commons.khazana.config.LoggerConfiguration.REGISTRATIONID;
import static io.mosip.commons.khazana.config.LoggerConfiguration.SESSIONID;
import static io.mosip.commons.khazana.constant.KhazanaConstant.TAGS_FILENAME;
import static io.mosip.commons.khazana.constant.KhazanaErrorCodes.OBJECT_STORE_NOT_ACCESSIBLE;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.annotation.PostConstruct;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;

import io.mosip.commons.khazana.config.LoggerConfiguration;
import io.mosip.commons.khazana.dto.ObjectDto;
import io.mosip.commons.khazana.exception.ObjectStoreAdapterException;
import io.mosip.commons.khazana.spi.ObjectStoreAdapter;
import io.mosip.commons.khazana.util.ObjectStoreUtil;
import io.mosip.kernel.core.exception.ExceptionUtils;
import io.mosip.kernel.core.logger.spi.Logger;

@Service
public class S3Adapter implements ObjectStoreAdapter {

    private static final String SEPARATOR = "/";
    private static final String TAG_BACKWARD_COMPATIBILITY_ERROR = "Object-prefix is already an object, please choose a different object-prefix name";
    private static final String TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR = "Access Denied";

    private final Logger LOGGER = LoggerConfiguration.logConfig(S3Adapter.class);

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

    private AmazonS3 s3Client;

    private final Set<String> existingBuckets = ConcurrentHashMap.newKeySet();

    @PostConstruct
    public void init() {
        var awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
        var clientConfig = new ClientConfiguration()
                .withConnectionTimeout(connectionTimeout)
                .withSocketTimeout(socketTimeout)
                .withClientExecutionTimeout(clientExecutionTimeout)
                .withMaxConnections(maxConnection)
                .withMaxErrorRetry(maxRetry)
                .withConnectionMaxIdleMillis(60_000); // Clean up idle connections

        s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .withClientConfiguration(clientConfig)
                .enablePathStyleAccess()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(url, region))
                .build();
    }

    private String addBucketPrefix(String bucketName) {
        return (bucketNamePrefix != null && !bucketNamePrefix.isEmpty() && !bucketName.startsWith(bucketNamePrefix))
                ? bucketNamePrefix + bucketName
                : bucketName;
    }

    private boolean doesBucketExist(String bucketName) {
        if (existingBuckets.contains(bucketName)) return true;
        var exists = s3Client.doesBucketExistV2(bucketName);
        if (exists) existingBuckets.add(bucketName);
        return exists;
    }

    @Override
    public InputStream getObject(String account, String container, String source, String process, String objectName) {
        var bucketName = resolveBucketName(account, container);
        var finalObjectName = resolveObjectName(account, container, source, process, objectName);
        try {
            var s3Object = s3Client.getObject(bucketName, finalObjectName);
            return s3Object.getObjectContent();
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception getObject for: " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public boolean exists(String account, String container, String source, String process, String objectName) {
        var bucketName = resolveBucketName(account, container);
        var finalObjectName = resolveObjectName(account, container, source, process, objectName);
        return s3Client.doesObjectExist(bucketName, finalObjectName);
    }

    @Override
    public boolean putObject(String account, String container, String source, String process, String objectName, InputStream data) {
        var bucketName = resolveBucketName(account, container);
        var finalObjectName = resolveObjectName(account, container, source, process, objectName);
        int attempts = 0;
        while (attempts < 2) {
            try {
                var metadata = new ObjectMetadata();
                s3Client.putObject(bucketName, finalObjectName, data, metadata);
                existingBuckets.add(bucketName);
                return true;
            } catch (AmazonS3Exception e) {
                if (e.getStatusCode() == 404 && "NoSuchBucket".equalsIgnoreCase(e.getErrorCode()) && attempts == 0) {
                    synchronized (existingBuckets) {
                        if (!existingBuckets.contains(bucketName)) {
                            s3Client.createBucket(bucketName);
                            existingBuckets.add(bucketName);
                        }
                    }
                    attempts++;
                } else {
                    throw e;
                }
            } catch (Exception e) {
                LOGGER.error(SESSIONID, REGISTRATIONID, "Exception putObject for: " + container, ExceptionUtils.getStackTrace(e));
                throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                        OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
            }
        }
        return false;
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process, String objectName, Map<String, Object> metadata) {
        var bucketName = resolveBucketName(account, container);
        var finalObjectName = resolveObjectName(account, container, source, process, objectName);
        try {
            var s3Object = s3Client.getObject(bucketName, finalObjectName);
            var objectMetadata = new ObjectMetadata();
            if (s3Object.getObjectMetadata() != null && s3Object.getObjectMetadata().getUserMetadata() != null) {
                objectMetadata.setUserMetadata(new HashMap<>(s3Object.getObjectMetadata().getUserMetadata()));
            }
            metadata.forEach((k, v) -> objectMetadata.addUserMetadata(k, v != null ? v.toString() : null));
            try (var content = s3Object.getObjectContent()) {
                var req = new PutObjectRequest(bucketName, finalObjectName, content, objectMetadata);
                req.getRequestClientOptions().setReadLimit(readlimit);
                s3Client.putObject(req);
            }
            return metadata;
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception addObjectMetaData for: " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
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
        var bucketName = resolveBucketName(account, container);
        var finalObjectName = resolveObjectName(account, container, source, process, objectName);
        Map<String, Object> metaData = new HashMap<>();
        try {
            var s3Object = s3Client.getObject(bucketName, finalObjectName);
            var objectMetadata = s3Object.getObjectMetadata();
            if (objectMetadata != null && objectMetadata.getUserMetadata() != null) {
                metaData.putAll(objectMetadata.getUserMetadata());
            }
            return metaData;
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception getMetaData for: " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public Integer incMetadata(String account, String container, String source, String process, String objectName, String metaDataKey) {
        var metadata = getMetaData(account, container, source, process, objectName);
        if (metadata.get(metaDataKey) != null) {
            metadata.put(metaDataKey, Integer.valueOf(metadata.get(metaDataKey).toString()) + 1);
            addObjectMetaData(account, container, source, process, objectName, metadata);
            return Integer.valueOf(metadata.get(metaDataKey).toString());
        }
        return null;
    }

    @Override
    public Integer decMetadata(String account, String container, String source, String process, String objectName, String metaDataKey) {
        var metadata = getMetaData(account, container, source, process, objectName);
        if (metadata.get(metaDataKey) != null) {
            metadata.put(metaDataKey, Integer.valueOf(metadata.get(metaDataKey).toString()) - 1);
            addObjectMetaData(account, container, source, process, objectName, metadata);
            return Integer.valueOf(metadata.get(metaDataKey).toString());
        }
        return null;
    }

    @Override
    public boolean deleteObject(String account, String container, String source, String process, String objectName) {
        var bucketName = resolveBucketName(account, container);
        var finalObjectName = resolveObjectName(account, container, source, process, objectName);
        try {
            s3Client.deleteObject(bucketName, finalObjectName);
            return true;
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception deleteObject for: " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public boolean removeContainer(String account, String container, String source, String process) { return false; }
    @Override
    public boolean pack(String account, String container, String source, String process, String refId) { return false; }

    public List<ObjectDto> getAllObjects(String account, String id) {
        var objectSummaries = new ArrayList<S3ObjectSummary>();
        var bucketName = useAccountAsBucketname ? addBucketPrefix(account).toLowerCase() : addBucketPrefix(id).toLowerCase();
        var prefix = useAccountAsBucketname ? id + SEPARATOR : "";

        try {
            var req = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(prefix);
            ListObjectsV2Result result;
            do {
                result = s3Client.listObjectsV2(req);
                objectSummaries.addAll(result.getObjectSummaries());
                req.setContinuationToken(result.getNextContinuationToken());
            } while (result.isTruncated());
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception getAllObjects for: " + id, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
        if (!objectSummaries.isEmpty()) {
            var objectDtos = new ArrayList<ObjectDto>();
            objectSummaries.forEach(o -> {
                var tempKeys = o.getKey().split("/");
                if (useAccountAsBucketname) {
                    if (tempKeys.length > 1 && tempKeys[1] != null && tempKeys[1].endsWith(TAGS_FILENAME))
                        return;
                } else {
                    if (tempKeys.length > 0 && tempKeys[0] != null && tempKeys[0].endsWith(TAGS_FILENAME))
                        return;
                }
                var keys = removeIdFromObjectPath(useAccountAsBucketname, tempKeys);
                if (ArrayUtils.isNotEmpty(keys)) {
                    ObjectDto objectDto = null;
                    switch (keys.length) {
                        case 1 -> objectDto = new ObjectDto(null, null, keys[0], o.getLastModified());
                        case 2 -> objectDto = new ObjectDto(keys[0], null, keys[1], o.getLastModified());
                        case 3 -> objectDto = new ObjectDto(keys[0], keys[1], keys[2], o.getLastModified());
                    }
                    if (objectDto != null) objectDtos.add(objectDto);
                }
            });
            return objectDtos;
        }
        return null;
    }

    private String[] removeIdFromObjectPath(boolean isAccountBucket, String[] keys) {
        return (isAccountBucket && ArrayUtils.isNotEmpty(keys))
                ? (String[]) ArrayUtils.remove(keys, 0) : keys;
    }

    @Override
    public Map<String, String> addTags(String account, String container, Map<String, String> tags) {
        String bucketName, finalObjectName;
        try {
            if (useAccountAsBucketname) {
                bucketName = account;
                finalObjectName = ObjectStoreUtil.getName(container, null, TAGS_FILENAME);
            } else {
                bucketName = container;
                finalObjectName = TAGS_FILENAME;
            }
            bucketName = addBucketPrefix(bucketName).toLowerCase();

            int attempts = 0;
            while (attempts < 2) {
                try {
                    for (var entry : tags.entrySet()) {
                        var tagName = ObjectStoreUtil.getName(finalObjectName, entry.getKey());
                        var data = IOUtils.toInputStream(entry.getValue(), StandardCharsets.UTF_8);
                        s3Client.putObject(bucketName, tagName, data, null);
                    }
                    existingBuckets.add(bucketName);
                    return tags;
                } catch (AmazonS3Exception e) {
                    if ((e.getMessage().contains(TAG_BACKWARD_COMPATIBILITY_ERROR)
                            || e.getMessage().contains(TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR))
                            && attempts == 0) {
                        if (s3Client.doesObjectExist(bucketName, finalObjectName)) {
                            s3Client.deleteObject(bucketName, finalObjectName);
                            attempts++;
                        }
                    } else if (e.getStatusCode() == 404 && "NoSuchBucket".equalsIgnoreCase(e.getErrorCode())
                            && attempts == 0) {
                        synchronized (existingBuckets) {
                            if (!existingBuckets.contains(bucketName)) {
                                s3Client.createBucket(bucketName);
                                existingBuckets.add(bucketName);
                            }
                        }
                        attempts++;
                    } else {
                        LOGGER.error(SESSIONID, REGISTRATIONID, "Exception addTags for: " + container, ExceptionUtils.getStackTrace(e));
                        throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                                OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception addTags for: " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
        return tags;
    }

    @Override
    public Map<String, String> getTags(String account, String container) {
        var objectTags = new HashMap<String, String>();
        try {
            String bucketName, finalObjectName;
            if (useAccountAsBucketname) {
                bucketName = account;
                finalObjectName = ObjectStoreUtil.getName(container, null, TAGS_FILENAME) + SEPARATOR;
            } else {
                bucketName = container;
                finalObjectName = TAGS_FILENAME + SEPARATOR;
            }
            bucketName = addBucketPrefix(bucketName).toLowerCase();

            var objectSummary = new ArrayList<S3ObjectSummary>();
            var req = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(finalObjectName);
            ListObjectsV2Result result;
            do {
                result = s3Client.listObjectsV2(req);
                objectSummary.addAll(result.getObjectSummaries());
                req.setContinuationToken(result.getNextContinuationToken());
            } while (result.isTruncated());

            var tagNames = new ArrayList<String>();
            objectSummary.forEach(o -> {
                var keys = o.getKey().split("/");
                if (ArrayUtils.isNotEmpty(keys)) {
                    if (useAccountAsBucketname) {
                        if (keys.length > 1 && keys[1] != null && keys[1].endsWith(TAGS_FILENAME) && keys.length > 2)
                            tagNames.add(keys[2]);
                    } else {
                        if (keys.length > 0 && keys[0] != null && keys[0].endsWith(TAGS_FILENAME) && keys.length > 1)
                            tagNames.add(keys[1]);
                    }
                }
            });
            for (var tagName : tagNames) {
                objectTags.put(tagName, s3Client.getObjectAsString(bucketName, finalObjectName + tagName));
            }
            return objectTags;
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception getTags for: " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public void deleteTags(String account, String container, List<String> tags) {
        try {
            String bucketName, finalObjectName;
            if (useAccountAsBucketname) {
                bucketName = account;
                finalObjectName = ObjectStoreUtil.getName(container, null, TAGS_FILENAME);
            } else {
                bucketName = container;
                finalObjectName = TAGS_FILENAME;
            }
            bucketName = addBucketPrefix(bucketName).toLowerCase();

            int attempts = 0;
            while (attempts < 2) {
                try {
                    for (var tag : tags) {
                        var tagName = ObjectStoreUtil.getName(finalObjectName, tag);
                        s3Client.deleteObject(bucketName, tagName);
                    }
                    return;
                } catch (AmazonS3Exception e) {
                    if (e.getStatusCode() == 404 && "NoSuchBucket".equalsIgnoreCase(e.getErrorCode())
                            && attempts == 0) {
                        synchronized (existingBuckets) {
                            if (!existingBuckets.contains(bucketName)) {
                                s3Client.createBucket(bucketName);
                                existingBuckets.add(bucketName);
                            }
                        }
                        attempts++;
                    } else {
                        LOGGER.error(SESSIONID, REGISTRATIONID, "Exception deleteTags for: " + container, ExceptionUtils.getStackTrace(e));
                        throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                                OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception deleteTags for: " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    // ===== New Java 21-style helpers =====
    private String resolveBucketName(String account, String container) {
        return addBucketPrefix(useAccountAsBucketname ? account : container).toLowerCase();
    }
    private String resolveObjectName(String account, String container, String source, String process, String objectName) {
        return useAccountAsBucketname
                ? ObjectStoreUtil.getName(container, source, process, objectName)
                : ObjectStoreUtil.getName(source, process, objectName);
    }
}