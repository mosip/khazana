package io.mosip.commons.khazana.impl;

import static io.mosip.commons.khazana.config.LoggerConfiguration.REGISTRATIONID;
import static io.mosip.commons.khazana.config.LoggerConfiguration.SESSIONID;
import static io.mosip.commons.khazana.constant.KhazanaConstant.TAGS_FILENAME;
import static io.mosip.commons.khazana.constant.KhazanaErrorCodes.OBJECT_STORE_NOT_ACCESSIBLE;

import java.io.InputStream;
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
    // Use thread-safe set for existing buckets
    private final Set<String> existingBuckets = ConcurrentHashMap.newKeySet();

    private static final String SEPARATOR = "/";
    private static final String TAG_BACKWARD_COMPATIBILITY_ERROR = "Object-prefix is already an object, please choose a different object-prefix name";
    private static final String TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR = "Access Denied";

    @PostConstruct
    public void init() {
        AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
        ClientConfiguration clientConfig = new ClientConfiguration()
                .withConnectionTimeout(connectionTimeout)
                .withSocketTimeout(socketTimeout)
                .withClientExecutionTimeout(clientExecutionTimeout)
                .withMaxConnections(maxConnection)
                .withMaxErrorRetry(maxRetry);

        this.s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .enablePathStyleAccess()
                .withClientConfiguration(clientConfig)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(url, region))
                .build();
    }

    private String addBucketPrefix(String bucketName) {
        if (bucketNamePrefix != null && !bucketNamePrefix.isEmpty() && !bucketName.startsWith(bucketNamePrefix)) {
            return bucketNamePrefix + bucketName;
        }
        return bucketName;
    }

    private boolean doesBucketExist(String bucketName) {
        if (existingBuckets.contains(bucketName)) return true;
        boolean exists = s3Client.doesBucketExistV2(bucketName);
        if (exists) existingBuckets.add(bucketName);
        return exists;
    }

    // --- Core S3 Operations ---

    @Override
    public InputStream getObject(String account, String container, String source, String process, String objectName) {
        String bucketName;
        String finalObjectName;
        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
            bucketName = account;
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName = container;
        }
        bucketName = addBucketPrefix(bucketName).toLowerCase();
        try {
            S3Object s3Object = s3Client.getObject(bucketName, finalObjectName);
            // Return streaming InputStream directly
            return s3Object.getObjectContent();
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception getObject for: " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public boolean exists(String account, String container, String source, String process, String objectName) {
        String bucketName;
        String finalObjectName;
        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
            bucketName = account;
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName = container;
        }
        bucketName = addBucketPrefix(bucketName).toLowerCase();
        return s3Client.doesObjectExist(bucketName, finalObjectName);
    }

    @Override
    public boolean putObject(String account, String container, String source, String process, String objectName, InputStream data) {
        String bucketName;
        String finalObjectName;
        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
            bucketName = account;
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName = container;
        }
        bucketName = addBucketPrefix(bucketName).toLowerCase();
        if (!doesBucketExist(bucketName)) {
            s3Client.createBucket(bucketName);
            existingBuckets.add(bucketName);
        }
        // Streaming upload, no buffering!
        ObjectMetadata metadata = new ObjectMetadata();
        // Set content-length if known: metadata.setContentLength(len);
        s3Client.putObject(bucketName, finalObjectName, data, metadata);
        return true;
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process,
                                                 String objectName, Map<String, Object> metadata) {
        String bucketName;
        String finalObjectName;
        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
            bucketName = account;
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName = container;
        }
        bucketName = addBucketPrefix(bucketName).toLowerCase();
        try {
            S3Object s3Object = s3Client.getObject(bucketName, finalObjectName);
            ObjectMetadata objectMetadata = new ObjectMetadata();
            // Copy existing user metadata
            if (s3Object.getObjectMetadata() != null && s3Object.getObjectMetadata().getUserMetadata() != null) {
                objectMetadata.setUserMetadata(new HashMap<>(s3Object.getObjectMetadata().getUserMetadata()));
            }
            metadata.forEach((k, v) -> objectMetadata.addUserMetadata(k, v != null ? v.toString() : null));
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, finalObjectName, s3Object.getObjectContent(), objectMetadata);
            putObjectRequest.getRequestClientOptions().setReadLimit(readlimit);
            s3Client.putObject(putObjectRequest);
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
    public Map<String, Object> getMetaData(String account, String container, String source, String process,
                                           String objectName) {
        String bucketName;
        String finalObjectName;
        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
            bucketName = account;
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName = container;
        }
        bucketName = addBucketPrefix(bucketName).toLowerCase();
        Map<String, Object> metaData = new HashMap<>();
        try {
            S3Object s3Object = s3Client.getObject(bucketName, finalObjectName);
            ObjectMetadata objectMetadata = s3Object.getObjectMetadata();
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
        String bucketName;
        String finalObjectName;
        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
            bucketName = account;
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName = container;
        }
        bucketName = addBucketPrefix(bucketName).toLowerCase();
        s3Client.deleteObject(bucketName, finalObjectName);
        return true;
    }

    @Override
    public boolean removeContainer(String account, String container, String source, String process) {
        return false;
    }

    @Override
    public boolean pack(String account, String container, String source, String process, String refId) {
        return false;
    }

    public List<ObjectDto> getAllObjects(String account, String id) {
        List<S3ObjectSummary> os;
        if (useAccountAsBucketname) {
            String searchPattern = id + SEPARATOR;
            account = addBucketPrefix(account).toLowerCase();
            os = s3Client.listObjects(account, searchPattern).getObjectSummaries();
        } else {
            id = addBucketPrefix(id).toLowerCase();
            os = s3Client.listObjects(id).getObjectSummaries();
        }
        if (os != null && !os.isEmpty()) {
            List<ObjectDto> objectDtos = new ArrayList<>();
            os.forEach(o -> {
                String[] tempKeys = o.getKey().split("/");
                if (useAccountAsBucketname) {
                    if (tempKeys[1] != null && tempKeys[1].endsWith(TAGS_FILENAME))
                        tempKeys = null;
                } else {
                    if (tempKeys[0] != null && tempKeys[0].endsWith(TAGS_FILENAME))
                        tempKeys = null;
                }
                String[] keys = removeIdFromObjectPath(useAccountAsBucketname, tempKeys);
                if (ArrayUtils.isNotEmpty(keys)) {
                    ObjectDto objectDto = null;
                    switch (keys.length) {
                        case 1:
                            objectDto = new ObjectDto(null, null, keys[0], o.getLastModified());
                            break;
                        case 2:
                            objectDto = new ObjectDto(keys[0], null, keys[1], o.getLastModified());
                            break;
                        case 3:
                            objectDto = new ObjectDto(keys[0], keys[1], keys[2], o.getLastModified());
                            break;
                    }
                    if (objectDto != null)
                        objectDtos.add(objectDto);
                }
            });
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
            if (!doesBucketExist(bucketName)) {
                s3Client.createBucket(bucketName);
                existingBuckets.add(bucketName);
            }
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                String tagName = ObjectStoreUtil.getName(finalObjectName, entry.getKey());
                InputStream data = IOUtils.toInputStream(entry.getValue(), StandardCharsets.UTF_8);
                try {
                    s3Client.putObject(bucketName, tagName, data, null);
                } catch (Exception e) {
                    // backward compatibility logic
                    if (e instanceof AmazonS3Exception && (e.getMessage().contains(TAG_BACKWARD_COMPATIBILITY_ERROR)
                            || e.getMessage().contains(TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR))) {
                        if (s3Client.doesObjectExist(bucketName, finalObjectName)) {
                            s3Client.deleteObject(bucketName, finalObjectName);
                            addTags(account, container, tags);
                        } else {
                            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception addTags for: " + container, ExceptionUtils.getStackTrace(e));
                            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
                        }
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
            List<S3ObjectSummary> objectSummary;
            if (useAccountAsBucketname)
                objectSummary = s3Client.listObjects(bucketName, finalObjectName).getObjectSummaries();
            else
                objectSummary = s3Client.listObjects(bucketName).getObjectSummaries();

            List<String> tagNames = new ArrayList<>();
            if (objectSummary != null && objectSummary.size() > 0) {
                objectSummary.forEach(o -> {
                    String[] keys = o.getKey().split("/");
                    if (ArrayUtils.isNotEmpty(keys)) {
                        if (useAccountAsBucketname) {
                            if (keys[1] != null && keys[1].endsWith(TAGS_FILENAME))
                                tagNames.add(keys[2]);
                        } else {
                            if (keys[0] != null && keys[0].endsWith(TAGS_FILENAME))
                                tagNames.add(keys[1]);
                        }
                    }
                });
            }
            for (String tagName : tagNames) {
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
            if (!doesBucketExist(bucketName)) {
                s3Client.createBucket(bucketName);
                existingBuckets.add(bucketName);
            }
            for (String tag : tags) {
                String tagName = ObjectStoreUtil.getName(finalObjectName, tag);
                s3Client.deleteObject(bucketName, tagName);
            }
        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception deleteTags for: " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }
}