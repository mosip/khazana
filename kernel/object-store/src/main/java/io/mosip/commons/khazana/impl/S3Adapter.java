package io.mosip.commons.khazana.impl;

import static io.mosip.commons.khazana.config.LoggerConfiguration.REGISTRATIONID;
import static io.mosip.commons.khazana.config.LoggerConfiguration.SESSIONID;
import static io.mosip.commons.khazana.constant.KhazanaConstant.TAGS_FILENAME;
import static io.mosip.commons.khazana.constant.KhazanaErrorCodes.OBJECT_STORE_NOT_ACCESSIBLE;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import io.mosip.commons.khazana.util.SafeS3InputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import io.mosip.commons.khazana.config.LoggerConfiguration;
import io.mosip.commons.khazana.dto.ObjectDto;
import io.mosip.commons.khazana.exception.ObjectStoreAdapterException;
import io.mosip.commons.khazana.spi.ObjectStoreAdapter;
import io.mosip.commons.khazana.util.ObjectStoreUtil;
import io.mosip.kernel.core.exception.ExceptionUtils;
import io.mosip.kernel.core.logger.spi.Logger;

/**
 * S3 Object Store Adapter with proper stream handling to prevent connection leaks.
 */
@Service
@Qualifier("S3Adapter")
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

    @Value("${object.store.s3.stream.buffer.size:8192}")
    private int streamBufferSize;

    private int retry = 0;

    private List<String> existingBuckets = new ArrayList<>();

    private AmazonS3 connection = null;

    private static final String SEPARATOR = "/";

    private static final String TAG_BACKWARD_COMPATIBILITY_ERROR = "Object-prefix is already an object, please choose a different object-prefix name";

    private static final String TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR = "Access Denied";

    @Override
    public InputStream getObject(String account, String container, String source, String process, String objectName) {
        String finalObjectName = null;
        String bucketName = null;

        try {
            if (useAccountAsBucketname) {
                finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
                bucketName = account;
            } else {
                finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
                bucketName = container;
            }

            bucketName = addBucketPrefix(bucketName);
            bucketName = bucketName.toLowerCase();

            S3Object s3Object = getConnection(bucketName).getObject(bucketName, finalObjectName);

            if (s3Object != null) {
                ObjectMetadata metadata = s3Object.getObjectMetadata();
                long contentLength = metadata != null ? metadata.getContentLength() : -1;
                return new SafeS3InputStream(s3Object, contentLength);
            }
        } catch (Exception e) {
            connection = null;
            LOGGER.error(SESSIONID, REGISTRATIONID, "getObject failed for objectName: " + objectName,
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
        return null;
    }

    @Override
    public boolean exists(String account, String container, String source, String process, String objectName) {
        try {
            String finalObjectName = null;
            String bucketName = null;

            if (useAccountAsBucketname) {
                finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
                bucketName = account;
            } else {
                finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
                bucketName = container;
            }

            bucketName = addBucketPrefix(bucketName);
            bucketName = bucketName.toLowerCase();

            return getConnection(bucketName).doesObjectExist(bucketName, finalObjectName);
        } catch (Exception e) {
            connection = null;
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public boolean putObject(String account, final String container, String source, String process, String objectName, InputStream data) {
        try {
            String finalObjectName = null;
            String bucketName = null;

            if (useAccountAsBucketname) {
                finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
                bucketName = account;
            } else {
                finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
                bucketName = container;
            }

            bucketName = addBucketPrefix(bucketName);
            bucketName = bucketName.toLowerCase();

            AmazonS3 connection = getConnection(bucketName);
            if (!doesBucketExists(bucketName)) {
                connection.createBucket(bucketName);
                if (useAccountAsBucketname)
                    existingBuckets.add(bucketName);
            }

            try (InputStream inputStream = data) {
                connection.putObject(bucketName, finalObjectName, inputStream, new ObjectMetadata());
                return true;
            }
        } catch (Exception e) {
            connection = null;
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process,
                                                 String objectName, Map<String, Object> metadata) {
        S3Object s3Object = null;

        try {
            String finalObjectName = null;
            String bucketName = null;

            if (useAccountAsBucketname) {
                finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
                bucketName = account;
            } else {
                finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
                bucketName = container;
            }

            bucketName = addBucketPrefix(bucketName);
            bucketName = bucketName.toLowerCase();

            s3Object = getConnection(bucketName).getObject(bucketName, finalObjectName);
            ObjectMetadata objectMetadata = new ObjectMetadata();

            if (s3Object.getObjectMetadata() != null && s3Object.getObjectMetadata().getUserMetadata() != null) {
                for (Entry<String, String> entry : s3Object.getObjectMetadata().getUserMetadata().entrySet()) {
                    objectMetadata.addUserMetadata(entry.getKey(), entry.getValue());
                }
            }

            for (Entry<String, Object> entry : metadata.entrySet()) {
                objectMetadata.addUserMetadata(entry.getKey(),
                        entry.getValue() != null ? entry.getValue().toString() : null);
            }

            try (InputStream content = s3Object.getObjectContent()) {
                PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, finalObjectName, content, objectMetadata);
                putObjectRequest.getRequestClientOptions().setReadLimit(readlimit);
                getConnection(bucketName).putObject(putObjectRequest);
                return metadata;
            }
        } catch (Exception e) {
            connection = null;
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        } finally {
            if (s3Object != null) {
                try {
                    s3Object.close();
                } catch (IOException e) {
                    LOGGER.error(SESSIONID, REGISTRATIONID, "Error closing S3Object",
                            ExceptionUtils.getStackTrace(e));
                }
            }
        }
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process,
                                                 String objectName, String key, String value) {
        Map<String, Object> meta = new HashMap<>();
        meta.put(key, value);
        String finalObjectName = null;

        if (useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
        } else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
        }

        return addObjectMetaData(account, container, source, process, finalObjectName, meta);
    }

    @Override
    public Map<String, Object> getMetaData(String account, String container, String source, String process,
                                           String objectName) {
        S3Object s3Object = null;

        try {
            String finalObjectName = null;
            String bucketName = null;

            if (useAccountAsBucketname) {
                finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
                bucketName = account;
            } else {
                finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
                bucketName = container;
            }

            bucketName = addBucketPrefix(bucketName);
            bucketName = bucketName.toLowerCase();

            Map<String, Object> metaData = new HashMap<>();

            s3Object = getConnection(bucketName).getObject(bucketName, finalObjectName);
            ObjectMetadata objectMetadata = s3Object.getObjectMetadata();

            if (objectMetadata != null && objectMetadata.getUserMetadata() != null) {
                for (Entry<String, String> entry : objectMetadata.getUserMetadata().entrySet()) {
                    metaData.put(entry.getKey(), entry.getValue());
                }
            }

            return metaData;
        } catch (Exception e) {
            connection = null;
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        } finally {
            if (s3Object != null) {
                try {
                    s3Object.close();
                } catch (IOException e) {
                    LOGGER.error(SESSIONID, REGISTRATIONID, "Error closing S3Object",
                            ExceptionUtils.getStackTrace(e));
                }
            }
        }
    }

    @Override
    public Integer incMetadata(String account, String container, String source, String process, String objectName, String metaDataKey) {
        try {
            Map<String, Object> metadata = getMetaData(account, container, source, process, objectName);
            if (metadata.get(metaDataKey) != null) {
                int newValue = Integer.valueOf(metadata.get(metaDataKey).toString()) + 1;
                metadata.put(metaDataKey, newValue);
                addObjectMetaData(account, container, source, process, objectName, metadata);
                return newValue;
            }
            return null;
        } catch (Exception e) {
            connection = null;
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public Integer decMetadata(String account, String container, String source, String process, String objectName, String metaDataKey) {
        try {
            Map<String, Object> metadata = getMetaData(account, container, source, process, objectName);
            if (metadata.get(metaDataKey) != null) {
                int newValue = Integer.valueOf(metadata.get(metaDataKey).toString()) - 1;
                metadata.put(metaDataKey, newValue);
                addObjectMetaData(account, container, source, process, objectName, metadata);
                return newValue;
            }
            return null;
        } catch (Exception e) {
            connection = null;
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public boolean deleteObject(String account, String container, String source, String process, String objectName) {
        try {
            String finalObjectName = null;
            String bucketName = null;

            if (useAccountAsBucketname) {
                finalObjectName = ObjectStoreUtil.getName(container, source, process, objectName);
                bucketName = account;
            } else {
                finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
                bucketName = container;
            }

            bucketName = addBucketPrefix(bucketName);
            bucketName = bucketName.toLowerCase();

            getConnection(bucketName).deleteObject(bucketName, finalObjectName);
            return true;
        } catch (Exception e) {
            connection = null;
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public boolean removeContainer(String account, String container, String source, String process) {
        return false;
    }

    @Override
    public boolean pack(String account, String container, String source, String process, String refId) {
        return false;
    }

    /**
     * Get or create S3 connection with retry logic
     */
    private AmazonS3 getConnection(String bucketName) {
        if (connection != null) {
            return connection;
        }

        try {
            AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
            ClientConfiguration clientConfig = new ClientConfiguration()
                    .withConnectionTimeout(connectionTimeout)
                    .withSocketTimeout(socketTimeout)
                    .withClientExecutionTimeout(clientExecutionTimeout)
                    .withMaxConnections(maxConnection)
                    .withMaxErrorRetry(maxRetry)
                    .withTcpKeepAlive(true);

            connection = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                    .enablePathStyleAccess()
                    .withClientConfiguration(clientConfig)
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(url, region))
                    .build();

            connection.doesBucketExistV2(bucketName);
            retry = 0;
        } catch (Exception e) {
            if (retry >= maxRetry) {
                retry = 0;
                connection = null;
                throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                        OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
            } else {
                connection = null;
                retry = retry + 1;
                long backoffTime = (long) Math.pow(2, retry) * 100;
                try {
                    Thread.sleep(Math.min(backoffTime, 10000));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                return getConnection(bucketName);
            }
        }
        return connection;
    }

    public List<ObjectDto> getAllObjects(String account, String id) {
        try {
            List<S3ObjectSummary> os = null;
            String bucketName;

            if (useAccountAsBucketname) {
                String searchPattern = id + SEPARATOR;
                bucketName = addBucketPrefix(account);
                bucketName = bucketName.toLowerCase();
                os = getConnection(bucketName).listObjects(bucketName, searchPattern).getObjectSummaries();
            } else {
                bucketName = addBucketPrefix(id);
                bucketName = bucketName.toLowerCase();
                os = getConnection(bucketName).listObjects(bucketName).getObjectSummaries();
            }

            if (os != null && os.size() > 0) {
                List<ObjectDto> objectDtos = new ArrayList<>();

                for (S3ObjectSummary o : os) {
                    String[] tempKeys = o.getKey().split("/");

                    if (useAccountAsBucketname) {
                        if (tempKeys.length > 1 && tempKeys[1] != null && tempKeys[1].endsWith(TAGS_FILENAME))
                            continue;
                    } else {
                        if (tempKeys.length > 0 && tempKeys[0] != null && tempKeys[0].endsWith(TAGS_FILENAME))
                            continue;
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
                }

                return objectDtos;
            }

            return null;
        } catch (Exception e) {
            connection = null;
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    private String[] removeIdFromObjectPath(boolean useAccountAsBucketname, String[] keys) {
        return (useAccountAsBucketname && ArrayUtils.isNotEmpty(keys)) ?
                (String[]) ArrayUtils.remove(keys, 0) : keys;
    }

    @Override
    public Map<String, String> addTags(String account, String container, Map<String, String> tags) {
        String bucketName = null;
        String finalObjectName = null;

        try {
            if (useAccountAsBucketname) {
                bucketName = account;
                finalObjectName = ObjectStoreUtil.getName(container, null, TAGS_FILENAME);
            } else {
                bucketName = container;
                finalObjectName = TAGS_FILENAME;
            }

            bucketName = addBucketPrefix(bucketName);
            bucketName = bucketName.toLowerCase();
            AmazonS3 connection = getConnection(bucketName);

            if (!doesBucketExists(bucketName)) {
                connection.createBucket(bucketName);
                if (useAccountAsBucketname)
                    existingBuckets.add(bucketName);
            }

            for (Entry<String, String> entry : tags.entrySet()) {
                String tagName = ObjectStoreUtil.getName(finalObjectName, entry.getKey());
                InputStream data = IOUtils.toInputStream(entry.getValue(), StandardCharsets.UTF_8);

                try {
                    try (InputStream tagData = data) {
                        connection.putObject(bucketName, tagName, tagData, new ObjectMetadata());
                    }
                } catch (AmazonS3Exception e) {
                    if (e.getMessage().contains(TAG_BACKWARD_COMPATIBILITY_ERROR) ||
                            e.getMessage().contains(TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR)) {
                        if (connection.doesObjectExist(bucketName, finalObjectName)) {
                            connection.deleteObject(bucketName, finalObjectName);
                            addTags(account, container, tags);
                        } else {
                            connection = null;
                            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
                        }
                    } else {
                        connection = null;
                        throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                                OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
                    }
                }
            }
        } catch (Exception e) {
            connection = null;
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
        return tags;
    }

    @Override
    public Map<String, String> getTags(String account, String container) {
        Map<String, String> objectTags = new HashMap<>();

        try {
            String bucketName = null;
            String finalObjectName = null;

            if (useAccountAsBucketname) {
                bucketName = account;
                finalObjectName = ObjectStoreUtil.getName(container, null, TAGS_FILENAME) + SEPARATOR;
            } else {
                bucketName = container;
                finalObjectName = TAGS_FILENAME + SEPARATOR;
            }

            bucketName = addBucketPrefix(bucketName);
            bucketName = bucketName.toLowerCase();
            AmazonS3 connection = getConnection(bucketName);

            List<S3ObjectSummary> objectSummary = null;
            if (useAccountAsBucketname)
                objectSummary = connection.listObjects(bucketName, finalObjectName).getObjectSummaries();
            else
                objectSummary = connection.listObjects(bucketName).getObjectSummaries();

            List<String> tagNames = new ArrayList<>();
            if (objectSummary != null && objectSummary.size() > 0) {
                for (S3ObjectSummary o : objectSummary) {
                    String[] keys = o.getKey().split("/");
                    if (ArrayUtils.isNotEmpty(keys)) {
                        if (useAccountAsBucketname) {
                            if (keys.length > 1 && keys[1] != null && keys[1].endsWith(TAGS_FILENAME))
                                tagNames.add(keys[2]);
                        } else {
                            if (keys.length > 0 && keys[0] != null && keys[0].endsWith(TAGS_FILENAME))
                                tagNames.add(keys[1]);
                        }
                    }
                }
            }

            for (String tagName : tagNames) {
                objectTags.put(tagName, connection.getObjectAsString(bucketName, finalObjectName + tagName));
            }

            return objectTags;
        } catch (Exception e) {
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public void deleteTags(String account, String container, List<String> tags) {
        try {
            String bucketName = null;
            String finalObjectName = null;

            if (useAccountAsBucketname) {
                bucketName = account;
                finalObjectName = ObjectStoreUtil.getName(container, null, TAGS_FILENAME);
            } else {
                bucketName = container;
                finalObjectName = TAGS_FILENAME;
            }

            bucketName = addBucketPrefix(bucketName);
            bucketName = bucketName.toLowerCase();
            AmazonS3 connection = getConnection(bucketName);

            if (!doesBucketExists(bucketName)) {
                connection.createBucket(bucketName);
                if (useAccountAsBucketname)
                    existingBuckets.add(bucketName);
            }

            for (String tag : tags) {
                String tagName = ObjectStoreUtil.getName(finalObjectName, tag);
                connection.deleteObject(bucketName, tagName);
            }
        } catch (Exception e) {
            connection = null;
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    private boolean doesBucketExists(String bucketName) {
        boolean result = false;

        if (useAccountAsBucketname && existingBuckets.contains(bucketName)) {
            result = true;
        } else if (useAccountAsBucketname && !existingBuckets.contains(bucketName)) {
            result = connection.doesBucketExistV2(bucketName);
            if (result)
                existingBuckets.add(bucketName);
        } else {
            result = connection.doesBucketExistV2(bucketName);
        }

        return result;
    }

    private String addBucketPrefix(String bucketName) {
        if (bucketName.startsWith(bucketNamePrefix)) {
            return bucketName;
        } else {
            return bucketNamePrefix + bucketName;
        }
    }
}