package io.mosip.commons.khazana.impl;

import static io.mosip.commons.khazana.config.LoggerConfiguration.REGISTRATIONID;
import static io.mosip.commons.khazana.config.LoggerConfiguration.SESSIONID;
import static io.mosip.commons.khazana.constant.KhazanaConstant.TAGS_FILENAME;
import static io.mosip.commons.khazana.constant.KhazanaErrorCodes.OBJECT_STORE_NOT_ACCESSIBLE;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
 *
 * Key improvements:
 * - Proper try-with-resources for stream management
 * - Content-Length validation to prevent memory buffering issues
 * - Explicit stream closing to prevent "Not all bytes were read" warnings
 * - Efficient stream-based operations
 * - Connection pooling and reuse optimization
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
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "getObject - method started for account: " + account + ", container: " + container + ", source: " + source + ", process: " + process + ", objectName: " + objectName);

        String finalObjectName=null;
        String bucketName=null;
        if(useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container,source, process, objectName);
            bucketName=account;
        }else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName=container;
        }

        bucketName = addBucketPrefix(bucketName);
        bucketName = bucketName.toLowerCase();
        S3Object s3Object = null;
        try {
            s3Object = getConnection(bucketName).getObject(bucketName, finalObjectName);
            if (s3Object != null) {
                // IMPORTANT: Get content length to prevent "Not all bytes were read" warning
                ObjectMetadata metadata = s3Object.getObjectMetadata();
                long contentLength = metadata != null ? metadata.getContentLength() : -1;

                LOGGER.info(SESSIONID, REGISTRATIONID, "getObject - retrieved object with contentLength: " + contentLength + " bytes for objectName: " + objectName);

                // Return wrapper stream that ensures proper cleanup
                return new SafeS3InputStream(s3Object, contentLength);
            }
        } catch (Exception e) {
            connection = null;
            if (s3Object != null) {
                try {
                    s3Object.close();
                } catch (IOException ioe) {
                    LOGGER.error(SESSIONID, REGISTRATIONID, "Error closing S3Object on exception", ExceptionUtils.getStackTrace(ioe));
                }
            }
            long endTime = System.currentTimeMillis();
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured in getObject for : " + container + " after " + (endTime - startTime) + "ms", ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
        long endTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "getObject - method completed in " + (endTime - startTime) + "ms for objectName: " + objectName);
        return null;
    }

    @Override
    public boolean exists(String account, String container, String source, String process, String objectName) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "exists - method started for account: " + account + ", container: " + container + ", source: " + source + ", process: " + process + ", objectName: " + objectName);

        String finalObjectName=null;
        String bucketName=null;
        if(useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container,source, process, objectName);
            bucketName=account;
        }else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName=container;
        }
        bucketName = addBucketPrefix(bucketName);
        bucketName = bucketName.toLowerCase();

        try {
            boolean result = getConnection(bucketName).doesObjectExist(bucketName,finalObjectName);
            long endTime = System.currentTimeMillis();
            LOGGER.info(SESSIONID, REGISTRATIONID, "exists - method completed in " + (endTime - startTime) + "ms, objectExists: " + result + " for objectName: " + objectName);
            return result;
        } catch (Exception e) {
            connection = null;
            long endTime = System.currentTimeMillis();
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured in exists method for : " + container + " after " + (endTime - startTime) + "ms", ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public boolean putObject(String account, final String container, String source, String process, String objectName, InputStream data) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "putObject - method started for account: " + account + ", container: " + container + ", source: " + source + ", process: " + process + ", objectName: " + objectName);

        String finalObjectName=null;
        String bucketName=null;
        if(useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container,source, process, objectName);
            bucketName=account;
        }else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName=container;
        }
        bucketName = addBucketPrefix(bucketName);
        bucketName = bucketName.toLowerCase();

        try {
            AmazonS3 connection = getConnection(bucketName);
            if (!doesBucketExists(bucketName)) {
                LOGGER.info(SESSIONID, REGISTRATIONID, "putObject - creating bucket: " + bucketName);
                connection.createBucket(bucketName);
                if (useAccountAsBucketname)
                    existingBuckets.add(bucketName);
            }

            // Use try-with-resources to ensure stream is properly closed
            try (InputStream inputStream = data) {
                connection.putObject(bucketName, finalObjectName, inputStream, new ObjectMetadata());
                long endTime = System.currentTimeMillis();
                LOGGER.info(SESSIONID, REGISTRATIONID, "putObject - method completed successfully in " + (endTime - startTime) + "ms for objectName: " + objectName);
                return true;
            }
        } catch (Exception e) {
            connection = null;
            long endTime = System.currentTimeMillis();
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured in putObject for : " + container + " after " + (endTime - startTime) + "ms", ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process,
                                                 String objectName, Map<String, Object> metadata) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "addObjectMetaData - method started for account: " + account + ", container: " + container + ", source: " + source + ", process: " + process + ", objectName: " + objectName + ", metadata keys: " + (metadata != null ? metadata.keySet() : "null"));

        S3Object s3Object = null;
        try {
            String finalObjectName=null;
            String bucketName=null;
            if(useAccountAsBucketname) {
                finalObjectName = ObjectStoreUtil.getName(container,source, process, objectName);
                bucketName=account;
            }else {
                finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
                bucketName=container;
            }
            bucketName = addBucketPrefix(bucketName);
            bucketName = bucketName.toLowerCase();

            s3Object = getConnection(bucketName).getObject(bucketName, finalObjectName);
            ObjectMetadata objectMetadata = new ObjectMetadata();

            if (s3Object.getObjectMetadata() != null && s3Object.getObjectMetadata().getUserMetadata() != null)
                s3Object.getObjectMetadata().getUserMetadata().entrySet().forEach(m -> objectMetadata.addUserMetadata(m.getKey(), m.getValue()));

            metadata.entrySet().stream().forEach(m -> objectMetadata.addUserMetadata(m.getKey(), m.getValue() != null ? m.getValue().toString() : null));

            // IMPORTANT: Use try-with-resources to ensure proper stream cleanup
            try (InputStream content = s3Object.getObjectContent()) {
                PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, finalObjectName, content, objectMetadata);
                putObjectRequest.getRequestClientOptions().setReadLimit(readlimit);
                getConnection(bucketName).putObject(putObjectRequest);
                long endTime = System.currentTimeMillis();
                LOGGER.info(SESSIONID, REGISTRATIONID, "addObjectMetaData - method completed successfully in " + (endTime - startTime) + "ms for objectName: " + objectName);
                return metadata;
            }
        } catch (Exception e) {
            connection = null;
            long endTime = System.currentTimeMillis();
            LOGGER.error(SESSIONID, REGISTRATIONID,"Exception occured to addObjectMetaData for : " + container + " after " + (endTime - startTime) + "ms", ExceptionUtils.getStackTrace(e));
            metadata = null;
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        } finally {
            try {
                if (s3Object != null)
                    s3Object.close();
            } catch (IOException e) {
                LOGGER.error(SESSIONID, REGISTRATIONID,"IO occured : " + container, ExceptionUtils.getStackTrace(e));
            }
        }
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process,
                                                 String objectName, String key, String value) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "addObjectMetaData(key-value) - method started for account: " + account + ", container: " + container + ", objectName: " + objectName + ", key: " + key + ", value: " + value);

        Map<String, Object> meta = new HashMap<>();
        meta.put(key, value);
        String finalObjectName=null;

        if(useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container,source, process, objectName);
        }else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
        }

        Map<String, Object> result = addObjectMetaData(account, container, source, process, finalObjectName, meta);
        long endTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "addObjectMetaData(key-value) - method completed in " + (endTime - startTime) + "ms for objectName: " + objectName);
        return result;
    }

    @Override
    public Map<String, Object> getMetaData(String account, String container, String source, String process,
                                           String objectName) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "getMetaData - method started for account: " + account + ", container: " + container + ", source: " + source + ", process: " + process + ", objectName: " + objectName);

        S3Object s3Object = null;
        try {
            String finalObjectName=null;
            String bucketName=null;
            if(useAccountAsBucketname) {
                finalObjectName = ObjectStoreUtil.getName(container,source, process, objectName);
                bucketName=account;
            }else {
                finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
                bucketName=container;
            }
            bucketName = addBucketPrefix(bucketName);
            bucketName = bucketName.toLowerCase();
            Map<String, Object> metaData = new HashMap<>();

            s3Object = getConnection(bucketName).getObject(bucketName, finalObjectName);
            ObjectMetadata objectMetadata = s3Object.getObjectMetadata();
            if (objectMetadata != null && objectMetadata.getUserMetadata() != null)
                objectMetadata.getUserMetadata().entrySet().forEach(entry -> metaData.put(entry.getKey(), entry.getValue()));

            long endTime = System.currentTimeMillis();
            LOGGER.info(SESSIONID, REGISTRATIONID, "getMetaData - method completed successfully in " + (endTime - startTime) + "ms, metadata keys: " + metaData.keySet() + " for objectName: " + objectName);
            return metaData;
        } catch (Exception e) {
            connection = null;
            long endTime = System.currentTimeMillis();
            LOGGER.error(SESSIONID, REGISTRATIONID,"Exception occured to getMetaData for : " + container + " after " + (endTime - startTime) + "ms", ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        } finally {
            try {
                if (s3Object != null)
                    s3Object.close();
            } catch (IOException e) {
                LOGGER.error(SESSIONID, REGISTRATIONID,"IO occured : " + container, ExceptionUtils.getStackTrace(e));
            }
        }
    }

    @Override
    public Integer incMetadata(String account, String container, String source, String process, String objectName, String metaDataKey) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "incMetadata - method started for account: " + account + ", container: " + container + ", objectName: " + objectName + ", metaDataKey: " + metaDataKey);

        try {
            Map<String, Object> metadata = getMetaData(account, container, source, process, objectName);
            if (metadata.get(metaDataKey) != null) {
                metadata.put(metaDataKey, Integer.valueOf(metadata.get(metaDataKey).toString()) + 1);
                addObjectMetaData(account, container, source, process, objectName, metadata);
                long endTime = System.currentTimeMillis();
                int result = Integer.valueOf(metadata.get(metaDataKey).toString());
                LOGGER.info(SESSIONID, REGISTRATIONID, "incMetadata - method completed successfully in " + (endTime - startTime) + "ms, new value: " + result + " for objectName: " + objectName);
                return result;
            }
            long endTime = System.currentTimeMillis();
            LOGGER.info(SESSIONID, REGISTRATIONID, "incMetadata - metaDataKey not found after " + (endTime - startTime) + "ms for objectName: " + objectName);
            return null;
        } catch (Exception e) {
            connection = null;
            long endTime = System.currentTimeMillis();
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured in incMetadata after " + (endTime - startTime) + "ms", ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public Integer decMetadata(String account, String container, String source, String process, String objectName, String metaDataKey) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "decMetadata - method started for account: " + account + ", container: " + container + ", objectName: " + objectName + ", metaDataKey: " + metaDataKey);

        try {
            Map<String, Object> metadata = getMetaData(account, container, source, process, objectName);
            if (metadata.get(metaDataKey) != null) {
                metadata.put(metaDataKey, Integer.valueOf(metadata.get(metaDataKey).toString()) - 1);
                addObjectMetaData(account, container, source, process, objectName, metadata);
                long endTime = System.currentTimeMillis();
                int result = Integer.valueOf(metadata.get(metaDataKey).toString());
                LOGGER.info(SESSIONID, REGISTRATIONID, "decMetadata - method completed successfully in " + (endTime - startTime) + "ms, new value: " + result + " for objectName: " + objectName);
                return result;
            }
            long endTime = System.currentTimeMillis();
            LOGGER.info(SESSIONID, REGISTRATIONID, "decMetadata - metaDataKey not found after " + (endTime - startTime) + "ms for objectName: " + objectName);
            return null;
        } catch (Exception e) {
            connection = null;
            long endTime = System.currentTimeMillis();
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured in decMetadata after " + (endTime - startTime) + "ms", ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public boolean deleteObject(String account, String container, String source, String process, String objectName) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "deleteObject - method started for account: " + account + ", container: " + container + ", source: " + source + ", process: " + process + ", objectName: " + objectName);

        String finalObjectName=null;
        String bucketName=null;
        if(useAccountAsBucketname) {
            finalObjectName = ObjectStoreUtil.getName(container,source, process, objectName);
            bucketName=account;
        }else {
            finalObjectName = ObjectStoreUtil.getName(source, process, objectName);
            bucketName=container;
        }
        bucketName = addBucketPrefix(bucketName);
        bucketName = bucketName.toLowerCase();

        try {
            getConnection(bucketName).deleteObject(bucketName, finalObjectName);
            long endTime = System.currentTimeMillis();
            LOGGER.info(SESSIONID, REGISTRATIONID, "deleteObject - method completed successfully in " + (endTime - startTime) + "ms for objectName: " + objectName);
            return true;
        } catch (Exception e) {
            connection = null;
            long endTime = System.currentTimeMillis();
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured in deleteObject after " + (endTime - startTime) + "ms", ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public boolean removeContainer(String account, String container, String source, String process) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "removeContainer - method started for account: " + account + ", container: " + container + ", source: " + source + ", process: " + process);
        long endTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "removeContainer - method not supported, completed in " + (endTime - startTime) + "ms");
        return false;
    }

    @Override
    public boolean pack(String account, String container, String source, String process, String refId) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "pack - method started for account: " + account + ", container: " + container + ", source: " + source + ", process: " + process + ", refId: " + refId);
        long endTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "pack - method not supported, completed in " + (endTime - startTime) + "ms");
        return false;
    }

    /**
     * This method will return a singleton connection. It will verify connection for the first time and will reuse same connection in subsequent calls.
     *
     * @param bucketName
     * @return
     */
    private AmazonS3 getConnection(String bucketName) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "getConnection - method started for bucketName: " + bucketName);

        if (connection != null) {
            long endTime = System.currentTimeMillis();
            LOGGER.info(SESSIONID, REGISTRATIONID, "getConnection - reusing existing connection in " + (endTime - startTime) + "ms for bucketName: " + bucketName);
            return connection;
        }

        try {
            AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
            ClientConfiguration clientConfig = new ClientConfiguration()
                    .withConnectionTimeout(connectionTimeout)
                    .withSocketTimeout(socketTimeout)
                    .withClientExecutionTimeout(clientExecutionTimeout)
                    .withMaxConnections(maxConnection)
                    .withMaxErrorRetry(maxRetry);

            connection = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                    .enablePathStyleAccess()
                    .withClientConfiguration(clientConfig)
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(url, region))
                    .build();

            connection.doesBucketExistV2(bucketName);
            retry = 0;
            long endTime = System.currentTimeMillis();
            LOGGER.info(SESSIONID, REGISTRATIONID, "getConnection - new connection established successfully in " + (endTime - startTime) + "ms for bucketName: " + bucketName);
        } catch (Exception e) {
            if (retry >= maxRetry) {
                retry = 0;
                connection = null;
                long endTime = System.currentTimeMillis();
                LOGGER.error(SESSIONID, REGISTRATIONID,"Maximum retry limit exceeded. Could not obtain connection for "+ bucketName +". Retry count :" + retry + " after " + (endTime - startTime) + "ms", ExceptionUtils.getStackTrace(e));
                throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
            } else {
                connection = null;
                retry = retry + 1;
                long endTime = System.currentTimeMillis();
                LOGGER.error(SESSIONID, REGISTRATIONID,"Exception occured while obtaining connection for "+ bucketName +". Will try again. Retry count : " + retry + " after " + (endTime - startTime) + "ms", ExceptionUtils.getStackTrace(e));
                getConnection(bucketName);
            }
        }
        return connection;
    }

    public List<ObjectDto> getAllObjects(String account, String id) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "getAllObjects - method started for account: " + account + ", id: " + id);

        List<S3ObjectSummary> os = null;
        if(useAccountAsBucketname) {
            String searchPattern = id + SEPARATOR;
            account = addBucketPrefix(account);
            account = account.toLowerCase();
            os = getConnection(account).listObjects(account, searchPattern).getObjectSummaries();
        }
        else {
            id = addBucketPrefix(id);
            id = id.toLowerCase();
            os = getConnection(id).listObjects(id).getObjectSummaries();
        }

        if (os != null && os.size() > 0) {
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
            long endTime = System.currentTimeMillis();
            LOGGER.info(SESSIONID, REGISTRATIONID, "getAllObjects - method completed successfully in " + (endTime - startTime) + "ms, found " + objectDtos.size() + " objects for account: " + account);
            return objectDtos;
        }

        long endTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "getAllObjects - method completed in " + (endTime - startTime) + "ms, no objects found for account: " + account);
        return null;
    }

    /**
     * If account is used as bucket name then first element of array is the packet id.
     * This method removes packet id from array so that path is same irrespective of useAccountAsBucketname is true or false
     *
     * @param useAccountAsBucketname
     * @param keys
     */
    private String[] removeIdFromObjectPath(boolean useAccountAsBucketname, String[] keys) {
        return (useAccountAsBucketname && ArrayUtils.isNotEmpty(keys)) ?
                (String[]) ArrayUtils.remove(keys, 0) : keys;
    }

    @Override
    public Map<String, String> addTags(String account, String container, Map<String, String> tags) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "addTags - method started for account: " + account + ", container: " + container + ", tags: " + (tags != null ? tags.keySet() : "null"));

        String bucketName=null;
        String finalObjectName=null;
        try {
            if(useAccountAsBucketname) {
                bucketName=account;
                finalObjectName = ObjectStoreUtil.getName(container,null,TAGS_FILENAME);
            }else {
                bucketName=container;
                finalObjectName = TAGS_FILENAME;
            }
            bucketName = addBucketPrefix(bucketName);
            bucketName = bucketName.toLowerCase();
            AmazonS3 connection = getConnection(bucketName);
            if (!doesBucketExists(bucketName)) {
                LOGGER.info(SESSIONID, REGISTRATIONID, "addTags - creating bucket: " + bucketName);
                connection.createBucket(bucketName);
                if (useAccountAsBucketname)
                    existingBuckets.add(bucketName);
            }
            for(Entry<String, String> entry:tags.entrySet()) {
                String tagName=null;
                InputStream data = IOUtils.toInputStream(entry.getValue(), StandardCharsets.UTF_8);
                tagName = ObjectStoreUtil.getName(finalObjectName, entry.getKey());
                try {
                    LOGGER.info(SESSIONID, REGISTRATIONID, "addTags - adding tag: " + entry.getKey() + " for container: " + container);
                    // Use try-with-resources to ensure stream closure
                    try (InputStream tagData = data) {
                        connection.putObject(bucketName, tagName, tagData, new ObjectMetadata());
                    }
                } catch (Exception e) {
                    if (e instanceof AmazonS3Exception && (e.getMessage().contains(TAG_BACKWARD_COMPATIBILITY_ERROR)
                            || e.getMessage().contains(TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR))) {
                        LOGGER.info(SESSIONID, REGISTRATIONID, "addTags - backward compatibility error detected, attempting recovery for tag: " + entry.getKey());
                        if (connection.doesObjectExist(bucketName, finalObjectName)) {
                            connection.deleteObject(bucketName, finalObjectName);
                            LOGGER.info(SESSIONID, REGISTRATIONID, "addTags - deleted existing object and retrying");
                            addTags(account, container, tags);
                        } else {
                            connection = null;
                            long endTime = System.currentTimeMillis();
                            LOGGER.error(SESSIONID, REGISTRATIONID,
                                    "Exception occured while addTags for : " + container + " after " + (endTime - startTime) + "ms",
                                    ExceptionUtils.getStackTrace(e));
                            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
                        }
                    }else {
                        connection = null;
                        long endTime = System.currentTimeMillis();
                        LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured while addTags for : " + container + " after " + (endTime - startTime) + "ms",
                                ExceptionUtils.getStackTrace(e));
                        throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                                OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
                    }
                }
            }
            long endTime = System.currentTimeMillis();
            LOGGER.info(SESSIONID, REGISTRATIONID, "addTags - method completed successfully in " + (endTime - startTime) + "ms for container: " + container);

        } catch (Exception e) {
            connection = null;
            long endTime = System.currentTimeMillis();
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured while addTags for : " + container + " after " + (endTime - startTime) + "ms",
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
        return tags;
    }

    @Override
    public Map<String, String> getTags(String account, String container) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "getTags - method started for account: " + account + ", container: " + container);

        Map<String, String> objectTags = new HashMap<String, String>();
        try {

            String bucketName=null;
            String finalObjectName=null;
            if (useAccountAsBucketname) {
                bucketName=account;
                finalObjectName = ObjectStoreUtil.getName(container, null, TAGS_FILENAME) + SEPARATOR;
            }else {
                bucketName=container;
                finalObjectName = TAGS_FILENAME + SEPARATOR;
            }
            bucketName = addBucketPrefix(bucketName);
            bucketName = bucketName.toLowerCase();
            AmazonS3 connection = getConnection(bucketName);

            List<S3ObjectSummary> objectSummary = null;
            if(useAccountAsBucketname)
                objectSummary = connection.listObjects(bucketName, finalObjectName).getObjectSummaries();
            else
                objectSummary = connection.listObjects(bucketName).getObjectSummaries();

            List<String> tagNames=new ArrayList<String>();
            if (objectSummary != null && objectSummary.size() > 0) {
                LOGGER.info(SESSIONID, REGISTRATIONID, "getTags - found " + objectSummary.size() + " tag objects");

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
            LOGGER.info(SESSIONID, REGISTRATIONID, "getTags - processing " + tagNames.size() + " tags");
            for(String tagName:tagNames) {
                objectTags.put(tagName, connection.getObjectAsString(bucketName, finalObjectName+tagName));
            }

            long endTime = System.currentTimeMillis();
            LOGGER.info(SESSIONID, REGISTRATIONID, "getTags - method completed successfully in " + (endTime - startTime) + "ms, retrieved " + objectTags.size() + " tags for container: " + container);

            return objectTags;

        }catch(Exception e){
            long endTime = System.currentTimeMillis();
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured while getTags for : " + container + " after " + (endTime - startTime) + "ms",
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }

    }

    @Override
    public void deleteTags(String account, String container, List<String> tags) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "deleteTags - method started for account: " + account + ", container: " + container + ", tags: " + (tags != null ? tags.size() : 0));

        try {
            String bucketName=null;
            String finalObjectName=null;
            if(useAccountAsBucketname) {
                bucketName=account;
                finalObjectName = ObjectStoreUtil.getName(container,null,TAGS_FILENAME);
            }else {
                bucketName=container;
                finalObjectName = TAGS_FILENAME;
            }
            bucketName = addBucketPrefix(bucketName);
            bucketName = bucketName.toLowerCase();
            AmazonS3 connection = getConnection(container);
            if (!doesBucketExists(container)) {
                LOGGER.info(SESSIONID, REGISTRATIONID, "deleteTags - creating bucket: " + container);
                connection.createBucket(container);
                if (useAccountAsBucketname)
                    existingBuckets.add(bucketName);
            }
            for(String tag:tags) {
                String tagName=null;
                tagName=ObjectStoreUtil.getName(finalObjectName, tag);
                LOGGER.info(SESSIONID, REGISTRATIONID, "deleteTags - deleting tag: " + tag + " for container: " + container);
                connection.deleteObject(bucketName, tagName);
            }
            long endTime = System.currentTimeMillis();
            LOGGER.info(SESSIONID, REGISTRATIONID, "deleteTags - method completed successfully in " + (endTime - startTime) + "ms, deleted " + tags.size() + " tags for container: " + container);

        } catch (Exception e) {
            connection = null;
            long endTime = System.currentTimeMillis();
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured while deleteTags for : " + container + " after " + (endTime - startTime) + "ms",
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }

    }

    private boolean doesBucketExists(String bucketName) {
        long startTime = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID, "doesBucketExists - method started for bucketName: " + bucketName);

        boolean result = false;
        if (useAccountAsBucketname && existingBuckets.contains(bucketName)) {
            result = true;
            long endTime = System.currentTimeMillis();
            LOGGER.info(SESSIONID, REGISTRATIONID, "doesBucketExists - bucket found in cache in " + (endTime - startTime) + "ms for bucketName: " + bucketName);
            return result;
        }
        else if (useAccountAsBucketname && !existingBuckets.contains(bucketName)) {
            boolean doesBucketExistsInObjectStore = connection.doesBucketExistV2(bucketName);
            if (doesBucketExistsInObjectStore) {
                existingBuckets.add(bucketName);
                result = true;
            }
            long endTime = System.currentTimeMillis();
            LOGGER.info(SESSIONID, REGISTRATIONID, "doesBucketExists - checked object store in " + (endTime - startTime) + "ms, bucketExists: " + result + " for bucketName: " + bucketName);
            return result;
        } else {
            result = connection.doesBucketExistV2(bucketName);
            long endTime = System.currentTimeMillis();
            LOGGER.info(SESSIONID, REGISTRATIONID, "doesBucketExists - method completed in " + (endTime - startTime) + "ms, bucketExists: " + result + " for bucketName: " + bucketName);
            return result;
        }
    }

    private String addBucketPrefix(String bucketName) {
        long startTime = System.currentTimeMillis();
        LOGGER.debug(SESSIONID, REGISTRATIONID, "addBucketPrefix - method started for bucketName: " + bucketName);

        if (bucketName.startsWith(bucketNamePrefix)) {
            long endTime = System.currentTimeMillis();
            LOGGER.debug(SESSIONID, REGISTRATIONID, "addBucketPrefix - prefix already present in " + (endTime - startTime) + "ms, bucketName: " + bucketName);
            return bucketName;
        } else {
            bucketName = bucketNamePrefix + bucketName;
            long endTime = System.currentTimeMillis();
            LOGGER.debug(SESSIONID, REGISTRATIONID, "addBucketPrefix - prefix added in " + (endTime - startTime) + "ms, bucketName: " + bucketName);
            return bucketName;
        }
    }
}