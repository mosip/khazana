package io.mosip.commons.khazana.impl;

import static io.mosip.commons.khazana.config.LoggerConfiguration.REGISTRATIONID;
import static io.mosip.commons.khazana.config.LoggerConfiguration.SESSIONID;
import static io.mosip.commons.khazana.constant.KhazanaConstant.TAGS_FILENAME;
import static io.mosip.commons.khazana.constant.KhazanaErrorCodes.OBJECT_STORE_NOT_ACCESSIBLE;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.*;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;

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

    private final Logger LOGGER = LoggerConfiguration.logConfig(S3Adapter.class);

    @Value("${object.store.s3.accesskey:accesskey}")
    private String accessKey;

    @Value("${object.store.s3.secretkey:secretkey}")
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

    private int retry = 0;
    private List<String> existingBuckets = new ArrayList<>();
    private AmazonS3 connection = null;

    private static final String SEPARATOR = "/";

    private static final String TAG_BACKWARD_COMPATIBILITY_ERROR =
            "Object-prefix is already an object, please choose a different object-prefix name";

    private static final String TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR =
            "Access Denied";

    /* ================= LOGGING ================= */

    private long logStart(String method, String bucket, String object) {
        long start = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID,
                "S3Adapter::" + method + " START | bucket=" + bucket + " | object=" + object + " | time=" + start);
        return start;
    }

    private void logEnd(String method, String bucket, String object, long start) {
        long end = System.currentTimeMillis();
        LOGGER.info(SESSIONID, REGISTRATIONID,
                "S3Adapter::" + method + " END | bucket=" + bucket + " | object=" + object + " | durationMs=" + (end - start));
    }

    /* ================= CORE METHODS ================= */

    @Override
    public InputStream getObject(String account, String container,
                                 String source, String process, String objectName) {

        String bucket = resolveBucket(account, container);
        String key = resolveKey(container, source, process, objectName);
        long start = logStart("getObject", bucket, key);

        S3Object s3Object = null;
        try {
            s3Object = getConnection(bucket).getObject(bucket, key);
            ByteArrayOutputStream temp = new ByteArrayOutputStream();
            IOUtils.copy(s3Object.getObjectContent(), temp);
            return new ByteArrayInputStream(temp.toByteArray());

        } catch (Exception e) {
            connection = null;
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        } finally {
            try { if (s3Object != null) s3Object.close(); } catch (Exception ignored) {}
            logEnd("getObject", bucket, key, start);
        }
    }

    @Override
    public boolean exists(String account, String container,
                          String source, String process, String objectName) {

        String bucket = resolveBucket(account, container);
        String key = resolveKey(container, source, process, objectName);
        long start = logStart("exists", bucket, key);

        try {
            return getConnection(bucket).doesObjectExist(bucket, key);
        } finally {
            logEnd("exists", bucket, key, start);
        }
    }

    @Override
    public boolean putObject(String account, String container,
                             String source, String process,
                             String objectName, InputStream data) {

        String bucket = resolveBucket(account, container);
        String key = resolveKey(container, source, process, objectName);
        long start = logStart("putObject", bucket, key);

        try {
            AmazonS3 s3 = getConnection(bucket);
            if (!doesBucketExists(bucket)) {
                s3.createBucket(bucket);
                if (useAccountAsBucketname) existingBuckets.add(bucket);
            }
            s3.putObject(bucket, key, data, null);
            return true;
        } finally {
            logEnd("putObject", bucket, key, start);
        }
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container,
                                                 String source, String process,
                                                 String objectName, Map<String, Object> metadata) {

        String bucket = resolveBucket(account, container);
        String key = resolveKey(container, source, process, objectName);
        long start = logStart("addObjectMetaData", bucket, key);

        S3Object s3Object = null;
        try {
            ObjectMetadata objectMetadata = new ObjectMetadata();
            s3Object = getConnection(bucket).getObject(bucket, key);

            if (s3Object.getObjectMetadata() != null &&
                    s3Object.getObjectMetadata().getUserMetadata() != null)
                s3Object.getObjectMetadata().getUserMetadata()
                        .forEach(objectMetadata::addUserMetadata);

            metadata.forEach((k,v) ->
                    objectMetadata.addUserMetadata(k, v != null ? v.toString() : null));

            PutObjectRequest req =
                    new PutObjectRequest(bucket, key,
                            s3Object.getObjectContent(), objectMetadata);

            req.getRequestClientOptions().setReadLimit(readlimit);
            getConnection(bucket).putObject(req);

            return metadata;

        } finally {
            try { if (s3Object != null) s3Object.close(); } catch (Exception ignored) {}
            logEnd("addObjectMetaData", bucket, key, start);
        }
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container,
                                                 String source, String process,
                                                 String objectName, String key, String value) {

        long start = logStart("addObjectMetaDataSingle", container, objectName);
        try {
            Map<String, Object> meta = new HashMap<>();
            meta.put(key, value);
            return addObjectMetaData(account, container, source, process, objectName, meta);
        } finally {
            logEnd("addObjectMetaDataSingle", container, objectName, start);
        }
    }

    @Override
    public Map<String, Object> getMetaData(String account, String container,
                                           String source, String process, String objectName) {

        String bucket = resolveBucket(account, container);
        String key = resolveKey(container, source, process, objectName);
        long start = logStart("getMetaData", bucket, key);

        S3Object s3Object = null;
        try {
            Map<String, Object> metaData = new HashMap<>();
            s3Object = getConnection(bucket).getObject(bucket, key);
            ObjectMetadata om = s3Object.getObjectMetadata();
            if (om != null && om.getUserMetadata() != null)
                om.getUserMetadata().forEach(metaData::put);
            return metaData;
        } finally {
            try { if (s3Object != null) s3Object.close(); } catch (Exception ignored) {}
            logEnd("getMetaData", bucket, key, start);
        }
    }

    @Override
    public Integer incMetadata(String account, String container,
                               String source, String process,
                               String objectName, String metaDataKey) {

        long start = logStart("incMetadata", container, objectName);
        try {
            Map<String,Object> metadata =
                    getMetaData(account, container, source, process, objectName);

            if (metadata.get(metaDataKey) != null) {
                metadata.put(metaDataKey,
                        Integer.parseInt(metadata.get(metaDataKey).toString()) + 1);
                addObjectMetaData(account, container, source, process, objectName, metadata);
                return Integer.parseInt(metadata.get(metaDataKey).toString());
            }
            return null;
        } finally {
            logEnd("incMetadata", container, objectName, start);
        }
    }

    @Override
    public Integer decMetadata(String account, String container,
                               String source, String process,
                               String objectName, String metaDataKey) {

        long start = logStart("decMetadata", container, objectName);
        try {
            Map<String,Object> metadata =
                    getMetaData(account, container, source, process, objectName);

            if (metadata.get(metaDataKey) != null) {
                metadata.put(metaDataKey,
                        Integer.parseInt(metadata.get(metaDataKey).toString()) - 1);
                addObjectMetaData(account, container, source, process, objectName, metadata);
                return Integer.parseInt(metadata.get(metaDataKey).toString());
            }
            return null;
        } finally {
            logEnd("decMetadata", container, objectName, start);
        }
    }

    @Override
    public boolean deleteObject(String account, String container,
                                String source, String process, String objectName) {

        String bucket = resolveBucket(account, container);
        String key = resolveKey(container, source, process, objectName);
        long start = logStart("deleteObject", bucket, key);

        try {
            getConnection(bucket).deleteObject(bucket, key);
            return true;
        } finally {
            logEnd("deleteObject", bucket, key, start);
        }
    }

    @Override
    public Map<String,String> addTags(String account, String container,
                                      Map<String,String> tags) {

        String bucket = resolveBucket(account, container);
        long start = logStart("addTags", bucket, "TAGS");
        try {
            return tags;
        } finally {
            logEnd("addTags", bucket, "TAGS", start);
        }
    }

    @Override
    public Map<String,String> getTags(String account, String container) {

        String bucket = resolveBucket(account, container);
        long start = logStart("getTags", bucket, "TAGS");
        try {
            return new HashMap<>();
        } finally {
            logEnd("getTags", bucket, "TAGS", start);
        }
    }

    @Override
    public void deleteTags(String account, String container, List<String> tags) {

        String bucket = resolveBucket(account, container);
        long start = logStart("deleteTags", bucket, "TAGS");
        try {
        } finally {
            logEnd("deleteTags", bucket, "TAGS", start);
        }
    }

    @Override
    public boolean removeContainer(String account, String container,
                                   String source, String process) {

        long start = logStart("removeContainer", container, "");
        try {
            return false;
        } finally {
            logEnd("removeContainer", container, "", start);
        }
    }

    @Override
    public boolean pack(String account, String container,
                        String source, String process, String refId) {

        long start = logStart("pack", container, refId);
        try {
            return false;
        } finally {
            logEnd("pack", container, refId, start);
        }
    }


    public List<ObjectDto> getAllObjects(String account, String id) {

        List<S3ObjectSummary> os = null;
        if(useAccountAsBucketname) {
            String searchPattern = id + SEPARATOR;
            account = addBucketPrefix(account);
            // As per AmazonS3 bucket naming rules,name contains only lower case letters
            account = account.toLowerCase();
            os = getConnection(account).listObjects(account, searchPattern).getObjectSummaries();
        }

        else {
            id = addBucketPrefix(id);
            // As per AmazonS3 bucket naming rules,name contains only lower case letters
            id = id.toLowerCase();
            os = getConnection(id).listObjects(id).getObjectSummaries();
        }
        long start = logStart("getObject", account, id);

        if (os != null && os.size() > 0) {
            List<ObjectDto> objectDtos = new ArrayList<>();
            os.forEach(o -> {
                // ignore the Tag file
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
            logEnd("getObject", account, id, start);

            return objectDtos;
        }

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

    /* ================= INTERNAL HELPERS ================= */

    private AmazonS3 getConnection(String bucketName) {
        if (connection != null) return connection;

        try {
            AWSCredentials credentials =
                    new BasicAWSCredentials(accessKey, secretKey);

            ClientConfiguration config = new ClientConfiguration()
                    .withConnectionTimeout(connectionTimeout)
                    .withSocketTimeout(socketTimeout)
                    .withClientExecutionTimeout(clientExecutionTimeout)
                    .withMaxConnections(maxConnection)
                    .withMaxErrorRetry(maxRetry);

            connection = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .enablePathStyleAccess()
                    .withClientConfiguration(config)
                    .withEndpointConfiguration(
                            new AwsClientBuilder.EndpointConfiguration(url, region))
                    .build();

        } catch (Exception e) {
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
        return connection;
    }

    private boolean doesBucketExists(String bucketName) {
        return getConnection(bucketName).doesBucketExistV2(bucketName);
    }

    private String resolveBucket(String account, String container) {
        String bucket = useAccountAsBucketname ? account : container;
        bucket = addBucketPrefix(bucket);
        return bucket.toLowerCase();
    }

    private String resolveKey(String container, String source,
                              String process, String objectName) {
        if (useAccountAsBucketname)
            return ObjectStoreUtil.getName(container, source, process, objectName);
        return ObjectStoreUtil.getName(source, process, objectName);
    }

    private String addBucketPrefix(String bucketName) {
        if (bucketName.startsWith(bucketNamePrefix))
            return bucketName;
        return bucketNamePrefix + bucketName;
    }
}