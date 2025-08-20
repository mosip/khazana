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
import java.util.*;
import java.util.Map.Entry;
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
    private int readlimit; // kept for compatibility (not required with server-side copy)

    @Value("${object.store.connection.max.retry:5}")
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

    /** Hard ceiling to avoid heap blowups; you said 10 MB max payloads. */
    @Value("${object.store.max.object.size.bytes:10485760}") // 10 * 1024 * 1024
    private long maxObjectSizeBytes;

    private volatile AmazonS3 connection = null;
    private final List<String> existingBuckets = Collections.synchronizedList(new ArrayList<>());
    private int retry = 0;

    private static final String SEPARATOR = "/";
    private static final String TAG_BACKWARD_COMPATIBILITY_ERROR = "Object-prefix is already an object, please choose a different object-prefix name";
    private static final String TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR = "Access Denied";

    /* ======================= Core Ops ======================= */

    @Override
    public InputStream getObject(String account, String container, String source, String process, String objectName) {
        String bucketName = resolveBucket(account, container);
        String key = buildKey(account, container, source, process, objectName);
        try {
            S3Object s3Object = getConnection(bucketName).getObject(bucketName, key);
            if (s3Object != null) {
                // Return streaming content; caller MUST close the stream.
                return s3Object.getObjectContent();
            }
            return null;
        } catch (Exception e) {
            invalidateConnection();
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occured to getObject for : " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public boolean exists(String account, String container, String source, String process, String objectName) {
        String bucketName = resolveBucket(account, container);
        String key = buildKey(account, container, source, process, objectName);
        try {
            return getConnection(bucketName).doesObjectExist(bucketName, key);
        } catch (Exception e) {
            invalidateConnection();
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occured to exists for : " + container, ExceptionUtils.getStackTrace(e));
            return false;
        }
    }

    @Override
    public boolean putObject(String account, final String container, String source, String process, String objectName, InputStream data) {
        String bucketName = resolveBucket(account, container);
        String key = buildKey(account, container, source, process, objectName);
        try {
            AmazonS3 conn = getConnection(bucketName);
            if (!doesBucketExists(bucketName)) {
                conn.createBucket(bucketName);
                if (useAccountAsBucketname) existingBuckets.add(bucketName);
            }

            // Buffer into a capped byte[] so we can set Content-Length and avoid chunked uploads (EOF on MinIO).
            byte[] bytes = toByteArrayCapped(data, maxObjectSizeBytes);
            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(bytes.length);

            conn.putObject(new PutObjectRequest(bucketName, key, new ByteArrayInputStream(bytes), meta));
            return true;
        } catch (Exception e) {
            invalidateConnection();
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occured to putObject for : " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public boolean deleteObject(String account, String container, String source, String process, String objectName) {
        String bucketName = resolveBucket(account, container);
        String key = buildKey(account, container, source, process, objectName);
        try {
            getConnection(bucketName).deleteObject(bucketName, key);
            return true;
        } catch (Exception e) {
            invalidateConnection();
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occured to deleteObject for : " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    /* ======================= Metadata ======================= */

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process,
                                                 String objectName, Map<String, Object> metadata) {
        String bucketName = resolveBucket(account, container);
        String key = buildKey(account, container, source, process, objectName);
        try {
            AmazonS3 conn = getConnection(bucketName);
            // Read current user metadata to merge.
            ObjectMetadata current = conn.getObjectMetadata(bucketName, key);
            Map<String, String> merged = new HashMap<>();
            if (current != null && current.getUserMetadata() != null) {
                merged.putAll(current.getUserMetadata());
            }
            if (metadata != null) {
                for (Map.Entry<String, Object> e : metadata.entrySet()) {
                    merged.put(e.getKey(), e.getValue() != null ? String.valueOf(e.getValue()) : null);
                }
            }
            ObjectMetadata newMeta = new ObjectMetadata();
            newMeta.setUserMetadata(merged);

            CopyObjectRequest copyReq = new CopyObjectRequest(bucketName, key, bucketName, key);
            copyReq.setNewObjectMetadata(newMeta);
            copyReq.setMetadataDirective("REPLACE");
            conn.copyObject(copyReq);

            return metadata;
        } catch (Exception e) {
            invalidateConnection();
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occured to addObjectMetaData for : " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process,
                                                 String objectName, String key, String value) {
        Map<String, Object> meta = new HashMap<>();
        meta.put(key, value);
        String finalObjectName = useAccountAsBucketname
                ? ObjectStoreUtil.getName(container, source, process, objectName)
                : ObjectStoreUtil.getName(source, process, objectName);
        return addObjectMetaData(account, container, source, process, finalObjectName, meta);
    }

    @Override
    public Map<String, Object> getMetaData(String account, String container, String source, String process,
                                           String objectName) {
        String bucketName = resolveBucket(account, container);
        String key = buildKey(account, container, source, process, objectName);
        try {
            ObjectMetadata objectMetadata = getConnection(bucketName).getObjectMetadata(bucketName, key);
            Map<String, Object> metaData = new HashMap<>();
            if (objectMetadata != null && objectMetadata.getUserMetadata() != null) {
                objectMetadata.getUserMetadata().forEach(metaData::put);
            }
            return metaData;
        } catch (Exception e) {
            invalidateConnection();
            LOGGER.error(SESSIONID, REGISTRATIONID,
                    "Exception occured to getMetaData for : " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
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

    /* ======================= Remove/Pack (unsupported) ======================= */

    @Override
    public boolean removeContainer(String account, String container, String source, String process) {
        // Not supported in this adapter (kept same as original)
        return false;
    }

    @Override
    public boolean pack(String account, String container, String source, String process, String refId) {
        // Not supported in this adapter (kept same as original)
        return false;
    }

    /* ======================= Tag APIs (kept, small fixes) ======================= */

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

            AmazonS3 connection = getConnection(bucketName);
            if (!doesBucketExists(bucketName)) {
                connection.createBucket(bucketName);
                if (useAccountAsBucketname) existingBuckets.add(bucketName);
            }

            for (Entry<String, String> entry : tags.entrySet()) {
                String tagName = ObjectStoreUtil.getName(finalObjectName, entry.getKey());
                InputStream data = IOUtils.toInputStream(entry.getValue(), StandardCharsets.UTF_8);
                try {
                    connection.putObject(bucketName, tagName, data, null);
                } catch (Exception e) {
                    if (e instanceof AmazonS3Exception && (e.getMessage().contains(TAG_BACKWARD_COMPATIBILITY_ERROR)
                            || e.getMessage().contains(TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR))) {
                        if (connection.doesObjectExist(bucketName, finalObjectName)) {
                            connection.deleteObject(bucketName, finalObjectName);
                            addTags(account, container, tags);
                        } else {
                            this.connection = null;
                            LOGGER.error(SESSIONID, REGISTRATIONID,
                                    "Exception occured while addTags for : " + container,
                                    ExceptionUtils.getStackTrace(e));
                            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
                        }
                    } else {
                        this.connection = null;
                        LOGGER.error(SESSIONID, REGISTRATIONID,
                                "Exception occured while addTags for : " + container,
                                ExceptionUtils.getStackTrace(e));
                        throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                                OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
                    }
                }
            }
        } catch (Exception e) {
            this.connection = null;
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured while addTags for : " + container,
                    ExceptionUtils.getStackTrace(e));
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

            AmazonS3 connection = getConnection(bucketName);
            List<S3ObjectSummary> objectSummary;
            if (useAccountAsBucketname) {
                objectSummary = connection.listObjects(bucketName, finalObjectName).getObjectSummaries();
            } else {
                objectSummary = connection.listObjects(bucketName).getObjectSummaries();
            }

            List<String> tagNames = new ArrayList<>();
            if (objectSummary != null && !objectSummary.isEmpty()) {
                objectSummary.forEach(o -> {
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
                });
            }

            for (String tagName : tagNames) {
                objectTags.put(tagName, connection.getObjectAsString(bucketName, finalObjectName + tagName));
            }
            return objectTags;

        } catch (Exception e) {
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured while getTags for : " + container,
                    ExceptionUtils.getStackTrace(e));
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

            AmazonS3 connection = getConnection(bucketName);
            if (!doesBucketExists(bucketName)) {
                connection.createBucket(bucketName);
                if (useAccountAsBucketname) existingBuckets.add(bucketName);
            }
            for (String tag : tags) {
                String tagName = ObjectStoreUtil.getName(finalObjectName, tag);
                connection.deleteObject(bucketName, tagName);
            }
        } catch (Exception e) {
            this.connection = null;
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured while deleteTags for : " + container,
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    /* ======================= Listing ======================= */

    @Override
    public List<ObjectDto> getAllObjects(String account, String id) {
        List<S3ObjectSummary> os = null;
        if (useAccountAsBucketname) {
            String searchPattern = id + SEPARATOR;
            account = addBucketPrefix(account).toLowerCase();
            os = getConnection(account).listObjects(account, searchPattern).getObjectSummaries();
        } else {
            id = addBucketPrefix(id).toLowerCase();
            os = getConnection(id).listObjects(id).getObjectSummaries();
        }

        if (os != null && os.size() > 0) {
            List<ObjectDto> objectDtos = new ArrayList<>();
            os.forEach(o -> {
                // ignore the Tag file
                String[] tempKeys = o.getKey().split("/");
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
                            objectDto = new ObjectDto(null, null, keys[0], o.getLastModified());
                            break;
                        case 2:
                            objectDto = new ObjectDto(keys[0], null, keys[1], o.getLastModified());
                            break;
                        case 3:
                            objectDto = new ObjectDto(keys[0], keys[1], keys[2], o.getLastModified());
                            break;
                        default:
                            break;
                    }
                    if (objectDto != null) objectDtos.add(objectDto);
                }
            });
            return objectDtos;
        }
        return null;
    }

    private String[] removeIdFromObjectPath(boolean useAccountAsBucketname, String[] keys) {
        return (useAccountAsBucketname && ArrayUtils.isNotEmpty(keys))
                ? (String[]) ArrayUtils.remove(keys, 0)
                : keys;
    }

    /* ======================= Connection Handling ======================= */

    private synchronized AmazonS3 getConnection(String bucketName) {
        if (connection != null) return connection;

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
                    .withClientConfiguration(clientConfig)
                    .enablePathStyleAccess()
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(url, region))
                    .build();

            // Validate once to ensure endpoint/creds are OK
            connection.doesBucketExistV2(bucketName);
            retry = 0;
        } catch (Exception e) {
            if (retry >= maxRetry) {
                retry = 0;
                connection = null;
                LOGGER.error(SESSIONID, REGISTRATIONID,
                        "Maximum retry limit exceeded. Could not obtain connection for " + bucketName + ".",
                        ExceptionUtils.getStackTrace(e));
                throw new ObjectStoreAdapterException(
                        OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                        OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
            } else {
                connection = null;
                retry = retry + 1;
                LOGGER.error(SESSIONID, REGISTRATIONID,
                        "Exception while obtaining connection for " + bucketName + ". Will try again. Retry: " + retry,
                        ExceptionUtils.getStackTrace(e));
                return getConnection(bucketName);
            }
        }
        return connection;
    }

    private void invalidateConnection() {
        connection = null;
    }

    private boolean doesBucketExists(String bucketName) {
        if (useAccountAsBucketname && existingBuckets.contains(bucketName)) return true;
        boolean doesBucketExistsInObjectStore = getConnection(bucketName).doesBucketExistV2(bucketName);
        if (useAccountAsBucketname && doesBucketExistsInObjectStore) existingBuckets.add(bucketName);
        return doesBucketExistsInObjectStore;
    }

    /* ======================= Helpers ======================= */

    private String resolveBucket(String account, String container) {
        String bucketName = useAccountAsBucketname ? account : container;
        bucketName = addBucketPrefix(bucketName);
        bucketName = bucketName.toLowerCase(); // S3/MinIO bucket rule
        return bucketName;
    }

    private String buildKey(String account, String container, String source, String process, String objectName) {
        return useAccountAsBucketname
                ? ObjectStoreUtil.getName(container, source, process, objectName)
                : ObjectStoreUtil.getName(source, process, objectName);
    }

    private String addBucketPrefix(String bucketName) {
        if (bucketName.startsWith(bucketNamePrefix)) {
            LOGGER.debug("Already bucketName with prefix is present " + bucketName);
            return bucketName;
        } else {
            String bn = bucketNamePrefix + bucketName;
            LOGGER.debug("Adding Prefix to bucketName " + bn);
            return bn;
        }
    }

    /** Read stream into memory but hard-limit to avoid heap blowups; also enables Content-Length. */
    private byte[] toByteArrayCapped(InputStream in, long maxBytes) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream((int) Math.min(maxBytes, 64 * 1024));
        byte[] buf = new byte[8192];
        long total = 0;
        int r;
        while ((r = in.read(buf)) != -1) {
            total += r;
            if (total > maxBytes) {
                throw new IOException("Object exceeds configured max size (" + maxBytes + " bytes). Read: " + total);
            }
            out.write(buf, 0, r);
        }
        return out.toByteArray();
    }
}
