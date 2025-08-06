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

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
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

    private int retry = 0;

    private List<String> existingBuckets = new ArrayList<>();

    private S3Client connection = null;

    private static final String SEPARATOR = "/";

    private static final String TAG_BACKWARD_COMPATIBILITY_ERROR = "Object-prefix is already an object, please choose a different object-prefix name";

    private static final String TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR = "Access Denied";

    @Override
    public InputStream getObject(String account, String container, String source, String process, String objectName) {
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
        try {
            S3Client client = getConnection(bucketName);
            GetObjectRequest req = GetObjectRequest.builder().bucket(bucketName).key(finalObjectName).build();
            ResponseInputStream<GetObjectResponse> s3Object = client.getObject(req);
            ByteArrayOutputStream temp = new ByteArrayOutputStream();
            IOUtils.copy(s3Object, temp);
            return new ByteArrayInputStream(temp.toByteArray());
        } catch (S3Exception e) {
            return null;
        } catch (Exception e) {
            connection = null;
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured to getObject for : " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public boolean exists(String account, String container, String source, String process, String objectName) {
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
        try {
            S3Client client = getConnection(bucketName);
            HeadObjectRequest req = HeadObjectRequest.builder().bucket(bucketName).key(finalObjectName).build();
            client.headObject(req);
            return true;
        } catch (S3Exception e) {
            return false;
        }
    }

    @Override
    public boolean putObject(String account, final String container, String source, String process, String objectName, InputStream data) {
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
        S3Client client = getConnection(bucketName);
        if (!doesBucketExists(bucketName)) {
            client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
            if (useAccountAsBucketname)
                existingBuckets.add(bucketName);
        }
        try {
            byte[] contentBytes = IOUtils.toByteArray(data);
            client.putObject(PutObjectRequest.builder().bucket(bucketName).key(finalObjectName).build(),
                    RequestBody.fromBytes(contentBytes));
            return true;
        } catch (Exception e) {
            connection = null;
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured to putObject for : " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process,
                                                 String objectName, Map<String, Object> metadata) {
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
        try {
            S3Client client = getConnection(bucketName);
            // Download the object content first
            GetObjectRequest getReq = GetObjectRequest.builder().bucket(bucketName).key(finalObjectName).build();
            ResponseInputStream<GetObjectResponse> s3Object = client.getObject(getReq);
            byte[] contentBytes = IOUtils.toByteArray(s3Object);
            // Get existing metadata
            HeadObjectResponse head = client.headObject(HeadObjectRequest.builder().bucket(bucketName).key(finalObjectName).build());
            Map<String, String> userMeta = new HashMap<>();
            if (head.metadata() != null) {
                userMeta.putAll(head.metadata());
            }
            metadata.forEach((k, v) -> userMeta.put(k, v != null ? v.toString() : null));
            // Re-upload the object with the new metadata
            client.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(finalObjectName)
                            .metadata(userMeta)
                            .build(),
                    RequestBody.fromBytes(contentBytes)
            );
            return metadata;
        } catch (Exception e) {
            connection = null;
            LOGGER.error(SESSIONID, REGISTRATIONID,"Exception occured to addObjectMetaData for : " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    @Override
    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process, String objectName, String key, String value) {
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
        try {
            S3Client client = getConnection(bucketName);
            HeadObjectResponse head = client.headObject(HeadObjectRequest.builder().bucket(bucketName).key(finalObjectName).build());
            Map<String, Object> metaData = new HashMap<>();
            if (head.metadata() != null) {
                metaData.putAll(head.metadata());
            }
            return metaData;
        } catch (Exception e) {
            connection = null;
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
        try {
            getConnection(bucketName).deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(finalObjectName).build());
            return true;
        } catch (Exception e) {
            connection = null;
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured to deleteObject for : " + container, ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
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

    private S3Client getConnection(String bucketName) {
        if (connection != null)
            return connection;

        try {
            connection = S3Client.builder()
                    .endpointOverride(java.net.URI.create(url))
                    .region(Region.of(region))
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                    .build();
            // test connection once before returning it
            connection.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
            retry = 0;
        } catch (Exception e) {
            if (retry >= maxRetry) {
                retry = 0;
                connection = null;
                LOGGER.error(SESSIONID, REGISTRATIONID,"Maximum retry limit exceeded. Could not obtain connection for "+ bucketName +". Retry count :" + retry, ExceptionUtils.getStackTrace(e));
                throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
            } else {
                connection = null;
                retry = retry + 1;
                LOGGER.error(SESSIONID, REGISTRATIONID,"Exception occured while obtaining connection for "+ bucketName +". Will try again. Retry count : " + retry, ExceptionUtils.getStackTrace(e));
                getConnection(bucketName);
            }
        }
        return connection;
    }

    public List<ObjectDto> getAllObjects(String account, String id) {
        List<S3Object> os = null;
        if (useAccountAsBucketname) {
            String searchPattern = id + SEPARATOR;
            account = addBucketPrefix(account);
            account = account.toLowerCase();
            os = getConnection(account)
                    .listObjectsV2(ListObjectsV2Request.builder().bucket(account).prefix(searchPattern).build())
                    .contents();
        } else {
            id = addBucketPrefix(id);
            id = id.toLowerCase();
            os = getConnection(id)
                    .listObjectsV2(ListObjectsV2Request.builder().bucket(id).build())
                    .contents();
        }
        if (os != null && !os.isEmpty()) {
            List<ObjectDto> objectDtos = new ArrayList<>();
            for (S3Object o : os) {
                String[] tempKeys = o.key().split("/");
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
                            objectDto = new ObjectDto(null, null, keys[0], Date.from( o.lastModified()));
                            break;
                        case 2:
                            objectDto = new ObjectDto(keys[0], null, keys[1],  Date.from(o.lastModified()));
                            break;
                        case 3:
                            objectDto = new ObjectDto(keys[0], keys[1], keys[2],  Date.from(o.lastModified()));
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
            S3Client client = getConnection(bucketName);
            if (!doesBucketExists(bucketName)) {
                client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
                if (useAccountAsBucketname)
                    existingBuckets.add(bucketName);
            }
            for (Entry<String, String> entry : tags.entrySet()) {
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
                            connection = null;
                            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured while addTags for : " + container,
                                    ExceptionUtils.getStackTrace(e));
                            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
                        }
                    } else {
                        connection = null;
                        LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured while addTags for : " + container,
                                ExceptionUtils.getStackTrace(e));
                        throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                                OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
                    }
                }
            }

        } catch (Exception e) {
            connection = null;
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
            S3Client client = getConnection(bucketName);

            List<S3Object> objectSummary = null;
            if (useAccountAsBucketname)
                objectSummary = client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).prefix(finalObjectName).build()).contents();
            else
                objectSummary = client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).build()).contents();

            List<String> tagNames = new ArrayList<>();
            if (objectSummary != null && objectSummary.size() > 0) {
                objectSummary.forEach(o -> {
                    String[] keys = o.key().split("/");
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
                objectTags.put(tagName, client.getObjectAsBytes(GetObjectRequest.builder().bucket(bucketName).key(finalObjectName + tagName).build()).asUtf8String());
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
            S3Client client = getConnection(bucketName);
            if (!doesBucketExists(bucketName)) {
                client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
                if (useAccountAsBucketname)
                    existingBuckets.add(bucketName);
            }
            for (String tag : tags) {
                String tagName = ObjectStoreUtil.getName(finalObjectName, tag);
                client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(tagName).build());
            }
        } catch (Exception e) {
            connection = null;
            LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured while deleteTags for : " + container,
                    ExceptionUtils.getStackTrace(e));
            throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                    OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
        }
    }

    private boolean doesBucketExists(String bucketName) {
        if (useAccountAsBucketname && existingBuckets.contains(bucketName))
            return true;
        else if (useAccountAsBucketname && !existingBuckets.contains(bucketName)) {
            boolean doesBucketExistsInObjectStore;
            try {
                doesBucketExistsInObjectStore = getConnection(bucketName)
                        .headBucket(HeadBucketRequest.builder().bucket(bucketName).build()) != null;
            } catch (S3Exception e) {
                doesBucketExistsInObjectStore = false;
            }
            if (doesBucketExistsInObjectStore)
                existingBuckets.add(bucketName);
            return doesBucketExistsInObjectStore;
        } else {
            try {
                getConnection(bucketName)
                        .headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
                return true;
            } catch (S3Exception e) {
                return false;
            }
        }
    }

    private String addBucketPrefix(String bucketName) {
        if (bucketName.startsWith(bucketNamePrefix)) {
            LOGGER.debug("Already bucketName with prefix is present" + bucketName);
            return bucketName;
        } else {
            bucketName = bucketNamePrefix + bucketName;
            LOGGER.debug("Adding  Prefix to bucketName" + bucketName);
            return bucketName;
        }
    }
}