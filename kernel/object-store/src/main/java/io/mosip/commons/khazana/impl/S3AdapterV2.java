package io.mosip.commons.khazana.impl;

import static io.mosip.commons.khazana.config.LoggerConfiguration.REGISTRATIONID;
import static io.mosip.commons.khazana.config.LoggerConfiguration.SESSIONID;
import static io.mosip.commons.khazana.constant.KhazanaConstant.TAGS_FILENAME;
import static io.mosip.commons.khazana.constant.KhazanaErrorCodes.OBJECT_STORE_NOT_ACCESSIBLE;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import io.mosip.commons.khazana.config.LoggerConfiguration;
import io.mosip.commons.khazana.exception.ObjectStoreAdapterException;
import io.mosip.commons.khazana.util.ObjectStoreUtil;
import io.mosip.kernel.core.exception.ExceptionUtils;
import io.mosip.kernel.core.logger.spi.Logger;

@Service
@Qualifier("S3AdapterV2")
public class S3AdapterV2 extends S3Adapter {

    private final Logger LOGGER = LoggerConfiguration.logConfig(S3AdapterV2.class);

    private static final String SEPARATOR = "/";

    private static final String TAG_BACKWARD_COMPATIBILITY_ERROR = "Object-prefix is already an object, please choose a different object-prefix name";

    private List<String> existingBuckets = new ArrayList<>();

    @Override
    public Map<String, String> addTags(String account, String container, Map<String, String> tags) {
        String bucketName = null;
        String finalObjectName = null;
        AmazonS3 connection = null;
        try {
            if (useAccountAsBucketname) {
                bucketName = account;
                finalObjectName = ObjectStoreUtil.getName(container, null, TAGS_FILENAME);
            } else {
                bucketName = container;
                finalObjectName = TAGS_FILENAME;
            }
            connection = getConnection(bucketName);
            if (!doesBucketExists(connection, bucketName)) {
                connection.createBucket(bucketName);
                if (useAccountAsBucketname)
                    existingBuckets.add(bucketName);
            }
            for (Entry<String, String> entry : tags.entrySet()) {
                String tagName = null;
                InputStream data = IOUtils.toInputStream(entry.getValue(), StandardCharsets.UTF_8);
                tagName = ObjectStoreUtil.getName(finalObjectName, entry.getKey());
                connection.putObject(bucketName, tagName, data, null);
            }
        } catch (Exception e) {
            // this check is introduced to support backward compatibility
            if (e instanceof AmazonS3Exception && e.getMessage().contains(TAG_BACKWARD_COMPATIBILITY_ERROR)) {
                connection.deleteObject(bucketName, finalObjectName);
                addTags(account, container, tags);
            } else {
                connection = null;
                LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured while addTags for : " + container,
                        ExceptionUtils.getStackTrace(e));
                throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
                        OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
            }
        }
        return tags;
    }

    @Override
    public Map<String, String> getTags(String account, String container) {
        Map<String, String> objectTags = new HashMap<String, String>();
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
            AmazonS3 connection = getConnection(bucketName);

            List<S3ObjectSummary> objectSummary = null;
            if (useAccountAsBucketname)
                objectSummary = connection.listObjects(bucketName, finalObjectName).getObjectSummaries();
            else
                objectSummary = connection.listObjects(bucketName).getObjectSummaries();

            List<String> tagNames = new ArrayList<String>();
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

    private boolean doesBucketExists(AmazonS3 connection, String bucketName) {
        // use account as bucket name and bucket name is present in existing bucket list
        if (useAccountAsBucketname && existingBuckets.contains(bucketName))
            return true;
        // use account as bucket name and bucket name is not present in existing bucket
        // list
        else if (useAccountAsBucketname && !existingBuckets.contains(bucketName)) {
            boolean doesBucketExistsInObjectStore = connection.doesBucketExistV2(bucketName);
            if (doesBucketExistsInObjectStore)
                existingBuckets.add(bucketName);
            return doesBucketExistsInObjectStore;
        } else
            return connection.doesBucketExistV2(bucketName);
    }

}
