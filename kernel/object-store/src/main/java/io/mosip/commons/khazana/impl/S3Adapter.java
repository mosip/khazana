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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
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
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

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

	@Value("${object.store.s3.use.account.as.bucketname:false}")
	private boolean useAccountAsBucketname;

	@Value("${object.store.s3.bucket-name-prefix:}")
	private String bucketNamePrefix;

	private int retry = 0;

	private List<String> existingBuckets = new ArrayList<>();

	private AmazonS3 connection = null;

	private static final String SEPARATOR = "/";

	private static final String TAG_BACKWARD_COMPATIBILITY_ERROR = "Object-prefix is already an object, please choose a different object-prefix name";

	private static final String TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR = "Access Denied";

	@Override
	public InputStream getObject(String account, String container, String source, String process, String objectName) {
	    String objectKey = useAccountAsBucketname
	            ? ObjectStoreUtil.getName(container, source, process, objectName)
	            : ObjectStoreUtil.getName(source, process, objectName);

	    String bucketName = useAccountAsBucketname ? account : container;
	    bucketName = addBucketPrefix(bucketName).toLowerCase();

	    S3Object s3Object = null;

	    try {
	        AmazonS3 connection = getConnection(bucketName);
	        s3Object = connection.getObject(bucketName, objectKey);

	        if (s3Object == null || s3Object.getObjectContent() == null) {
	            LOGGER.warn(SESSIONID, REGISTRATIONID, "Object not found: {}/{}", bucketName, objectKey);
	            return null;
	        }

	     // Copy fully and close underlying stream explicitly
	        S3ObjectInputStream s3is = s3Object.getObjectContent();
	        ByteArrayOutputStream temp = new ByteArrayOutputStream();
	        IOUtils.copy(s3is, temp);
	        s3is.close(); // ✅ Ensure full closure of stream

	        return new ByteArrayInputStream(temp.toByteArray());
	    } catch (Exception e) {
	        this.connection = null;
	        LOGGER.error(SESSIONID, REGISTRATIONID,
	                "Exception occurred while fetching object: {}/{}", bucketName, objectKey, ExceptionUtils.getStackTrace(e));
	        throw new ObjectStoreAdapterException(
	                OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
	                OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
	    } finally {
	        if (s3Object != null) {
	            try {
	                s3Object.close(); // This also closes the underlying input stream
	            } catch (IOException e) {
	                LOGGER.error(SESSIONID, REGISTRATIONID,
	                        "IO exception while closing S3 object: " + bucketName,
	                        ExceptionUtils.getStackTrace(e));
	            }
	        }
	    }
	}


	/*@Override
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
		// As per AmazonS3 bucket naming rules,name contains only lower case letters
		bucketName = bucketName.toLowerCase();
		S3Object s3Object = null;
		try {
			s3Object = getConnection(bucketName).getObject(bucketName, finalObjectName);
			if (s3Object != null) {
				ByteArrayOutputStream temp = new ByteArrayOutputStream();
				IOUtils.copy(s3Object.getObjectContent(), temp);
				ByteArrayInputStream bis = new ByteArrayInputStream(temp.toByteArray());
				return bis;
			}
		} catch (Exception e) {
			connection = null;
			LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured to getObject for : " + container,
					ExceptionUtils.getStackTrace(e));
			throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
					OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
		} finally {
			if (s3Object != null) {
				try {
					s3Object.close();
				} catch (IOException e) {
					LOGGER.error(SESSIONID, REGISTRATIONID, "IO occured : " + container,
							ExceptionUtils.getStackTrace(e));
				}
			}
		}
		return null;
	}*/

	@Override
	public boolean exists(String account, String container, String source, String process, String objectName) {
	    try {
	        String objectKey = useAccountAsBucketname
	                ? ObjectStoreUtil.getName(container, source, process, objectName)
	                : ObjectStoreUtil.getName(source, process, objectName);

	        String bucketName = useAccountAsBucketname ? account : container;
	        bucketName = addBucketPrefix(bucketName).toLowerCase();

	        AmazonS3 connection = getConnection(bucketName);
	        return connection.doesObjectExist(bucketName, objectKey);
	    } catch (Exception e) {
	        LOGGER.error(SESSIONID, REGISTRATIONID, "Error checking existence for object: {} in container: {}",
	                objectName, container, ExceptionUtils.getStackTrace(e));
	        throw new ObjectStoreAdapterException(
	                OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
	                OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
	    }
	}

	/*@Override
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
		// As per AmazonS3 bucket naming rules,name contains only lower case letters
		bucketName = bucketName.toLowerCase();
		return getConnection(bucketName).doesObjectExist(bucketName, finalObjectName);
	}
*/
	@Override
	public boolean putObject(String account, String container, String source, String process, String objectName,
	                         InputStream data) {
	    String objectKey = useAccountAsBucketname
	            ? ObjectStoreUtil.getName(container, source, process, objectName)
	            : ObjectStoreUtil.getName(source, process, objectName);

	    String bucketName = useAccountAsBucketname ? account : container;
	    bucketName = addBucketPrefix(bucketName).toLowerCase();

	    try {
	        AmazonS3 connection = getConnection(bucketName);

	        // Ensure bucket exists
	        if (!doesBucketExists(bucketName)) {
	            connection.createBucket(bucketName);
	            if (useAccountAsBucketname) {
	                existingBuckets.add(bucketName);
	            }
	        }

	        // Optional: wrap stream to determine content length
	        byte[] bytes = IOUtils.toByteArray(data);
	        ObjectMetadata metadata = new ObjectMetadata();
	        metadata.setContentLength(bytes.length);
	        // Optional: metadata.setContentType("application/octet-stream");

	        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
	            connection.putObject(bucketName, objectKey, bais, metadata);
	        }

	        LOGGER.info(SESSIONID, REGISTRATIONID, "Successfully uploaded object: {}/{}", bucketName, objectKey);
	        return true;

	    } catch (Exception e) {
	        this.connection = null;
	        LOGGER.error(SESSIONID, REGISTRATIONID,
	                "Failed to upload object: {}/{}. Error: {}", bucketName, objectName, ExceptionUtils.getStackTrace(e));
	        throw new ObjectStoreAdapterException(
	                OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
	                OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
	    }
	}
	
	/*@Override
	public boolean putObject(String account, final String container, String source, String process, String objectName,
			InputStream data) {
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
		// As per AmazonS3 bucket naming rules,name contains only lower case letters
		bucketName = bucketName.toLowerCase();
		AmazonS3 connection = getConnection(bucketName);
		if (!doesBucketExists(bucketName)) {
			connection.createBucket(bucketName);
			if (useAccountAsBucketname)
				existingBuckets.add(bucketName);
		}

		connection.putObject(bucketName, finalObjectName, data, null);
		return true;
	}*/

	@Override
	public Map<String, Object> addObjectMetaData(String account, String container, String source, String process,
	                                             String objectName, Map<String, Object> newMetadata) {
	    String objectKey = useAccountAsBucketname
	            ? ObjectStoreUtil.getName(container, source, process, objectName)
	            : ObjectStoreUtil.getName(source, process, objectName);
	    String bucketName = useAccountAsBucketname ? account : container;
	    bucketName = addBucketPrefix(bucketName).toLowerCase();

	    try (S3Object s3Object = getConnection(bucketName).getObject(bucketName, objectKey);
	         ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

	        // Copy content to memory
	        IOUtils.copy(s3Object.getObjectContent(), baos);
	        byte[] contentBytes = baos.toByteArray();

	        // Prepare new metadata object
	        ObjectMetadata existingMeta = s3Object.getObjectMetadata();
	        ObjectMetadata updatedMeta = new ObjectMetadata();
	        updatedMeta.setContentLength(contentBytes.length);
	        updatedMeta.setContentType(existingMeta.getContentType());

	        // Copy old user metadata
	        if (existingMeta.getUserMetadata() != null) {
	            existingMeta.getUserMetadata().forEach(updatedMeta::addUserMetadata);
	        }

	        // Add/override new metadata
	        newMetadata.forEach((k, v) ->
	                updatedMeta.addUserMetadata(k, v != null ? v.toString() : null)
	        );

	        // Re-upload object with updated metadata
	        try (ByteArrayInputStream bais = new ByteArrayInputStream(contentBytes)) {
	            PutObjectRequest request = new PutObjectRequest(bucketName, objectKey, bais, updatedMeta);
	            request.getRequestClientOptions().setReadLimit(readlimit);
	            getConnection(bucketName).putObject(request);
	        }

	        return newMetadata;

	    } catch (Exception e) {
	        connection = null;
	        LOGGER.error(SESSIONID, REGISTRATIONID,
	                "Exception occurred while updating metadata for object: {} in bucket: {}",
	                objectName, bucketName, ExceptionUtils.getStackTrace(e));
	        throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
	                OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
	    }
	}
	
	/*@Override
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
			// As per AmazonS3 bucket naming rules,name contains only lower case letters
			bucketName = bucketName.toLowerCase();
			ObjectMetadata objectMetadata = new ObjectMetadata();
			// changed usermetadata getting overrided
			// metadata.entrySet().stream().forEach(m ->
			// objectMetadata.addUserMetadata(m.getKey(), m.getValue() != null ?
			// m.getValue().toString() : null));

			s3Object = getConnection(bucketName).getObject(bucketName, finalObjectName);
			if (s3Object.getObjectMetadata() != null && s3Object.getObjectMetadata().getUserMetadata() != null)
				s3Object.getObjectMetadata().getUserMetadata().entrySet()
						.forEach(m -> objectMetadata.addUserMetadata(m.getKey(), m.getValue()));
			metadata.entrySet().stream().forEach(m -> objectMetadata.addUserMetadata(m.getKey(),
					m.getValue() != null ? m.getValue().toString() : null));
			PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, finalObjectName,
					s3Object.getObjectContent(), objectMetadata);
			putObjectRequest.getRequestClientOptions().setReadLimit(readlimit);
			getConnection(bucketName).putObject(putObjectRequest);
			return metadata;
		} catch (Exception e) {
			connection = null;
			LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured to addObjectMetaData for : " + container,
					ExceptionUtils.getStackTrace(e));
			metadata = null;
			throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
					OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
		} finally {
			try {
				if (s3Object != null)
					s3Object.close();
			} catch (IOException e) {
				LOGGER.error(SESSIONID, REGISTRATIONID, "IO occured : " + container, ExceptionUtils.getStackTrace(e));
			}
		}
	}*/

	@Override
	public Map<String, Object> addObjectMetaData(String account, String container, String source, String process,
	                                             String objectName, String key, String value) {
	    Map<String, Object> metadata = new HashMap<>();
	    metadata.put(key, value);
	    return addObjectMetaData(account, container, source, process, objectName, metadata);
	}

/*	@Override
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
	}*/

	@Override
	public Map<String, Object> getMetaData(String account, String container, String source, String process,
	                                       String objectName) {
	    Map<String, Object> metaData = new HashMap<>();
	    String bucketName;
	    String objectKey;

	    if (useAccountAsBucketname) {
	        objectKey = ObjectStoreUtil.getName(container, source, process, objectName);
	        bucketName = account;
	    } else {
	        objectKey = ObjectStoreUtil.getName(source, process, objectName);
	        bucketName = container;
	    }

	    bucketName = addBucketPrefix(bucketName).toLowerCase();

	    try (S3Object s3Object = getConnection(bucketName).getObject(bucketName, objectKey)) {
	        ObjectMetadata objectMetadata = s3Object.getObjectMetadata();
	        if (objectMetadata != null && objectMetadata.getUserMetadata() != null) {
	            objectMetadata.getUserMetadata().forEach(metaData::put);
	        }
	        return metaData;

	    } catch (Exception e) {
	        this.connection = null;
	        LOGGER.error(SESSIONID, REGISTRATIONID,
	                String.format("Exception occurred while getting metadata for object: %s in bucket: %s", objectKey, bucketName),
	                ExceptionUtils.getStackTrace(e));
	        throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
	                OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
	    }
	}

	/*@Override
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
			// As per AmazonS3 bucket naming rules,name contains only lower case letters
			bucketName = bucketName.toLowerCase();
			Map<String, Object> metaData = new HashMap<>();

			s3Object = getConnection(bucketName).getObject(bucketName, finalObjectName);
			ObjectMetadata objectMetadata = s3Object.getObjectMetadata();
			if (objectMetadata != null && objectMetadata.getUserMetadata() != null)
				objectMetadata.getUserMetadata().entrySet()
						.forEach(entry -> metaData.put(entry.getKey(), entry.getValue()));
			return metaData;
		} catch (Exception e) {
			connection = null;
			LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured to getMetaData for : " + container,
					ExceptionUtils.getStackTrace(e));
			throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
					OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
		} finally {
			try {
				if (s3Object != null)
					s3Object.close();
			} catch (IOException e) {
				LOGGER.error(SESSIONID, REGISTRATIONID, "IO occured : " + container, ExceptionUtils.getStackTrace(e));
			}
		}
	}*/

	@Override
	public Integer incMetadata(String account, String container, String source, String process, String objectName,
	                           String metaDataKey) {
	    return updateMetadataCounter(account, container, source, process, objectName, metaDataKey, 1);
	}

	@Override
	public Integer decMetadata(String account, String container, String source, String process, String objectName,
	                           String metaDataKey) {
	    return updateMetadataCounter(account, container, source, process, objectName, metaDataKey, -1);
	}

	private Integer updateMetadataCounter(String account, String container, String source, String process,
	                                      String objectName, String metaDataKey, int delta) {
	    try {
	        Map<String, Object> metadata = getMetaData(account, container, source, process, objectName);
	        if (metadata == null || !metadata.containsKey(metaDataKey)) {
	            LOGGER.warn(SESSIONID, REGISTRATIONID, "Metadata key '{}' not found for object '{}'", metaDataKey, objectName);
	            return null;
	        }

	        int currentValue = Integer.parseInt(metadata.get(metaDataKey).toString());
	        int newValue = currentValue + delta;
	        metadata.put(metaDataKey, newValue);
	        addObjectMetaData(account, container, source, process, objectName, metadata);
	        return newValue;

	    } catch (NumberFormatException e) {
	        LOGGER.error(SESSIONID, REGISTRATIONID, "Invalid numeric value for metadata key '{}' on object '{}': {}",
	                metaDataKey, objectName, ExceptionUtils.getStackTrace(e));
	        return null;
	    } catch (Exception e) {
	        LOGGER.error(SESSIONID, REGISTRATIONID, "Exception updating metadata '{}' on object '{}'",
	                metaDataKey, objectName, ExceptionUtils.getStackTrace(e));
	        throw new ObjectStoreAdapterException(
	                OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
	                OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
	    }
	}

	/*@Override
	public Integer incMetadata(String account, String container, String source, String process, String objectName,
			String metaDataKey) {
		Map<String, Object> metadata = getMetaData(account, container, source, process, objectName);
		if (metadata.get(metaDataKey) != null) {
			metadata.put(metaDataKey, Integer.valueOf(metadata.get(metaDataKey).toString()) + 1);
			addObjectMetaData(account, container, source, process, objectName, metadata);
			return Integer.valueOf(metadata.get(metaDataKey).toString());
		}
		return null;
	}

	@Override
	public Integer decMetadata(String account, String container, String source, String process, String objectName,
			String metaDataKey) {
		Map<String, Object> metadata = getMetaData(account, container, source, process, objectName);
		if (metadata.get(metaDataKey) != null) {
			metadata.put(metaDataKey, Integer.valueOf(metadata.get(metaDataKey).toString()) - 1);
			addObjectMetaData(account, container, source, process, objectName, metadata);
			return Integer.valueOf(metadata.get(metaDataKey).toString());
		}
		return null;
	}*/

	@Override
	public boolean deleteObject(String account, String container, String source, String process, String objectName) {
	    try {
	        Pair<String, String> resolved = resolveBucketAndObjectName(account, container, source, process, objectName);
	        String bucketName = resolved.getLeft();
	        String finalObjectName = resolved.getRight();

	        AmazonS3 connection = getConnection(bucketName);
	        connection.deleteObject(bucketName, finalObjectName);

	        LOGGER.debug(SESSIONID, REGISTRATIONID,
	                "Successfully deleted object: {} from bucket: {}", finalObjectName, bucketName);

	        return true;

	    } catch (Exception e) {
	        LOGGER.error(SESSIONID, REGISTRATIONID,
	                "Failed to delete object: " + objectName + " from container: " + container,
	                ExceptionUtils.getStackTrace(e));
	        throw new ObjectStoreAdapterException(
	                OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
	                OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
	    }
	}

	private Pair<String, String> resolveBucketAndObjectName(String account, String container, String source, String process, String objectName) {
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
	    return Pair.of(bucketName, finalObjectName);
	}
	
	/*@Override
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
		// As per AmazonS3 bucket naming rules,name contains only lower case letters
		bucketName = bucketName.toLowerCase();
		getConnection(bucketName).deleteObject(bucketName, finalObjectName);
		return true;
	}*/

	/**
	 * Removing container not supported in S3Adapter
	 *
	 * @param account
	 * @param container
	 * @param source
	 * @param process
	 * @return
	 */
	@Override
	public boolean removeContainer(String account, String container, String source, String process) {
		return false;
	}

	/**
	 * Not Supported in S3Adapter
	 *
	 * @param account
	 * @param container
	 * @param source
	 * @param process
	 * @return
	 */
	@Override
	public boolean pack(String account, String container, String source, String process, String refId) {
		return false;
	}

	/**
	 * This method will return a singleton connection. It will verify connection for
	 * the first time and will reuse same connection in subsequent calls.
	 *
	 * @param bucketName
	 * @return
	 */
	private AmazonS3 getConnection(String bucketName) {
	    if (connection != null) {
	        return connection;
	    }

	    synchronized (this) {
	        if (connection != null) {
	            return connection;
	        }

	        int attempts = 0;
	        while (attempts < maxRetry) {
	            try {
	                AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
	                AmazonS3 s3 = AmazonS3ClientBuilder.standard()
	                        .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
	                        .enablePathStyleAccess()
	                        .withClientConfiguration(new ClientConfiguration()
	                                .withMaxConnections(maxConnection)
	                                .withMaxErrorRetry(maxRetry))
	                        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(url, region))
	                        .build();

	                // Validate the connection
	                s3.doesBucketExistV2(bucketName);

	                // Success
	                connection = s3;
	                return connection;

	            } catch (Exception e) {
	                attempts++;
	                LOGGER.error(SESSIONID, REGISTRATIONID,
	                        "Attempt " + attempts + " failed to create S3 connection for bucket: " + bucketName,
	                        ExceptionUtils.getStackTrace(e));

	                if (attempts >= maxRetry) {
	                    LOGGER.error(SESSIONID, REGISTRATIONID,
	                            "Maximum retry limit exceeded. Unable to create connection for bucket: " + bucketName);
	                    throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
	                            OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
	                }

	                // Sleep briefly before retrying (optional)
	                try {
	                    Thread.sleep(1000);
	                } catch (InterruptedException ie) {
	                    Thread.currentThread().interrupt(); // Preserve interrupt status
	                    throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
	                            "Thread interrupted during S3 connection retry", ie);
	                }
	            }
	        }

	        throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
	                "Unable to connect to S3 after retries", null);
	    }
	}
	
/*	private AmazonS3 getConnection(String bucketName) {
		if (connection != null)
			return connection;

		try {
			AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
			connection = AmazonS3ClientBuilder.standard()
					.withCredentials(new AWSStaticCredentialsProvider(awsCredentials)).enablePathStyleAccess()
					.withClientConfiguration(
							new ClientConfiguration().withMaxConnections(maxConnection).withMaxErrorRetry(maxRetry))
					.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(url, region)).build();
			// test connection once before returning it
			connection.doesBucketExistV2(bucketName);
			// reset retry after every successful connection so that in case of failure it
			// starts from zero.
			retry = 0;
		} catch (Exception e) {
			if (retry >= maxRetry) {
				// reset the connection and retry count
				retry = 0;
				connection = null;
				LOGGER.error(SESSIONID, REGISTRATIONID, "Maximum retry limit exceeded. Could not obtain connection for "
						+ bucketName + ". Retry count :" + retry, ExceptionUtils.getStackTrace(e));
				throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
						OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
			} else {
				connection = null;
				retry = retry + 1;
				LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured while obtaining connection for " + bucketName
						+ ". Will try again. Retry count : " + retry, ExceptionUtils.getStackTrace(e));
				getConnection(bucketName);
			}
		}
		return connection;
	}*/

	@Override
	public List<ObjectDto> getAllObjects(String account, String id) {
	    String bucketName = useAccountAsBucketname ? account : id;
	    String prefix = useAccountAsBucketname ? id + SEPARATOR : null;

	    bucketName = addBucketPrefix(bucketName).toLowerCase();
	    AmazonS3 connection = getConnection(bucketName);

	    List<S3ObjectSummary> summaries = useAccountAsBucketname
	            ? connection.listObjects(bucketName, prefix).getObjectSummaries()
	            : connection.listObjects(bucketName).getObjectSummaries();

	    List<ObjectDto> objectDtos = new ArrayList<>();
	    if (summaries == null || summaries.isEmpty()) {
	        return objectDtos; // Return empty list instead of null
	    }

	    for (S3ObjectSummary summary : summaries) {
	        String[] parts = summary.getKey().split(SEPARATOR);

	        // Filter out tag file entries
	        if (useAccountAsBucketname && parts.length > 1 && TAGS_FILENAME.equals(parts[1])) {
	            continue;
	        } else if (!useAccountAsBucketname && parts.length > 0 && TAGS_FILENAME.equals(parts[0])) {
	            continue;
	        }

	        String[] keys = removeIdFromObjectPath(useAccountAsBucketname, parts);
	        if (ArrayUtils.isEmpty(keys)) {
	            continue;
	        }

	        ObjectDto dto = switch (keys.length) {
	            case 1 -> new ObjectDto(null, null, keys[0], summary.getLastModified());
	            case 2 -> new ObjectDto(keys[0], null, keys[1], summary.getLastModified());
	            case 3 -> new ObjectDto(keys[0], keys[1], keys[2], summary.getLastModified());
	            default -> null;
	        };

	        if (dto != null) {
	            objectDtos.add(dto);
	        }
	    }

	    return objectDtos;
	}

	
	/*public List<ObjectDto> getAllObjects(String account, String id) {
		List<S3ObjectSummary> os = null;
		if (useAccountAsBucketname) {
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
			return objectDtos;
		}

		return null;
	}*/

	/**
	 * If account is used as bucket name then first element of array is the packet
	 * id. This method removes packet id from array so that path is same
	 * irrespective of useAccountAsBucketname is true or false
	 *
	 * @param useAccountAsBucketname
	 * @param keys
	 */
	private String[] removeIdFromObjectPath(boolean useAccountAsBucketname, String[] keys) {
	    if (!useAccountAsBucketname || keys == null || keys.length == 0) {
	        return keys;
	    }
	    return Arrays.copyOfRange(keys, 1, keys.length);
	}

/*	private String[] removeIdFromObjectPath(boolean useAccountAsBucketname, String[] keys) {
		return (useAccountAsBucketname && ArrayUtils.isNotEmpty(keys)) ? (String[]) ArrayUtils.remove(keys, 0) : keys;
	}
*/
	@Override
	public Map<String, String> addTags(String account, String container, Map<String, String> tags) {
	    try {
	        String bucketName = useAccountAsBucketname ? account : container;
	        String tagPrefix = useAccountAsBucketname
	                ? ObjectStoreUtil.getName(container, null, TAGS_FILENAME)
	                : TAGS_FILENAME;

	        bucketName = addBucketPrefix(bucketName).toLowerCase();
	        AmazonS3 connection = getConnection(bucketName);

	        // Ensure the bucket exists
	        if (!doesBucketExists(bucketName)) {
	            connection.createBucket(bucketName);
	            if (useAccountAsBucketname) {
	                existingBuckets.add(bucketName);
	            }
	        }

	        for (Map.Entry<String, String> entry : tags.entrySet()) {
	            String tagKey = entry.getKey();
	            String tagValue = entry.getValue();
	            String fullTagPath = ObjectStoreUtil.getName(tagPrefix, tagKey);

	            try (InputStream data = IOUtils.toInputStream(tagValue, StandardCharsets.UTF_8)) {
	                connection.putObject(bucketName, fullTagPath, data, null);
	                LOGGER.debug(SESSIONID, REGISTRATIONID, "Tag written: {}", fullTagPath);
	            } catch (AmazonS3Exception ex) {
	                boolean isCompatibilityError = ex.getMessage().contains(TAG_BACKWARD_COMPATIBILITY_ERROR)
	                        || ex.getMessage().contains(TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR);
	                if (isCompatibilityError) {
	                    LOGGER.warn(SESSIONID, REGISTRATIONID,
	                            "Tag compatibility issue detected for path: {}, retrying...", fullTagPath);

	                    // Remove legacy object blocking directory-like structure
	                    if (connection.doesObjectExist(bucketName, tagPrefix)) {
	                        connection.deleteObject(bucketName, tagPrefix);
	                        // Retry writing this tag once
	                        try (InputStream retryData = IOUtils.toInputStream(tagValue, StandardCharsets.UTF_8)) {
	                            connection.putObject(bucketName, fullTagPath, retryData, null);
	                        }
	                    } else {
	                        throw ex;
	                    }
	                } else {
	                    throw ex;
	                }
	            }
	        }

	        return tags;

	    } catch (Exception e) {
	        this.connection = null;
	        LOGGER.error(SESSIONID, REGISTRATIONID,
	                "Exception occurred while addTags for container: " + container, ExceptionUtils.getStackTrace(e));
	        throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
	                OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
	    }
	}

	/*@Override
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
			// As per AmazonS3 bucket naming rules,name contains only lower case letters
			bucketName = bucketName.toLowerCase();
			AmazonS3 connection = getConnection(bucketName);
			if (!doesBucketExists(bucketName)) {
				connection.createBucket(bucketName);
				if (useAccountAsBucketname)
					existingBuckets.add(bucketName);
			}
			for (Entry<String, String> entry : tags.entrySet()) {
				String tagName = null;
				InputStream data = IOUtils.toInputStream(entry.getValue(), StandardCharsets.UTF_8);
				tagName = ObjectStoreUtil.getName(finalObjectName, entry.getKey());
				try {
					connection.putObject(bucketName, tagName, data, null);
				} catch (Exception e) {
					// this check is introduced to support backward compatibility
					if (e instanceof AmazonS3Exception && (e.getMessage().contains(TAG_BACKWARD_COMPATIBILITY_ERROR)
							|| e.getMessage().contains(TAG_BACKWARD_COMPATIBILITY_ACCESS_DENIED_ERROR))) {
						if (connection.doesObjectExist(bucketName, finalObjectName)) {
							connection.deleteObject(bucketName, finalObjectName);
							addTags(account, container, tags);
						} else {
							connection = null;
							LOGGER.error(SESSIONID, REGISTRATIONID,
									"Exception occured while addTags for : " + container,
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
	}*/

	@Override
	public Map<String, String> getTags(String account, String container) {
	    Map<String, String> objectTags = new HashMap<>();
	    try {
	        String bucketName;
	        String tagsPrefix;

	        if (useAccountAsBucketname) {
	            bucketName = account;
	            tagsPrefix = ObjectStoreUtil.getName(container, null, TAGS_FILENAME) + SEPARATOR;
	        } else {
	            bucketName = container;
	            tagsPrefix = TAGS_FILENAME + SEPARATOR;
	        }

	        bucketName = addBucketPrefix(bucketName).toLowerCase();
	        AmazonS3 connection = getConnection(bucketName);

	        List<S3ObjectSummary> objectSummaries = connection.listObjects(bucketName, tagsPrefix).getObjectSummaries();

	        if (objectSummaries != null && !objectSummaries.isEmpty()) {
	            for (S3ObjectSummary summary : objectSummaries) {
	                String[] parts = summary.getKey().split(SEPARATOR);
	                if (useAccountAsBucketname && parts.length >= 3 && TAGS_FILENAME.equals(parts[1])) {
	                    String tagName = parts[2];
	                    objectTags.put(tagName, connection.getObjectAsString(bucketName, summary.getKey()));
	                } else if (!useAccountAsBucketname && parts.length >= 2 && TAGS_FILENAME.equals(parts[0])) {
	                    String tagName = parts[1];
	                    objectTags.put(tagName, connection.getObjectAsString(bucketName, summary.getKey()));
	                }
	            }
	        }

	        return objectTags;
	    } catch (Exception e) {
	        LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occurred while getting tags for container: " + container,
	                ExceptionUtils.getStackTrace(e));
	        throw new ObjectStoreAdapterException(
	                OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
	                OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
	    }
	}

	/*@Override
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
			bucketName = addBucketPrefix(bucketName);
			// As per AmazonS3 bucket naming rules,name contains only lower case letters
			bucketName = bucketName.toLowerCase();
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
	}*/

	@Override
	public void deleteTags(String account, String container, List<String> tags) {
	    String bucketName;
	    String finalObjectName;

	    try {
	        // Resolve bucket name and tag prefix path
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
	            if (useAccountAsBucketname) {
	                existingBuckets.add(bucketName);
	            }
	        }

	        for (String tag : tags) {
	            String tagPath = ObjectStoreUtil.getName(finalObjectName, tag);
	            try {
	                connection.deleteObject(bucketName, tagPath);
	                LOGGER.info(SESSIONID, REGISTRATIONID, "Deleted tag: " + tagPath);
	            } catch (Exception e) {
	                LOGGER.warn(SESSIONID, REGISTRATIONID, "Failed to delete tag: " + tagPath,
	                        ExceptionUtils.getStackTrace(e));
	            }
	        }

	    } catch (Exception e) {
	        this.connection = null; // Invalidate cached connection
	        LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occurred while deleteTags for container: " + container,
	                ExceptionUtils.getStackTrace(e));
	        throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
	                OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
	    }
	}

	/*@Override
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
			// As per AmazonS3 bucket naming rules,name contains only lower case letters
			bucketName = bucketName.toLowerCase();
			AmazonS3 connection = getConnection(container);
			if (!doesBucketExists(container)) {
				connection.createBucket(container);
				if (useAccountAsBucketname)
					existingBuckets.add(bucketName);
			}
			for (String tag : tags) {
				String tagName = null;
				tagName = ObjectStoreUtil.getName(finalObjectName, tag);
				connection.deleteObject(bucketName, tagName);
			}

		} catch (Exception e) {
			connection = null;
			LOGGER.error(SESSIONID, REGISTRATIONID, "Exception occured while deleteTags for : " + container,
					ExceptionUtils.getStackTrace(e));
			throw new ObjectStoreAdapterException(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(),
					OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage(), e);
		}
	}*/

	private boolean doesBucketExists(String bucketName) {
	    if (useAccountAsBucketname) {
	        if (existingBuckets.contains(bucketName)) return true;

	        boolean exists = getConnection(bucketName).doesBucketExistV2(bucketName);
	        if (exists) existingBuckets.add(bucketName);
	        return exists;
	    }
	    return getConnection(bucketName).doesBucketExistV2(bucketName);
	}

/*	private boolean doesBucketExists(String bucketName) {
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
	}*/

	private String addBucketPrefix(String bucketName) {
	    if (!bucketName.startsWith(bucketNamePrefix)) {
	        String prefixed = bucketNamePrefix + bucketName;
	        LOGGER.debug("Adding prefix to bucket name: {}", prefixed);
	        return prefixed;
	    }
	    LOGGER.debug("Bucket name already has prefix: {}", bucketName);
	    return bucketName;
	}
	
/*	private String addBucketPrefix(String bucketName) {
		if (bucketName.startsWith(bucketNamePrefix)) {
			LOGGER.debug("Already bucketName with prefix is present" + bucketName);
			return bucketName;
		} else {
			bucketName = bucketNamePrefix + bucketName;
			LOGGER.debug("Adding  Prefix to bucketName" + bucketName);
			return bucketName;
		}
	}*/
}