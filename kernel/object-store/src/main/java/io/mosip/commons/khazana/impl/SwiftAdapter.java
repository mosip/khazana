package io.mosip.commons.khazana.impl;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import io.mosip.commons.khazana.dto.ObjectDto;
import io.mosip.commons.khazana.spi.ObjectStoreAdapter;

/**
 * OpenStack Swift implementation of {@link ObjectStoreAdapter}, selected with
 * {@code @Qualifier("SwiftAdapter")}.
 * <p>
 * Swift adapter has not been tested.
 * Accounts are cached in {@link #accounts}. Object bytes and metadata use JOSS
 * {@link StoredObject}. Tags are container metadata. {@code incMetadata},
 * {@code decMetadata}, {@code removeContainer}, {@code pack}, and {@code getAllObjects}
 * are not implemented. {@code listObjectsByPrefix} and {@code moveObject} use the SPI defaults.
 * <p>
 * The {@code @Value} fields below are literal strings, not {@code ${...}} placeholders,
 * so they do not read Spring properties.
 */
@Service
@Qualifier("SwiftAdapter")
public class SwiftAdapter implements ObjectStoreAdapter {

    /**
     * SLF4J logger for this adapter. Declared for Swift diagnostics; current methods do not log.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(SwiftAdapter.class);


    /**
     * Swift user name. {@code @Value} is the literal {@code object.store.swift.username:test},
     * not a {@code ${...}} placeholder, so the field is that whole string.
     * Intended property key {@code object.store.swift.username}, intended default {@code test}.
     */
    @Value("object.store.swift.username:test")
    private String userName;

    /**
     * Swift password. {@code @Value} is the literal {@code object.store.swift.password:test},
     * not a {@code ${...}} placeholder, so the field is that whole string.
     * Intended property key {@code object.store.swift.password}, intended default {@code test}.
     */
    @Value("object.store.swift.password:test")
    private String password;

    /**
     * Swift auth URL. {@code @Value} is the literal {@code object.store.swift.url:null},
     * not a {@code ${...}} placeholder, so the field is that whole string.
     * Intended property key {@code object.store.swift.url}, intended default {@code null}.
     */
    @Value("object.store.swift.url:null")
    private String authUrl;

    /**
     * Authenticated JOSS accounts keyed by tenant (account) name.
     */
    private Map<String, Account> accounts = new HashMap<>();


    /**
     * Downloads {@code objectName} from the Swift container.
     * <p>
     * Creates the container when it does not exist. {@code source} and {@code process}
     * are not applied to the object name.
     *
     * @param account       Swift tenant name
     * @param containerName Swift container
     * @param source        unused path segment
     * @param process       unused path segment
     * @param objectName    stored object name
     * @return object contents
     */
    public InputStream getObject(String account, String containerName, String source, String process, String objectName) {
        Container container = getConnection(account).getContainer(containerName);
        if (!container.exists())
            container = getConnection(account).getContainer(containerName).create();
        return container.getObject(objectName).downloadObjectAsInputStream();
    }

    /**
     * Uploads {@code data} as {@code objectName}.
     * <p>
     * Creates the container when it does not exist. {@code source} and {@code process}
     * are not applied to the object name.
     *
     * @param account       Swift tenant name
     * @param containerName Swift container
     * @param source        unused path segment
     * @param process       unused path segment
     * @param objectName    stored object name
     * @param data          object contents
     * @return {@code true} after the upload call
     */
    public boolean putObject(String account, String containerName, String source, String process, String objectName, InputStream data) {
        Container container = getConnection(account).getContainer(containerName);
        if (!container.exists())
            container = getConnection(account).getContainer(containerName).create();
        StoredObject storedObject = container.getObject(objectName);
        storedObject.uploadObject(data);

        return true;
    }

    /**
     * Reports whether the container and object exist.
     *
     * @param account       Swift tenant name
     * @param containerName Swift container
     * @param source        unused path segment
     * @param process       unused path segment
     * @param objectName    stored object name
     * @return {@code true} when both the container and the object exist
     */
    public boolean exists(String account, String containerName, String source, String process, String objectName) {
        Container container = getConnection(account).getContainer(containerName);
        return container.exists() && container.getObject(objectName).exists();
    }

    /**
     * Replaces object metadata with {@code metadata}.
     *
     * @param account       Swift tenant name
     * @param containerName Swift container
     * @param source        unused path segment
     * @param process       unused path segment
     * @param objectName    stored object name
     * @param metadata      metadata to save
     * @return {@code metadata}, or {@code null} when the container does not exist
     */
    public Map<String, Object> addObjectMetaData(String account, String containerName, String source, String process, String objectName, Map<String, Object> metadata) {

        Container container = getConnection(account).getContainer(containerName);
        if (!container.exists())
            return null;
        StoredObject storedObject = container.getObject(objectName);
        storedObject.setMetadata(metadata);
        storedObject.saveMetadata();
        return metadata;
    }

    /**
     * Adds one metadata entry to the object, keeping existing entries.
     *
     * @param account       Swift tenant name
     * @param containerName Swift container
     * @param source        unused path segment
     * @param process       unused path segment
     * @param objectName    stored object name
     * @param key           metadata key
     * @param value         metadata value
     * @return metadata after the update, or {@code null} when the container does not exist
     */
    public Map<String, Object> addObjectMetaData(String account, String containerName, String source, String process, String objectName, String key, String value) {
        Container container = getConnection(account).getContainer(containerName);
        if (!container.exists())
            return null;
        StoredObject storedObject = container.getObject(objectName);
        storedObject.getMetadata();
        Map<String, Object> existingMetadata = storedObject.getMetadata();
        existingMetadata.put(key, value);
        storedObject.setMetadata(existingMetadata);
        storedObject.saveMetadata();
        return existingMetadata;
    }

    /**
     * Reads object metadata.
     * <p>
     * When {@code objectName} is {@code null}, every object in the container is listed
     * and its metadata is keyed by object name.
     *
     * @param account       Swift tenant name
     * @param containerName Swift container
     * @param source        unused path segment
     * @param process       unused path segment
     * @param objectName    stored object name, or {@code null} to list the container
     * @return metadata keyed by object name, or {@code null} when the container does not exist
     */
    public Map<String, Object> getMetaData(String account, String containerName, String source, String process, String objectName) {
        Map<String, Object> metaData = new HashMap<>();
        Container container = getConnection(account).getContainer(containerName);
        if (!container.exists())
            return null;
        if (objectName == null)
            container.list().forEach(obj -> metaData.put(obj.getName(), obj.getMetadata()));
        else {
            StoredObject storedObject = container.getObject(objectName);
            metaData.put(storedObject.getName(), storedObject.getMetadata());
        }
        return metaData;
    }

    /**
     * Returns a cached JOSS account or authenticates with BASIC auth and caches it.
     *
     * @param accountName Swift tenant name
     * @return authenticated account
     */
    private Account getConnection(String accountName) {
        if (!accounts.isEmpty() && accounts.get(accountName) != null)
            return accounts.get(accountName);

        AccountConfig config = new AccountConfig();
        config.setUsername(userName);
        config.setPassword(password);
        config.setAuthUrl(authUrl);
        config.setTenantName(accountName);
        config.setAuthenticationMethod(AuthenticationMethod.BASIC);
        Account account = new AccountFactory(config).setAllowReauthenticate(true).createAccount();
        accounts.put(accountName, account);
        return account;
    }

    /**
     * Not implemented. Always returns {@code 0}.
     *
     * @param account     Swift tenant name
     * @param container   Swift container
     * @param source      unused path segment
     * @param process     unused path segment
     * @param objectName  stored object name
     * @param metaDataKey metadata key that would be incremented
     * @return {@code 0}
     */
    @Override
    public Integer incMetadata(String account, String container, String source, String process, String objectName, String metaDataKey) {
        // TODO Auto-generated method stub
        return 0;
    }

    /**
     * Not implemented. Always returns {@code 0}.
     *
     * @param account     Swift tenant name
     * @param container   Swift container
     * @param source      unused path segment
     * @param process     unused path segment
     * @param objectName  stored object name
     * @param metaDataKey metadata key that would be decremented
     * @return {@code 0}
     */
    @Override
    public Integer decMetadata(String account, String container, String source, String process, String objectName, String metaDataKey) {
        // TODO Auto-generated method stub
        return 0;
    }

    /**
     * Not implemented. Returns {@code true} without deleting the object.
     *
     * @param account    Swift tenant name
     * @param container  Swift container
     * @param source     unused path segment
     * @param process    unused path segment
     * @param objectName stored object name
     * @return {@code true}
     */
    @Override
    public boolean deleteObject(String account, String container, String source, String process, String objectName) {
        return true;
    }

    /**
     * Not Supported in SwiftAdapter
     *
     * @param account   Swift tenant name
     * @param container Swift container
     * @param source    unused path segment
     * @param process   unused path segment
     * @return {@code false}
     */
    @Override
    public boolean removeContainer(String account, String container, String source, String process) {
        return false;
    }

    /**
     * Not Supported in SwiftAdapter. Does not encrypt or rewrite the container.
     *
     * @param account   Swift tenant name
     * @param container Swift container
     * @param source    unused path segment
     * @param process   unused path segment
     * @param refId     reference id of the encryption key; ignored
     * @return {@code false}
     */
    @Override
    public boolean pack(String account, String container, String source, String process, String refId) {
        return false;
    }

	/**
	 * Merges {@code tags} into container metadata.
	 * <p>
	 * Creates the container when it does not exist. Existing tags are kept.
	 *
	 * @param account       Swift tenant name
	 * @param containerName Swift container
	 * @param tags          tags to add
	 * @return {@code tags}
	 */
	@Override
	public Map<String, String> addTags(String account, String containerName, Map<String, String> tags) {
		Map<String, Object> tagMap = new HashMap<>();
		Container container = getConnection(account).getContainer(containerName);
		 if (!container.exists())
	            container = getConnection(account).getContainer(containerName).create();
		Map<String, String> existingTags = getTags(account, containerName);
		existingTags.entrySet().forEach(m -> tagMap.put(m.getKey(), m.getValue()));
		tags.entrySet().stream().forEach(m -> tagMap.put(m.getKey(), m.getValue()));
		container.setMetadata(tagMap);
		container.saveMetadata();
		return tags;
	}

	/**
	 * Reads container metadata as tags.
	 * <p>
	 * Creates the container when it does not exist.
	 *
	 * @param account       Swift tenant name
	 * @param containerName Swift container
	 * @return tag name to string value; empty when the container has no metadata
	 */
	@Override
	public Map<String, String> getTags(String account, String containerName) {
		Map<String, String> metaData = new HashMap<>();
		Container container = getConnection(account).getContainer(containerName);
		 if (!container.exists())
	            container = getConnection(account).getContainer(containerName).create();
		if (container.getMetadata() != null) {
			container.getMetadata().entrySet().stream().forEach(m -> metaData.put(m.getKey(), m.getValue().toString()));

		}

		return metaData;

	}

    /**
     * Not supported in swift adapter. Always returns {@code null}.
     *
     * @param account   Swift tenant name
     * @param container Swift container
     * @return {@code null}
     */
    public List<ObjectDto> getAllObjects(String account, String container) {
        return null;
    }

	/**
	 * Removes the named tags from container metadata and saves what remains.
	 * <p>
	 * Creates the container when it does not exist.
	 *
	 * @param account       Swift tenant name
	 * @param containerName Swift container
	 * @param tags          tag names to remove
	 */
	@Override
	public void deleteTags(String account, String containerName, List<String> tags) {
		Map<String, Object> tagMap = new HashMap<>();
		Container container = getConnection(account).getContainer(containerName);
		 if (!container.exists())
	            container = getConnection(account).getContainer(containerName).create();
		Map<String, String> existingTags = getTags(account, containerName);
		tags.forEach(m -> existingTags.remove(m));
		existingTags.entrySet().forEach(m -> tagMap.put(m.getKey(), m.getValue()));
		container.setMetadata(tagMap);
		container.saveMetadata();

	}
}
