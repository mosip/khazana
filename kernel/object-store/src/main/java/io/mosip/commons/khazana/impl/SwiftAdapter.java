package io.mosip.commons.khazana.impl;

import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
 * Swift adapter has not been tested.
 */
@Service
@Qualifier("SwiftAdapter")
public class SwiftAdapter implements ObjectStoreAdapter {

	private static final Logger LOGGER = LoggerFactory.getLogger(SwiftAdapter.class);

	@Value("object.store.swift.username:test")
	private String userName;

	@Value("object.store.swift.password:test")
	private String password;

	@Value("object.store.swift.url:null")
	private String authUrl;

	private Map<String, Account> accounts = new ConcurrentHashMap<>();

	@Override
	public InputStream getObject(String account, String containerName, String source, String process,
			String objectName) {
		if (objectName == null || objectName.trim().isEmpty()) {
			LOGGER.warn("Object name is null or empty. Cannot download object.");
			return null;
		}

		Container container = getConnection(account).getContainer(containerName);

		if (!container.exists()) {
			LOGGER.warn("Container '{}' does not exist for account '{}'. Cannot download object.", containerName,
					account);
			return null;
		}

		StoredObject storedObject = container.getObject(objectName);

		if (!storedObject.exists()) {
			LOGGER.warn("Object '{}' does not exist in container '{}'. Cannot download.", objectName, containerName);
			return null;
		}

		try {
			LOGGER.info("Downloading object '{}' from container '{}'", objectName, containerName);
			return storedObject.downloadObjectAsInputStream();
		} catch (Exception e) {
			LOGGER.error("Failed to download object '{}' from container '{}'", objectName, containerName, e);
			return null;
		}
	}

	/*
	 * public InputStream getObject(String account, String containerName, String
	 * source, String process, String objectName) { Container container =
	 * getConnection(account).getContainer(containerName); if (!container.exists())
	 * container = getConnection(account).getContainer(containerName).create();
	 * return container.getObject(objectName).downloadObjectAsInputStream(); }
	 */

	@Override
	public boolean putObject(String account, String containerName, String source, String process, String objectName,
			InputStream data) {
		if (objectName == null || objectName.trim().isEmpty()) {
			LOGGER.warn("Object name is null or empty. Skipping upload.");
			return false;
		}

		if (data == null) {
			LOGGER.warn("Input stream is null for object '{}'. Skipping upload.", objectName);
			return false;
		}

		try {
			Account swiftAccount = getConnection(account);
			Container container = swiftAccount.getContainer(containerName);

			if (!container.exists()) {
				LOGGER.debug("Container '{}' does not exist. Creating it.", containerName);
				container = container.create();
			}

			StoredObject storedObject = container.getObject(objectName);
			storedObject.uploadObject(data);

			LOGGER.debug("Successfully uploaded object '{}' to container '{}'", objectName, containerName);
			return true;

		} catch (Exception e) {
			LOGGER.error("Failed to upload object '{}' to container '{}'", objectName, containerName, e);
			return false;
		}
	}

	/*
	 * public boolean putObject(String account, String containerName, String source,
	 * String process, String objectName, InputStream data) { Container container =
	 * getConnection(account).getContainer(containerName); if (!container.exists())
	 * container = getConnection(account).getContainer(containerName).create();
	 * StoredObject storedObject = container.getObject(objectName);
	 * storedObject.uploadObject(data);
	 * 
	 * return true; }
	 */

	@Override
	public boolean exists(String account, String containerName, String source, String process, String objectName) {
		if (objectName == null || objectName.trim().isEmpty()) {
			LOGGER.warn("Object name is null or empty. Cannot check existence.");
			return false;
		}

		Container container = getConnection(account).getContainer(containerName);
		if (!container.exists()) {
			LOGGER.info("Container '{}' does not exist in account '{}'", containerName, account);
			return false;
		}

		boolean objectExists = container.getObject(objectName).exists();

		LOGGER.debug("Checked existence for object '{}' in container '{}': {}", objectName, containerName,
				objectExists);

		return objectExists;
	}

	/*
	 * public boolean exists(String account, String containerName, String source,
	 * String process, String objectName) { Container container =
	 * getConnection(account).getContainer(containerName); return container.exists()
	 * && container.getObject(objectName).exists(); }
	 */

	@Override
	public Map<String, Object> addObjectMetaData(String account, String containerName, String source, String process,
			String objectName, Map<String, Object> metadata) {
		if (metadata == null || metadata.isEmpty()) {
			LOGGER.warn("Empty or null metadata provided for object '{}'. Skipping metadata update.", objectName);
			return Collections.emptyMap();
		}

		Container container = getConnection(account).getContainer(containerName);
		if (!container.exists()) {
			LOGGER.warn("Container '{}' does not exist for account '{}'. Cannot add metadata.", containerName, account);
			return Collections.emptyMap();
		}

		StoredObject storedObject = container.getObject(objectName);
		if (!storedObject.exists()) {
			LOGGER.warn("Object '{}' does not exist in container '{}'. Cannot add metadata.", objectName,
					containerName);
			return Collections.emptyMap();
		}

		storedObject.setMetadata(metadata);
		storedObject.saveMetadata();

		LOGGER.info("Added metadata to object '{}' in container '{}': {}", objectName, containerName, metadata);

		return metadata;
	}

	/*
	 * public Map<String, Object> addObjectMetaData(String account, String
	 * containerName, String source, String process, String objectName, Map<String,
	 * Object> metadata) {
	 * 
	 * Container container = getConnection(account).getContainer(containerName); if
	 * (!container.exists()) return null; StoredObject storedObject =
	 * container.getObject(objectName); storedObject.setMetadata(metadata);
	 * storedObject.saveMetadata(); return metadata; }
	 */

	@Override
	public Map<String, Object> addObjectMetaData(String account, String containerName, String source, String process,
			String objectName, String key, String value) {
		Container container = getConnection(account).getContainer(containerName);

		if (!container.exists()) {
			LOGGER.warn("Container '{}' does not exist for account '{}'. Cannot add metadata.", containerName, account);
			return Collections.emptyMap();
		}

		StoredObject storedObject = container.getObject(objectName);
		if (!storedObject.exists()) {
			LOGGER.warn("Object '{}' does not exist in container '{}'. Cannot add metadata.", objectName,
					containerName);
			return Collections.emptyMap();
		}

		// Retrieve and update metadata
		Map<String, Object> existingMetadata = storedObject.getMetadata();
		existingMetadata.put(key, value);
		storedObject.setMetadata(existingMetadata);
		storedObject.saveMetadata();

		LOGGER.info("Added metadata '{}' = '{}' to object '{}' in container '{}'", key, value, objectName,
				containerName);

		return existingMetadata;
	}

	/*
	 * public Map<String, Object> addObjectMetaData(String account, String
	 * containerName, String source, String process, String objectName, String key,
	 * String value) { Container container =
	 * getConnection(account).getContainer(containerName); if (!container.exists())
	 * return null; StoredObject storedObject = container.getObject(objectName);
	 * storedObject.getMetadata(); Map<String, Object> existingMetadata =
	 * storedObject.getMetadata(); existingMetadata.put(key, value);
	 * storedObject.setMetadata(existingMetadata); storedObject.saveMetadata();
	 * return existingMetadata; }
	 */

	@Override
	public Map<String, Object> getMetaData(String account, String containerName, String source, String process,
			String objectName) {
		Map<String, Object> metaData = new HashMap<>();
		Container container = getConnection(account).getContainer(containerName);

		if (!container.exists()) {
			LOGGER.warn("Container '{}' does not exist in account '{}'", containerName, account);
			return Collections.emptyMap();
		}

		if (objectName == null) {
			for (StoredObject obj : container.list()) {
				metaData.put(obj.getName(), obj.getMetadata());
			}
		} else {
			StoredObject storedObject = container.getObject(objectName);
			if (!storedObject.exists()) {
				LOGGER.warn("Object '{}' does not exist in container '{}'", objectName, containerName);
				return Collections.emptyMap();
			}
			metaData.put(storedObject.getName(), storedObject.getMetadata());
		}

		return metaData;
	}

	/*
	 * public Map<String, Object> getMetaData(String account, String containerName,
	 * String source, String process, String objectName) { Map<String, Object>
	 * metaData = new HashMap<>(); Container container =
	 * getConnection(account).getContainer(containerName); if (!container.exists())
	 * return null; if (objectName == null) container.list().forEach(obj ->
	 * metaData.put(obj.getName(), obj.getMetadata())); else { StoredObject
	 * storedObject = container.getObject(objectName);
	 * metaData.put(storedObject.getName(), storedObject.getMetadata()); } return
	 * metaData; }
	 */
	private Account getConnection(String accountName) {
		// Check and return cached connection if available
		if (accounts.containsKey(accountName)) {
			return accounts.get(accountName);
		}

		synchronized (this) {
			// Double-check locking to prevent race condition
			if (accounts.containsKey(accountName)) {
				return accounts.get(accountName);
			}

			AccountConfig config = new AccountConfig();
			config.setUsername(userName);
			config.setPassword(password);
			config.setAuthUrl(authUrl);
			config.setTenantName(accountName);
			config.setAuthenticationMethod(AuthenticationMethod.BASIC);

			Account account = new AccountFactory(config).setAllowReauthenticate(true).createAccount();

			accounts.put(accountName, account);

			LOGGER.debug("Created new Swift account connection for tenant '{}'", accountName);

			return account;
		}
	}
	/*
	 * private Account getConnection(String accountName) { if (!accounts.isEmpty()
	 * && accounts.get(accountName) != null) return accounts.get(accountName);
	 * 
	 * AccountConfig config = new AccountConfig(); config.setUsername(userName);
	 * config.setPassword(password); config.setAuthUrl(authUrl);
	 * config.setTenantName(accountName);
	 * config.setAuthenticationMethod(AuthenticationMethod.BASIC); Account account =
	 * new AccountFactory(config).setAllowReauthenticate(true).createAccount();
	 * accounts.put(accountName, account); return account; }
	 */

	@Override
	public Integer incMetadata(String account, String container, String source, String process, String objectName,
			String metaDataKey) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Integer decMetadata(String account, String container, String source, String process, String objectName,
			String metaDataKey) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean deleteObject(String account, String container, String source, String process, String objectName) {
		return true;
	}

	/**
	 * Not Supported in SwiftAdapter
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
	 * Not Supported in SwiftAdapter
	 *
	 * @param account
	 * @param container
	 * @param source
	 * @param process
	 * @param objectName
	 * @param data
	 * @return
	 */
	@Override
	public boolean pack(String account, String container, String source, String process, String refId) {
		return false;
	}

	@Override
	public Map<String, String> addTags(String account, String containerName, Map<String, String> tags) {
		Account swiftAccount = getConnection(account);
		Container container = swiftAccount.getContainer(containerName);

		if (!container.exists()) {
			LOGGER.info("Container '{}' does not exist. Creating new container.", containerName);
			container = container.create();
		}

		// Retrieve existing tags
		Map<String, String> existingTags = getTags(account, containerName);

		// Merge new tags into existing tags
		existingTags.putAll(tags);

		// Convert to metadata format
		Map<String, Object> metadataToSave = new HashMap<>();
		for (Map.Entry<String, String> entry : existingTags.entrySet()) {
			metadataToSave.put(entry.getKey(), entry.getValue());
		}

		// Save metadata
		container.setMetadata(metadataToSave);
		container.saveMetadata();

		return existingTags; // Return the full merged tag set
	}

	/*
	 * @Override public Map<String, String> addTags(String account, String
	 * containerName, Map<String, String> tags) { Map<String, Object> tagMap = new
	 * HashMap<>(); Container container =
	 * getConnection(account).getContainer(containerName); if (!container.exists())
	 * container = getConnection(account).getContainer(containerName).create();
	 * Map<String, String> existingTags = getTags(account, containerName);
	 * existingTags.entrySet().forEach(m -> tagMap.put(m.getKey(), m.getValue()));
	 * tags.entrySet().stream().forEach(m -> tagMap.put(m.getKey(), m.getValue()));
	 * container.setMetadata(tagMap); container.saveMetadata(); return tags; }
	 */

	@Override
	public Map<String, String> getTags(String account, String containerName) {
		Map<String, String> metaData = new HashMap<>();
		Container container = getConnection(account).getContainer(containerName);

		// Avoid creating container
		if (!container.exists()) {
			LOGGER.warn("Container '{}' does not exist. Returning empty metadata.", containerName);
			container = container.create();
		}

		Map<String, Object> rawMetadata = container.getMetadata();
		if (rawMetadata != null) {
			for (Map.Entry<String, Object> entry : rawMetadata.entrySet()) {
				metaData.put(entry.getKey(), entry.getValue().toString());
			}
		}

		return metaData;
	}

	/*
	 * @Override public Map<String, String> getTags(String account, String
	 * containerName) { Map<String, String> metaData = new HashMap<>(); Container
	 * container = getConnection(account).getContainer(containerName); if
	 * (!container.exists()) container =
	 * getConnection(account).getContainer(containerName).create(); if
	 * (container.getMetadata() != null) {
	 * container.getMetadata().entrySet().stream().forEach(m ->
	 * metaData.put(m.getKey(), m.getValue().toString())); }
	 * 
	 * return metaData; }
	 */

	/**
	 * Not supported in swift adapter
	 *
	 * @param account
	 * @param container
	 * @return
	 */
	public List<ObjectDto> getAllObjects(String account, String container) {
		return null;
	}

	@Override
	public void deleteTags(String account, String containerName, List<String> tags) {
		Container container = getConnection(account).getContainer(containerName);

		// Avoid creating container on delete
		if (!container.exists()) {
			LOGGER.warn("Container {} does not exist. Skipping tag deletion.", containerName);
			return;
		}

		Map<String, String> existingTags = getTags(account, containerName);

		// Remove the tags
		for (String tagKey : tags) {
			existingTags.remove(tagKey);
		}

		// Convert updated map back to Object map for metadata
		Map<String, Object> updatedMetadata = new HashMap<>();
		for (Map.Entry<String, String> entry : existingTags.entrySet()) {
			updatedMetadata.put(entry.getKey(), entry.getValue());
		}

		container.setMetadata(updatedMetadata);
		container.saveMetadata();
	}

	/*
	 * @Override public void deleteTags(String account, String containerName,
	 * List<String> tags) { Map<String, Object> tagMap = new HashMap<>(); Container
	 * container = getConnection(account).getContainer(containerName); if
	 * (!container.exists()) container =
	 * getConnection(account).getContainer(containerName).create(); Map<String,
	 * String> existingTags = getTags(account, containerName); tags.forEach(m ->
	 * existingTags.remove(m)); existingTags.entrySet().forEach(m ->
	 * tagMap.put(m.getKey(), m.getValue())); container.setMetadata(tagMap);
	 * container.saveMetadata();
	 * 
	 * }
	 */
}