package io.mosip.commons.khazana.impl;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.IOUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.mosip.commons.khazana.constant.KhazanaErrorCodes;
import io.mosip.commons.khazana.dto.ObjectDto;
import io.mosip.commons.khazana.exception.FileNotFoundInDestinationException;
import io.mosip.commons.khazana.spi.ObjectStoreAdapter;
import io.mosip.commons.khazana.util.EncryptionHelper;
import io.mosip.commons.khazana.util.ObjectStoreUtil;
import io.mosip.kernel.core.util.FileUtils;

@Service
@Qualifier("PosixAdapter")
public class PosixAdapter implements ObjectStoreAdapter {

	private static final Logger LOGGER = LoggerFactory.getLogger(PosixAdapter.class);
	private static final String SEPARATOR = File.separator;
	private static final String ZIP = ".zip";
	private static final String JSON = ".json";
	private static final String TAGS = "_tags";
	private static final String TMP = ".tmp";
	private static final String BAK = ".bak";

	@Autowired
	private ObjectMapper objectMapper;
	@Value("${object.store.base.location:home}")
	private String baseLocation;

	@Autowired
	private EncryptionHelper helper;

	public InputStream getObject(String account, String container, String source, String process, String objectName) {
		File containerZip = new File(baseLocation + File.separator + account + File.separator + container + ZIP);

		if (!containerZip.exists()) {
			LOGGER.error("Container file not found: {}", containerZip.getPath());
			throw new FileNotFoundInDestinationException(
					KhazanaErrorCodes.CONTAINER_NOT_PRESENT_IN_DESTINATION.getErrorCode(),
					KhazanaErrorCodes.CONTAINER_NOT_PRESENT_IN_DESTINATION.getErrorMessage());
		}

		String targetEntryName = ObjectStoreUtil.getName(source, process, objectName) + ZIP;

		try (java.util.zip.ZipFile zipFile = new java.util.zip.ZipFile(containerZip)) {
			ZipEntry entry = zipFile.stream().filter(e -> e.getName().equals(targetEntryName)).findFirst().orElse(null);

			if (entry == null) {
				LOGGER.warn("Entry not found in container: {}", targetEntryName);
				return null;
			}

			try (InputStream entryStream = zipFile.getInputStream(entry)) {
				// Copy entry stream to byte array to allow re-use outside ZipFile lifecycle
				byte[] data = IOUtils.toByteArray(entryStream);
				return new ByteArrayInputStream(data); // safe to return after ZipFile is closed
			}
		} catch (IOException e) {
			LOGGER.error("Error reading object '{}' from container '{}'", targetEntryName, container, e);
		}

		return null;
	}

	/*
	 * public InputStream getObject(String account, String container, String source,
	 * String process, String objectName) { try { File accountLoc = new
	 * File(baseLocation + SEPARATOR + account); if (!accountLoc.exists()) return
	 * null; File containerZip = new File(accountLoc.getPath() + SEPARATOR +
	 * container + ZIP); if (!containerZip.exists()) throw new
	 * FileNotFoundInDestinationException(
	 * KhazanaErrorCodes.CONTAINER_NOT_PRESENT_IN_DESTINATION.getErrorCode(),
	 * KhazanaErrorCodes.CONTAINER_NOT_PRESENT_IN_DESTINATION.getErrorMessage());
	 * 
	 * InputStream ios = new FileInputStream(containerZip); Map<ZipEntry,
	 * ByteArrayOutputStream> entries = getAllExistingEntries(ios);
	 * 
	 * Optional<ZipEntry> zipEntry = entries.keySet().stream() .filter(e ->
	 * e.getName().contains(ObjectStoreUtil.getName(source, process, objectName) +
	 * ZIP)) .findAny();
	 * 
	 * if (zipEntry.isPresent() && zipEntry.get() != null) return new
	 * ByteArrayInputStream(entries.get(zipEntry.get()).toByteArray());
	 * 
	 * } catch (FileNotFoundInDestinationException e) {
	 * LOGGER.error("exception occured to get object for id - " + container, e); }
	 * catch (IOException e) {
	 * LOGGER.error("exception occured to get object for id - " + container, e); }
	 * return null; }
	 */

	public boolean exists(String account, String container, String source, String process, String objectName) {
		File containerZip = new File(baseLocation + File.separator + account + File.separator + container + ZIP);
		if (!containerZip.exists()) {
			return false;
		}

		String targetEntryName = ObjectStoreUtil.getName(source, process, objectName) + ZIP;

		try (java.util.zip.ZipFile zipFile = new java.util.zip.ZipFile(containerZip)) {
			return zipFile.stream().anyMatch(entry -> entry.getName().equals(targetEntryName));
		} catch (IOException e) {
			LOGGER.error("Error checking existence of entry '{}' in '{}'", targetEntryName, containerZip.getPath(), e);
			return false;
		}
	}

	/*
	 * public boolean exists(String account, String container, String source, String
	 * process, String objectName) { return getObject(account, container, source,
	 * process, objectName) != null; }
	 */

	public boolean putObject(String account, String container, String source, String process, String objectName,
			InputStream data) {
		try {
			createContainerZipWithSubpacket(account, container, source, process, objectName + ZIP, data);
			return true;
		} catch (Exception e) {
			LOGGER.error("exception occured. Will create a new connection.", e);
		}
		return false;
	}

	public Map<String, Object> addObjectMetaData(String account, String container, String source, String process,
			String objectName, Map<String, Object> metadata) {
		String metadataEntryName = objectName + JSON;
		try {
			JSONObject jsonObject = mergeMetadata(account, container, source, process, metadataEntryName, metadata);
			byte[] jsonBytes = jsonObject.toString().getBytes(StandardCharsets.UTF_8);
			InputStream jsonStream = new ByteArrayInputStream(jsonBytes);

			createContainerZipWithSubpacket(account, container, source, process, metadataEntryName, jsonStream);

			return metadata;

		} catch (Exception e) {
			LOGGER.error("Error adding metadata for object '{}' in container '{}'", objectName, container, e);
			return Collections.emptyMap(); // or return metadata if partial success is allowed
		}
	}

	private JSONObject mergeMetadata(String account, String container, String source, String process, String objectName,
			Map<String, Object> newMetadata) {

		JSONObject merged = new JSONObject();
		try {
			Map<String, Object> existingMeta = getMetaData(account, container, source, process, objectName);

			// Start with existing metadata
			if (existingMeta != null) {
				for (Map.Entry<String, Object> entry : existingMeta.entrySet()) {
					merged.put(entry.getKey(), entry.getValue());
				}
			}

			// Override with new metadata
			for (Map.Entry<String, Object> entry : newMetadata.entrySet()) {
				merged.put(entry.getKey(), entry.getValue());
			}

		} catch (JSONException e) {
			LOGGER.error("Error merging metadata for object '{}' in container '{}'", objectName, container, e);
		}
		return merged;
	}

	/*
	 * public Map<String, Object> addObjectMetaData(String account, String
	 * container, String source, String process, String objectName, Map<String,
	 * Object> metadata) { try { JSONObject jsonObject = objectMetadata(account,
	 * container, source, process, objectName, metadata);
	 * createContainerZipWithSubpacket(account, container, source, process,
	 * objectName + JSON, new
	 * ByteArrayInputStream(jsonObject.toString().getBytes())); } catch
	 * (io.mosip.kernel.core.exception.IOException | IOException e) {
	 * LOGGER.error("exception occured to add metadata for id - " + container, e); }
	 * return metadata; }
	 */

	public Map<String, Object> addObjectMetaData(String account, String container, String source, String process,
			String objectName, String key, String value) {
		try {
			Map<String, Object> metaMap = new HashMap<>();
			metaMap.put(key, value);
			JSONObject jsonObject = objectMetadata(account, container, source, process, objectName, metaMap);
			createContainerZipWithSubpacket(account, container, source, process, objectName + JSON,
					new ByteArrayInputStream(jsonObject.toString().getBytes()));
			return metaMap;
		} catch (io.mosip.kernel.core.exception.IOException e) {
			LOGGER.error("exception occured to add metadata for id - " + container, e);
		} catch (IOException e) {
			LOGGER.error("exception occured to add metadata for id - " + container, e);
		}
		return null;
	}

	public Map<String, Object> getMetaData(String account, String container, String source, String process,
			String objectName) {
		Map<String, Object> metaMap = new HashMap<>();
		String entryName = ObjectStoreUtil.getName(source, process, objectName) + JSON;
		File containerZip = new File(baseLocation + File.separator + account + SEPARATOR + container + ZIP);

		if (!containerZip.exists()) {
			throw new FileNotFoundInDestinationException(
					KhazanaErrorCodes.CONTAINER_NOT_PRESENT_IN_DESTINATION.getErrorCode(),
					KhazanaErrorCodes.CONTAINER_NOT_PRESENT_IN_DESTINATION.getErrorMessage());
		}

		try (ZipFile zipFile = new ZipFile(containerZip)) {
			ZipEntry entry = zipFile.getEntry(entryName);
			if (entry == null) {
				LOGGER.info("Metadata entry '{}' not found in container '{}'", entryName, containerZip.getName());
				return metaMap; // return empty map instead of null
			}

			try (InputStream is = zipFile.getInputStream(entry)) {
				metaMap = objectMapper.readValue(is, new TypeReference<Map<String, Object>>() {});
			}

		} catch (IOException e) {
			LOGGER.error("Error reading metadata '{}' from container '{}'", entryName, containerZip.getName(), e);
		}

		return metaMap;
	}

	/*
	 * public Map<String, Object> getMetaData(String account, String container,
	 * String source, String process, String objectName) { Map<String, Object>
	 * metaMap = null; try { File accountLoc = new File(baseLocation + SEPARATOR +
	 * account); if (!accountLoc.exists()) return null; File containerZip = new
	 * File(accountLoc.getPath() + SEPARATOR + container + ZIP); if
	 * (!containerZip.exists()) throw new FileNotFoundInDestinationException(
	 * KhazanaErrorCodes.CONTAINER_NOT_PRESENT_IN_DESTINATION.getErrorCode(),
	 * KhazanaErrorCodes.CONTAINER_NOT_PRESENT_IN_DESTINATION.getErrorMessage());
	 * 
	 * InputStream ios = new FileInputStream(containerZip); Map<ZipEntry,
	 * ByteArrayOutputStream> entries = getAllExistingEntries(ios);
	 * 
	 * Optional<ZipEntry> zipEntry = entries.keySet().stream().filter(e ->
	 * e.getName().contains(objectName + JSON)) .findAny();
	 * 
	 * if (zipEntry.isPresent() && zipEntry.get() != null) { String string =
	 * entries.get(zipEntry.get()).toString(); JSONObject jsonObject =
	 * objectMapper.readValue(objectMapper.writeValueAsString(string),
	 * JSONObject.class); metaMap = objectMapper.readValue(jsonObject.toString(),
	 * HashMap.class); } } catch (FileNotFoundInDestinationException e) {
	 * LOGGER.error("exception occured. Will create a new connection.", e); throw e;
	 * } catch (IOException e) {
	 * LOGGER.error("exception occured to get metadata for id - " + container, e); }
	 * return metaMap; }
	 */

	private void createContainerZipWithSubpacket(String account, String container, String source, String process,
			String objectName, InputStream data) throws io.mosip.kernel.core.exception.IOException, IOException {

		File containerZip = getContainerZipFile(account, container);
		byte[] newData = IOUtils.toByteArray(data);

		if (!containerZip.exists()) {
			writeNewContainerZip(containerZip, source, process, objectName, newData);
		} else {
			updateExistingContainerZip(containerZip, source, process, objectName, newData);
		}
	}

	private void writeNewContainerZip(File containerZip, String source, String process, String objectName, byte[] data)
			throws IOException {
		try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(containerZip))) {
			addEntryToZip(ObjectStoreUtil.getName(source, process, objectName), data, zos);
		}
	}

	private void addEntryToZip(String entryName, byte[] data, ZipOutputStream zipOutputStream) throws IOException {
		ZipEntry zipEntry = new ZipEntry(entryName);
		zipOutputStream.putNextEntry(zipEntry);
		zipOutputStream.write(data);
		zipOutputStream.closeEntry();
	}

	private void updateExistingContainerZip(File containerZip, String source, String process, String objectName,
			byte[] newData) throws IOException {

		String entryName = ObjectStoreUtil.getName(source, process, objectName);
		Map<String, byte[]> existingEntries = extractEntriesExcept(containerZip, entryName);

		try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(containerZip))) {
			for (Map.Entry<String, byte[]> entry : existingEntries.entrySet()) {
				zos.putNextEntry(new ZipEntry(entry.getKey()));
				zos.write(entry.getValue());
				zos.closeEntry();
			}
			// Add or overwrite the object
			zos.putNextEntry(new ZipEntry(entryName));
			zos.write(newData);
			zos.closeEntry();
		}
	}

	private Map<String, byte[]> extractEntriesExcept(File containerZip, String excludeEntryName) throws IOException {
		Map<String, byte[]> entries = new HashMap<>();
		try (ZipInputStream zis = new ZipInputStream(new FileInputStream(containerZip))) {
			ZipEntry entry;
			while ((entry = zis.getNextEntry()) != null) {
				if (!entry.getName().equals(excludeEntryName)) {
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					IOUtils.copy(zis, baos);
					entries.put(entry.getName(), baos.toByteArray());
				}
				zis.closeEntry();
			}
		}
		return entries;
	}

	/*
	 * private void createContainerZipWithSubpacket(String account, String
	 * container, String source, String process, String objectName, InputStream
	 * data) throws io.mosip.kernel.core.exception.IOException, IOException { File
	 * accountLocation = new File(baseLocation + SEPARATOR + account); if
	 * (!accountLocation.exists()) accountLocation.mkdir(); File containerZip = new
	 * File(accountLocation.getPath() + SEPARATOR + container + ZIP);
	 * ByteArrayOutputStream out = new ByteArrayOutputStream(); if
	 * (!containerZip.exists()) { try (ZipOutputStream packetZip = new
	 * ZipOutputStream(new BufferedOutputStream(out))) {
	 * addEntryToZip(String.format(objectName), IOUtils.toByteArray(data),
	 * packetZip, source, process); } } else { InputStream ios = new
	 * FileInputStream(containerZip); Map<ZipEntry, ByteArrayOutputStream> entries =
	 * getAllExistingEntries(ios); try (ZipOutputStream packetZip = new
	 * ZipOutputStream(out)) { entries.entrySet().forEach(e -> { try {
	 * packetZip.putNextEntry(e.getKey());
	 * packetZip.write(e.getValue().toByteArray()); } catch (IOException e1) {
	 * LOGGER.error("exception occured. Will create a new connection.", e1); } });
	 * addEntryToZip(String.format(objectName), IOUtils.toByteArray(data),
	 * packetZip, source, process); } }
	 * 
	 * FileUtils.copyToFile(new ByteArrayInputStream(out.toByteArray()),
	 * containerZip); }
	 */

	private File getContainerZipFile(String account, String container) {
		File accountLocation = new File(baseLocation + SEPARATOR + account);
		if (!accountLocation.exists()) {
			accountLocation.mkdirs();
		}
		return new File(accountLocation, container + ZIP);
	}

	/*
	 * private void addEntryToZip(String fileName, byte[] data, ZipOutputStream
	 * zipOutputStream, String source, String process) { try { if (data != null) {
	 * ZipEntry zipEntry = new ZipEntry(ObjectStoreUtil.getName(source, process,
	 * fileName)); zipOutputStream.putNextEntry(zipEntry);
	 * zipOutputStream.write(data); } } catch (IOException e) {
	 * LOGGER.error("exception occured. Will create a new connection.", e); } }
	 */

	private Map<String, ByteArrayOutputStream> getAllExistingEntries(InputStream packetStream) throws IOException {
		Map<String, ByteArrayOutputStream> entries = new HashMap<>();

		try (ZipInputStream zis = new ZipInputStream(new BufferedInputStream(packetStream))) {
			ZipEntry entry;
			while ((entry = zis.getNextEntry()) != null) {
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				try {
					IOUtils.copy(zis, out);
					entries.put(entry.getName(), out); // Use entry name (String) as key
				} finally {
					zis.closeEntry();
				}
			}
		}

		return entries;
	}

	/*
	 * private Map<ZipEntry, ByteArrayOutputStream>
	 * getAllExistingEntries(InputStream packetStream) throws IOException {
	 * Map<ZipEntry, ByteArrayOutputStream> entries = new HashMap<>(); try
	 * (ZipInputStream zis = new ZipInputStream(packetStream)) { ZipEntry ze =
	 * zis.getNextEntry(); while (ze != null) { int len; byte[] buffer = new
	 * byte[2048]; ByteArrayOutputStream out = new ByteArrayOutputStream(); while
	 * ((len = zis.read(buffer)) > 0) { out.write(buffer, 0, len); } entries.put(ze,
	 * out); zis.closeEntry(); ze = zis.getNextEntry(); out.close(); }
	 * zis.closeEntry(); } finally { packetStream.close(); } return entries; }
	 */

	private JSONObject objectMetadata(String account, String container, String source, String process,
			String objectName, Map<String, Object> newMetadata) {
		JSONObject merged = new JSONObject();

		try {
			// Start with existing metadata
			Map<String, Object> existingMetaData = getMetaData(account, container, source, process, objectName);
			if (existingMetaData != null) {
				for (Map.Entry<String, Object> entry : existingMetaData.entrySet()) {
					try {
						merged.put(entry.getKey(), entry.getValue());
					} catch (JSONException e) {
						LOGGER.error("Error adding existing metadata key '{}' to merged object", entry.getKey(), e);
					}
				}
			}

			// Overlay with new metadata (overwrites if keys clash)
			for (Map.Entry<String, Object> entry : newMetadata.entrySet()) {
				try {
					merged.put(entry.getKey(), entry.getValue());
				} catch (JSONException e) {
					LOGGER.error("Error adding new metadata key '{}' to merged object", entry.getKey(), e);
				}
			}

		} catch (Exception e) {
			LOGGER.error("Exception occurred while merging metadata for object '{}' in container '{}'", objectName,
					container, e);
		}

		return merged;
	}

	/*
	 * private JSONObject objectMetadata(String account, String container, String
	 * source, String process, String objectName, Map<String, Object> metadata) {
	 * JSONObject jsonObject = new JSONObject(metadata); Map<String, Object>
	 * existingMetaData = getMetaData(account, container, source, process,
	 * objectName); if (!CollectionUtils.isEmpty(existingMetaData))
	 * existingMetaData.entrySet().forEach(entry -> { try {
	 * jsonObject.put(entry.getKey(), entry.getValue()); } catch (JSONException e) {
	 * LOGGER.error("exception occured to add metadata for id - " + container, e); }
	 * }); return jsonObject; }
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

	@Override
	public boolean removeContainer(String account, String container, String source, String process) {
		File containerZip = new File(baseLocation + SEPARATOR + account + SEPARATOR + container + ZIP);

		if (!containerZip.exists()) {
			LOGGER.warn("Container '{}' does not exist under account '{}'", container, account);
			throw new FileNotFoundInDestinationException(
					KhazanaErrorCodes.CONTAINER_NOT_PRESENT_IN_DESTINATION.getErrorCode(),
					KhazanaErrorCodes.CONTAINER_NOT_PRESENT_IN_DESTINATION.getErrorMessage());
		}

		try {
			FileUtils.forceDelete(containerZip);
			LOGGER.info("Successfully deleted container '{}'", containerZip.getPath());
			return true;
		} catch (Exception e) {
			LOGGER.error("Failed to delete container '{}'", containerZip.getPath(), e);
			return false;
		}
	}

	/*
	 * @Override public boolean removeContainer(String account, String container,
	 * String source, String process) { try { File accountLoc = new
	 * File(baseLocation + SEPARATOR + account); if (!accountLoc.exists()) return
	 * false; File containerZip = new File(accountLoc.getPath() + SEPARATOR +
	 * container + ZIP); if (!containerZip.exists()) throw new
	 * FileNotFoundInDestinationException(
	 * KhazanaErrorCodes.CONTAINER_NOT_PRESENT_IN_DESTINATION.getErrorCode(),
	 * KhazanaErrorCodes.CONTAINER_NOT_PRESENT_IN_DESTINATION.getErrorMessage());
	 * containerZip.delete(); FileUtils.forceDelete(containerZip); return true; }
	 * catch (Exception e) { LOGGER.error("exception occured while packing.", e);
	 * return false; } }
	 */

	@Override
	public boolean pack(String account, String container, String source, String process, String refId) {
		File containerZip = new File(baseLocation + SEPARATOR + account + SEPARATOR + container + ZIP);

		if (!containerZip.exists()) {
			throw new FileNotFoundInDestinationException(
					KhazanaErrorCodes.CONTAINER_NOT_PRESENT_IN_DESTINATION.getErrorCode(),
					KhazanaErrorCodes.CONTAINER_NOT_PRESENT_IN_DESTINATION.getErrorMessage());
		}

		File tempEncryptedFile = new File(containerZip.getAbsolutePath() + TMP);
		File backupFile = new File(containerZip.getAbsolutePath() + BAK);

		try (InputStream originalStream = new FileInputStream(containerZip)) {
			byte[] originalBytes = IOUtils.toByteArray(originalStream);

			// Perform encryption
			byte[] encryptedBytes = helper.encrypt(refId, originalBytes);
			if (encryptedBytes == null || encryptedBytes.length == 0) {
				LOGGER.error("Encryption failed for container '{}'", containerZip.getAbsolutePath());
				return false;
			}

			// Write encrypted data to a temp file
			FileUtils.copyToFile(new ByteArrayInputStream(encryptedBytes), tempEncryptedFile);

			// Backup original ZIP before replacing
			if (containerZip.renameTo(backupFile)) {
				if (tempEncryptedFile.renameTo(containerZip)) {
					FileUtils.forceDelete(backupFile);
					LOGGER.info("Successfully encrypted and replaced container '{}'", containerZip.getAbsolutePath());
					return true;
				} else {
					// Restore from backup on failure
					backupFile.renameTo(containerZip);
					LOGGER.error("Failed to replace original ZIP with encrypted file for container '{}'",
							containerZip.getAbsolutePath());
				}
			} else {
				LOGGER.error("Could not backup original container before encryption for '{}'",
						containerZip.getAbsolutePath());
			}
		} catch (Exception e) {
			LOGGER.error("Exception occurred while encrypting container '{}'", containerZip.getAbsolutePath(), e);
		} finally {
			FileUtils.deleteQuietly(tempEncryptedFile); // Ensure temp file is removed
		}

		return false;
	}

	/*
	 * @Override public boolean pack(String account, String container, String
	 * source, String process, String refId) { try { File accountLoc = new
	 * File(baseLocation + SEPARATOR + account); if (!accountLoc.exists()) return
	 * false; File containerZip = new File(accountLoc.getPath() + SEPARATOR +
	 * container + ZIP); if (!containerZip.exists()) throw new
	 * FileNotFoundInDestinationException(
	 * KhazanaErrorCodes.CONTAINER_NOT_PRESENT_IN_DESTINATION.getErrorCode(),
	 * KhazanaErrorCodes.CONTAINER_NOT_PRESENT_IN_DESTINATION.getErrorMessage());
	 * 
	 * InputStream ios = new FileInputStream(containerZip); byte[] encryptedPacket =
	 * helper.encrypt(refId, IOUtils.toByteArray(ios)); FileUtils.copyToFile(new
	 * ByteArrayInputStream(encryptedPacket), containerZip); return encryptedPacket
	 * != null; } catch (Exception e) {
	 * LOGGER.error("exception occured while packing.", e); return false; } }
	 */

	@Override
	public Map<String, String> addTags(String account, String container, Map<String, String> tags) {
		if (tags == null || tags.isEmpty()) {
			LOGGER.info("No tags provided to add for container '{}'", container);
			return Collections.emptyMap();
		}

		try {
			JSONObject mergedTags = containerTagging(account, container, tags);
			byte[] jsonBytes = mergedTags.toString().getBytes(StandardCharsets.UTF_8);

			createContainerWithTagging(account, container, new ByteArrayInputStream(jsonBytes));

			return tags;

		} catch (Exception e) {
			LOGGER.error("Failed to add tags to container '{}'. Input tags: {}", container, tags, e);
			return Collections.emptyMap(); // safer fallback
		}
	}

	/*
	 * @Override public Map<String, String> addTags(String account, String
	 * container, Map<String, String> tags) { try { JSONObject jsonObject =
	 * containerTagging(account, container, tags);
	 * createContainerWithTagging(account, container, new
	 * ByteArrayInputStream(jsonObject.toString().getBytes())); } catch (Exception
	 * e) { LOGGER.error("exception occured to add tags for id - " + container, e);
	 * } return tags; }
	 */
	@Override
	public Map<String, String> getTags(String account, String container) {
		File accountLocation = new File(baseLocation + SEPARATOR + account);
		if (!accountLocation.exists()) {
			accountLocation.mkdirs();
		}

		File tagFile = new File(accountLocation, container + TAGS + JSON);

		// If the tag file doesn't exist yet, return an empty map
		if (!tagFile.exists()) {
			LOGGER.info("Tag file does not exist for container '{}'", container);
			return new HashMap<>();
		}

		try (InputStream inputStream = new FileInputStream(tagFile)) {
			return objectMapper.readValue(inputStream, new TypeReference<Map<String, String>>() {
			});
		} catch (Exception e) {
			LOGGER.error("Failed to read tags for container '{}'", container, e);
			return new HashMap<>(); // Safe fallback
		}
	}

	/*
	 * @Override public Map<String, String> getTags(String account, String
	 * container) { Map<String, String> metaMap = new HashMap<String, String>();
	 * File accountLocation = new File(baseLocation + SEPARATOR + account); if
	 * (!accountLocation.exists()) accountLocation.mkdir(); File tagFile = new
	 * File(accountLocation.getPath() + SEPARATOR + container + TAGS + JSON); try {
	 * if (tagFile.createNewFile()) {
	 * LOGGER.info(" tags file not yet present for  id - " + container); } else {
	 * InputStream inputstream = new FileInputStream(tagFile); BufferedReader
	 * inputStreamReader = new BufferedReader(new InputStreamReader(inputstream,
	 * "UTF-8")); StringBuilder responseStrBuilder = new StringBuilder();
	 * 
	 * String inputTags; while ((inputTags = inputStreamReader.readLine()) != null)
	 * responseStrBuilder.append(inputTags);
	 * 
	 * inputStreamReader.close(); JSONObject jsonObject = objectMapper
	 * .readValue(objectMapper.writeValueAsString(responseStrBuilder.toString()),
	 * JSONObject.class); metaMap = objectMapper.readValue(jsonObject.toString(),
	 * HashMap.class); } } catch (Exception e) {
	 * LOGGER.error("exception occured to get tags for id - " + container, e); }
	 * return metaMap; }
	 */

	private JSONObject containerTagging(String account, String container, Map<String, String> newTags) {
		JSONObject mergedTags = new JSONObject();

		try {
			// Step 1: Load existing tags
			Map<String, String> existingTags = getTags(account, container);

			// Step 2: Put existing tags first
			if (existingTags != null) {
				for (Map.Entry<String, String> entry : existingTags.entrySet()) {
					mergedTags.put(entry.getKey(), entry.getValue());
				}
			}

			// Step 3: Override or add with new tags
			if (newTags != null) {
				for (Map.Entry<String, String> entry : newTags.entrySet()) {
					mergedTags.put(entry.getKey(), entry.getValue()); // new takes precedence
				}
			}

		} catch (JSONException e) {
			LOGGER.error("Failed to merge tags for container '{}'", container, e);
		}

		return mergedTags;
	}

	/*
	 * private JSONObject containterTagging(String account, String container,
	 * Map<String, String> tags) { JSONObject jsonObject = new JSONObject(tags);
	 * Map<String, String> existingTags = getTags(account, container); if
	 * (!CollectionUtils.isEmpty(existingTags))
	 * existingTags.entrySet().forEach(entry -> { try {
	 * jsonObject.put(entry.getKey(), entry.getValue()); } catch (JSONException e) {
	 * LOGGER.error("exception occured to add tags for id - " + container, e); } });
	 * return jsonObject; }
	 */

	private void createContainerWithTagging(String account, String container, InputStream data) throws IOException {
		File accountLocation = new File(baseLocation + SEPARATOR + account);
		if (!accountLocation.exists() && !accountLocation.mkdirs()) {
			throw new IOException("Failed to create directory: " + accountLocation.getAbsolutePath());
		}

		File tagFile = new File(accountLocation, container + TAGS + JSON);

		try (OutputStream outStream = new FileOutputStream(tagFile)) {
			IOUtils.copy(data, outStream);
			LOGGER.info("Tag file created/updated for container '{}'", container);
		} catch (IOException e) {
			LOGGER.error("Failed to write tag file for container '{}'", container, e);
			throw e;
		}
	}

	/*
	 * private void createContainerWithTagging(String account, String container,
	 * InputStream data) throws IOException { File accountLocation = new
	 * File(baseLocation + SEPARATOR + account); if (!accountLocation.exists())
	 * accountLocation.mkdir(); File tagFile = new File(accountLocation.getPath() +
	 * SEPARATOR + container + TAGS + JSON); OutputStream outStream = new
	 * FileOutputStream(tagFile); outStream.write(IOUtils.toByteArray(data));
	 * outStream.close(); }
	 */
	public List<ObjectDto> getAllObjects(String account, String container) {
		return null;
	}

	@Override
	public void deleteTags(String account, String container, List<String> tags) {
		if (tags == null || tags.isEmpty()) {
			LOGGER.info("No tags provided for deletion in container '{}'", container);
			return;
		}

		try {
			JSONObject updatedTags = containerRemoveTagging(account, container, tags);

			if (((CharSequence) updatedTags).isEmpty()) {
				LOGGER.info("All tags removed from container '{}'", container);
			} else {
				LOGGER.info("Remaining tags after deletion from container '{}': {}", container, updatedTags.keys());
			}

			createContainerWithTagging(account, container,
					new ByteArrayInputStream(updatedTags.toString().getBytes(StandardCharsets.UTF_8)));

		} catch (Exception e) {
			LOGGER.error("Failed to delete tags {} from container '{}'", tags, container, e);
		}
	}

	/*
	 * @Override public void deleteTags(String account, String container,
	 * List<String> tags) { try { JSONObject jsonObject =
	 * containerRemoveTagging(account, container, tags);
	 * createContainerWithTagging(account, container, new
	 * ByteArrayInputStream(jsonObject.toString().getBytes())); } catch (Exception
	 * e) { LOGGER.error("exception occured to delete tags for id - " + container,
	 * e); } }
	 */
	private JSONObject containerRemoveTagging(String account, String container, List<String> tagsToRemove) {
		Map<String, String> existingTags = getTags(account, container);

		if (existingTags == null || existingTags.isEmpty()) {
			return new JSONObject(); // nothing to remove
		}

		for (String tag : tagsToRemove) {
			existingTags.remove(tag); // silently skip if key not present
		}

		return new JSONObject(existingTags);
	}

	/*
	 * private JSONObject containterRemoveTagging(String account, String container,
	 * List<String> tags) { Map<String, String> existingTags = getTags(account,
	 * container); tags.stream().forEach(m -> existingTags.remove(m)); JSONObject
	 * jsonObject = new JSONObject(existingTags); return jsonObject; }
	 */
}