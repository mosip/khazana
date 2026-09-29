package io.mosip.commons.khazana.impl;

import java.io.BufferedOutputStream;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.zip.ZipEntry;
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
import org.springframework.util.CollectionUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.mosip.commons.khazana.constant.KhazanaErrorCodes;
import io.mosip.commons.khazana.dto.ObjectDto;
import io.mosip.commons.khazana.exception.FileNotFoundInDestinationException;
import io.mosip.commons.khazana.spi.ObjectStoreAdapter;
import io.mosip.commons.khazana.util.EncryptionHelper;
import io.mosip.commons.khazana.util.ObjectStoreUtil;
import io.mosip.kernel.core.util.FileUtils;

/**
 * Filesystem implementation of {@link ObjectStoreAdapter}, selected with
 * {@code @Qualifier("PosixAdapter")}.
 * <p>
 * Each container is one zip: {@code {object.store.base.location}/{account}/{container}.zip}.
 * An object entry is {@code source/process/objectName.zip}. Metadata is
 * {@code source/process/objectName.json}. Tags live beside the zip in
 * {@code {account}/{container}_tags.json}, not inside it.
 * {@link #pack} encrypts that zip in place through {@link EncryptionHelper}.
 * {@link #incMetadata} and {@link #decMetadata} return {@code 0}.
 * {@link #getAllObjects} returns {@code null}. {@code listObjectsByPrefix} and
 * {@code moveObject} use the SPI defaults.
 */
@Service
@Qualifier("PosixAdapter")
public class PosixAdapter implements ObjectStoreAdapter {

    /**
     * SLF4J logger. The logger name is {@code SwiftAdapter} as declared in source.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(SwiftAdapter.class);

    /**
     * Path separator between the base location, account, and container file.
     */
    private static final String SEPARATOR = "/";

    /**
     * Suffix for the container zip and for object entries inside it.
     */
    private static final String ZIP = ".zip";

    /**
     * Suffix for metadata entries inside the zip and for the tags file beside it.
     */
    private static final String JSON = ".json";

    /**
     * Infix of the tags file name: {@code {container}_tags.json}.
     */
	private static final String TAGS = "_tags";

    /**
     * Jackson mapper used to read metadata and tag JSON.
     */
    @Autowired
    private ObjectMapper objectMapper;

    /**
     * Directory that holds account folders.
     * Property {@code object.store.base.location}, default {@code home}.
     */
    @Value("${object.store.base.location:home}")
    private String baseLocation;

    /**
     * Encrypts the container zip during {@link #pack}.
     */
    @Autowired
    private EncryptionHelper helper;

    /**
     * Reads {@code source/process/objectName.zip} from the container zip.
     *
     * @param account    account directory under {@link #baseLocation}
     * @param container  container zip name, without {@code .zip}
     * @param source     source path segment
     * @param process    process path segment
     * @param objectName object name, without {@code .zip}
     * @return entry bytes, or {@code null} when the account, container, or entry is missing
     */
    public InputStream getObject(String account, String container, String source, String process, String objectName) {
        try {
            File accountLoc = new File(baseLocation + SEPARATOR + account);
            if (!accountLoc.exists())
                return null;
            File containerZip = new File(accountLoc.getPath() + SEPARATOR + container + ZIP);
            if (!containerZip.exists())
                throw new FileNotFoundInDestinationException(KhazanaErrorCodes.CONTAINER_NOT_PRESENT_IN_DESTINATION.getErrorCode(),
                        KhazanaErrorCodes.CONTAINER_NOT_PRESENT_IN_DESTINATION.getErrorMessage());

            InputStream ios = new FileInputStream(containerZip);
            Map<ZipEntry, ByteArrayOutputStream> entries = getAllExistingEntries(ios);

            Optional<ZipEntry> zipEntry = entries.keySet().stream().filter(e ->
                    e.getName().contains(ObjectStoreUtil.getName(source, process, objectName) + ZIP)).findAny();

            if (zipEntry.isPresent() && zipEntry.get() != null)
                return new ByteArrayInputStream(entries.get(zipEntry.get()).toByteArray());

        } catch (FileNotFoundInDestinationException e) {
            LOGGER.error("exception occured to get object for id - " + container, e);
        } catch (IOException e) {
            LOGGER.error("exception occured to get object for id - " + container, e);
        }
        return null;
    }

    /**
     * Reports whether {@link #getObject} finds an entry.
     *
     * @param account    account directory under {@link #baseLocation}
     * @param container  container zip name, without {@code .zip}
     * @param source     source path segment
     * @param process    process path segment
     * @param objectName object name, without {@code .zip}
     * @return {@code true} when the object entry is present
     */
    public boolean exists(String account, String container, String source, String process, String objectName) {
        return getObject(account, container, source, process, objectName) != null;
    }

    /**
     * Stores {@code data} as {@code objectName.zip} inside the container zip.
     *
     * @param account    account directory under {@link #baseLocation}
     * @param container  container zip name, without {@code .zip}
     * @param source     source path segment
     * @param process    process path segment
     * @param objectName object name; {@code .zip} is appended
     * @param data       entry bytes
     * @return {@code true} when the zip was written; {@code false} when writing fails
     */
    public boolean putObject(String account, String container, String source, String process, String objectName, InputStream data) {
        try {
            createContainerZipWithSubpacket(account, container, source, process, objectName + ZIP, data);
            return true;
        } catch (Exception e) {
            LOGGER.error("exception occured. Will create a new connection.", e);
        }
        return false;
    }

    /**
     * Writes metadata as {@code objectName.json} inside the container zip.
     * <p>
     * Existing metadata keys are copied onto the new JSON before it is stored.
     *
     * @param account    account directory under {@link #baseLocation}
     * @param container  container zip name, without {@code .zip}
     * @param source     source path segment
     * @param process    process path segment
     * @param objectName object name; {@code .json} is appended
     * @param metadata   metadata to store
     * @return {@code metadata}
     */
    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process, String objectName, Map<String, Object> metadata) {
        try {
            JSONObject jsonObject = objectMetadata(account, container, source, process, objectName, metadata);
            createContainerZipWithSubpacket(account, container, source, process, objectName + JSON,
                    new ByteArrayInputStream(jsonObject.toString().getBytes()));
        } catch (io.mosip.kernel.core.exception.IOException | IOException e) {
            LOGGER.error("exception occured to add metadata for id - " + container, e);
        }
        return metadata;
    }

    /**
     * Stores a single metadata entry as {@code objectName.json}.
     *
     * @param account    account directory under {@link #baseLocation}
     * @param container  container zip name, without {@code .zip}
     * @param source     source path segment
     * @param process    process path segment
     * @param objectName object name; {@code .json} is appended
     * @param key        metadata key
     * @param value      metadata value
     * @return a one-entry map, or {@code null} when writing fails
     */
    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process, String objectName, String key, String value) {
        try {
            Map<String, Object> metaMap = new HashMap<>();
            metaMap.put(key, value);
            JSONObject jsonObject = objectMetadata(account, container, source, process, objectName, metaMap);
            createContainerZipWithSubpacket(account, container, source, process, objectName + JSON, new ByteArrayInputStream(jsonObject.toString().getBytes()));
            return metaMap;
        } catch (io.mosip.kernel.core.exception.IOException e) {
            LOGGER.error("exception occured to add metadata for id - " + container, e);
        } catch (IOException e) {
            LOGGER.error("exception occured to add metadata for id - " + container, e);
        }
        return null;
    }

    /**
     * Reads {@code objectName.json} from the container zip.
     *
     * @param account    account directory under {@link #baseLocation}
     * @param container  container zip name, without {@code .zip}
     * @param source     source path segment; not used when matching the entry name
     * @param process    process path segment; not used when matching the entry name
     * @param objectName object name; the entry name must contain {@code objectName.json}
     * @return parsed metadata, or {@code null} when the account or entry is missing
     * @throws FileNotFoundInDestinationException when the container zip does not exist
     */
    public Map<String, Object> getMetaData(String account, String container, String source, String process, String objectName) {
        Map<String, Object> metaMap = null;
        try {
            File accountLoc = new File(baseLocation + SEPARATOR + account);
            if (!accountLoc.exists())
                return null;
            File containerZip = new File(accountLoc.getPath() + SEPARATOR + container + ZIP);
            if (!containerZip.exists())
                throw new FileNotFoundInDestinationException(KhazanaErrorCodes.CONTAINER_NOT_PRESENT_IN_DESTINATION.getErrorCode(),
                        KhazanaErrorCodes.CONTAINER_NOT_PRESENT_IN_DESTINATION.getErrorMessage());

            InputStream ios = new FileInputStream(containerZip);
            Map<ZipEntry, ByteArrayOutputStream> entries = getAllExistingEntries(ios);

            Optional<ZipEntry> zipEntry = entries.keySet().stream().filter(e -> e.getName().contains(objectName + JSON)).findAny();

            if (zipEntry.isPresent() && zipEntry.get() != null) {
                String string = entries.get(zipEntry.get()).toString();
                JSONObject jsonObject = objectMapper.readValue(objectMapper.writeValueAsString(string), JSONObject.class);
                metaMap = objectMapper.readValue(jsonObject.toString(), HashMap.class);
            }
        } catch (FileNotFoundInDestinationException e) {
            LOGGER.error("exception occured. Will create a new connection.", e);
            throw e;
        } catch (IOException e) {
            LOGGER.error("exception occured to get metadata for id - " + container, e);
        }
        return metaMap;
    }

    /**
     * Creates or rewrites the container zip, adding one entry named
     * {@code source/process/objectName}.
     *
     * @param account    account directory under {@link #baseLocation}
     * @param container  container zip name, without {@code .zip}
     * @param source     source path segment of the new entry
     * @param process    process path segment of the new entry
     * @param objectName entry file name, including {@code .zip} or {@code .json}
     * @param data       entry bytes
     * @throws io.mosip.kernel.core.exception.IOException when the zip cannot be copied onto the container file
     * @throws IOException when the existing zip cannot be read
     */
    private void createContainerZipWithSubpacket(String account, String container, String source, String process, String objectName, InputStream data) throws io.mosip.kernel.core.exception.IOException, IOException {
        File accountLocation = new File(baseLocation + SEPARATOR + account);
        if (!accountLocation.exists())
            accountLocation.mkdir();
        File containerZip = new File(accountLocation.getPath() + SEPARATOR + container + ZIP);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        if (!containerZip.exists()) {
            try (ZipOutputStream packetZip = new ZipOutputStream(new BufferedOutputStream(out))) {
                addEntryToZip(String.format(objectName),
                        IOUtils.toByteArray(data), packetZip, source, process);
            }
        } else {
            InputStream ios = new FileInputStream(containerZip);
            Map<ZipEntry, ByteArrayOutputStream> entries = getAllExistingEntries(ios);
            try (ZipOutputStream packetZip = new ZipOutputStream(out)) {
                entries.entrySet().forEach(e -> {
                    try {
                        packetZip.putNextEntry(e.getKey());
                        packetZip.write(e.getValue().toByteArray());
                    } catch (IOException e1) {
                        LOGGER.error("exception occured. Will create a new connection.", e1);
                    }
                });
                addEntryToZip(String.format(objectName),
                        IOUtils.toByteArray(data), packetZip, source, process);
            }
        }

        FileUtils.copyToFile(new ByteArrayInputStream(out.toByteArray()), containerZip);
    }

    /**
     * Adds one zip entry whose name is {@code source/process/fileName}.
     * <p>
     * A null {@code data} array is skipped. I/O failures are logged.
     *
     * @param fileName        entry file name
     * @param data            entry bytes; ignored when {@code null}
     * @param zipOutputStream zip being written
     * @param source          source path segment
     * @param process         process path segment
     */
    private void addEntryToZip(String fileName, byte[] data, ZipOutputStream zipOutputStream, String source, String process) {
        try {
            if (data != null) {
                ZipEntry zipEntry = new ZipEntry(ObjectStoreUtil.getName(source, process, fileName));
                zipOutputStream.putNextEntry(zipEntry);
                zipOutputStream.write(data);
            }
        } catch (IOException e) {
            LOGGER.error("exception occured. Will create a new connection.", e);
        }
    }

    /**
     * Reads every entry in {@code packetStream} into memory and closes the stream.
     *
     * @param packetStream container zip
     * @return entries keyed by their {@link ZipEntry}
     * @throws IOException when the zip cannot be read
     */
    private Map<ZipEntry, ByteArrayOutputStream> getAllExistingEntries(InputStream packetStream) throws IOException {
        Map<ZipEntry, ByteArrayOutputStream> entries = new HashMap<>();
        try (ZipInputStream zis = new ZipInputStream(packetStream)) {
            ZipEntry ze = zis.getNextEntry();
            while (ze != null) {
                int len;
                byte[] buffer = new byte[2048];
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                while ((len = zis.read(buffer)) > 0) {
                    out.write(buffer, 0, len);
                }
                entries.put(ze, out);
                zis.closeEntry();
                ze = zis.getNextEntry();
                out.close();
            }
            zis.closeEntry();
        } finally {
            packetStream.close();
        }
        return entries;
    }

    /**
     * Builds metadata JSON, copying keys already stored for the object.
     *
     * @param account    account directory under {@link #baseLocation}
     * @param container  container zip name, without {@code .zip}
     * @param source     source path segment
     * @param process    process path segment
     * @param objectName object name whose existing metadata is merged
     * @param metadata   new metadata; existing keys are added when absent from this map's JSON
     * @return merged metadata JSON
     */
    private JSONObject objectMetadata(String account, String container, String source, String process,
                                      String objectName, Map<String, Object> metadata) {
        JSONObject jsonObject = new JSONObject(metadata);
        Map<String, Object> existingMetaData = getMetaData(account, container, source, process, objectName);
        if (!CollectionUtils.isEmpty(existingMetaData))
            existingMetaData.entrySet().forEach(entry -> {
                try {
                    jsonObject.put(entry.getKey(), entry.getValue());
                } catch (JSONException e) {
                    LOGGER.error("exception occured to add metadata for id - " + container, e);
                }
            });
        return jsonObject;
    }

    /**
     * Not implemented. Always returns {@code 0}.
     *
     * @param account     account directory under {@link #baseLocation}
     * @param container   container zip name, without {@code .zip}
     * @param source      source path segment
     * @param process     process path segment
     * @param objectName  object name
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
     * @param account     account directory under {@link #baseLocation}
     * @param container   container zip name, without {@code .zip}
     * @param source      source path segment
     * @param process     process path segment
     * @param objectName  object name
     * @param metaDataKey metadata key that would be decremented
     * @return {@code 0}
     */
    @Override
    public Integer decMetadata(String account, String container, String source, String process, String objectName, String metaDataKey) {
        // TODO Auto-generated method stub
        return 0;
    }

    /**
     * Not implemented. Returns {@code true} without deleting the zip entry.
     *
     * @param account    account directory under {@link #baseLocation}
     * @param container  container zip name, without {@code .zip}
     * @param source     source path segment
     * @param process    process path segment
     * @param objectName object name
     * @return {@code true}
     */
    @Override
    public boolean deleteObject(String account, String container, String source, String process, String objectName) {
        return true;
    }

    /**
     * Deletes the container zip under the account directory.
     *
     * @param account   account directory under {@link #baseLocation}
     * @param container container zip name, without {@code .zip}
     * @param source    unused path segment
     * @param process   unused path segment
     * @return {@code true} when the zip was deleted; {@code false} when the account is missing or deletion fails
     */
    @Override
    public boolean removeContainer(String account, String container, String source, String process) {
        try {
            File accountLoc = new File(baseLocation + SEPARATOR + account);
            if (!accountLoc.exists())
                return false;
            File containerZip = new File(accountLoc.getPath() + SEPARATOR + container + ZIP);
            if (!containerZip.exists())
                throw new FileNotFoundInDestinationException(KhazanaErrorCodes.CONTAINER_NOT_PRESENT_IN_DESTINATION.getErrorCode(),
                        KhazanaErrorCodes.CONTAINER_NOT_PRESENT_IN_DESTINATION.getErrorMessage());
            containerZip.delete();
            FileUtils.forceDelete(containerZip);
            return true;
        } catch (Exception e) {
            LOGGER.error("exception occured while packing.", e);
            return false;
        }

    }

    /**
     * Encrypts the container zip in place with {@link #helper} and {@code refId}.
     *
     * @param account   account directory under {@link #baseLocation}
     * @param container container zip name, without {@code .zip}
     * @param source    unused path segment
     * @param process   unused path segment
     * @param refId     reference id of the encryption key
     * @return {@code true} when encryption returns a packet; {@code false} when the account is missing or encryption fails
     */
    @Override
    public boolean pack(String account, String container, String source, String process, String refId) {
        try {
            File accountLoc = new File(baseLocation + SEPARATOR + account);
            if (!accountLoc.exists())
                return false;
            File containerZip = new File(accountLoc.getPath() + SEPARATOR + container + ZIP);
            if (!containerZip.exists())
                throw new FileNotFoundInDestinationException(KhazanaErrorCodes.CONTAINER_NOT_PRESENT_IN_DESTINATION.getErrorCode(),
                        KhazanaErrorCodes.CONTAINER_NOT_PRESENT_IN_DESTINATION.getErrorMessage());

            InputStream ios = new FileInputStream(containerZip);
            byte[] encryptedPacket = helper.encrypt(refId, IOUtils.toByteArray(ios));
            FileUtils.copyToFile(new ByteArrayInputStream(encryptedPacket), containerZip);
            return encryptedPacket != null;
        } catch (Exception e) {
            LOGGER.error("exception occured while packing.", e);
            return false;
        }
    }

	/**
	 * Writes tags to {@code {account}/{container}_tags.json}, merging existing tags.
	 *
	 * @param account   account directory under {@link #baseLocation}
	 * @param container container name used in the tags file
	 * @param tags      tags to add
	 * @return {@code tags}
	 */
	@Override
	public Map<String, String> addTags(String account, String container, Map<String, String> tags) {
		try {
		JSONObject jsonObject = containterTagging(account, container, tags);
		createContainerWithTagging(account, container, new ByteArrayInputStream(jsonObject.toString().getBytes()));
		} catch (Exception e) {
			LOGGER.error("exception occured to add tags for id - " + container, e);
		}
		return tags;
	}

	/**
	 * Reads {@code {account}/{container}_tags.json}.
	 * <p>
	 * Creates the account directory and an empty tags file when they are missing.
	 *
	 * @param account   account directory under {@link #baseLocation}
	 * @param container container name used in the tags file
	 * @return tags from the file, or an empty map when the file is new or cannot be read
	 */
	@Override
	public Map<String, String> getTags(String account, String container) {
		Map<String, String> metaMap = new HashMap<String, String>();
		File accountLocation = new File(baseLocation + SEPARATOR + account);
		if (!accountLocation.exists())
			accountLocation.mkdir();
		File tagFile = new File(accountLocation.getPath() + SEPARATOR + container + TAGS + JSON);
		try {
		if (tagFile.createNewFile()) {
			LOGGER.info(" tags file not yet present for  id - " + container);
		} else {
			InputStream inputstream = new FileInputStream(tagFile);
			BufferedReader inputStreamReader = new BufferedReader(new InputStreamReader(inputstream, "UTF-8"));
			StringBuilder responseStrBuilder = new StringBuilder();

			String inputTags;
			while ((inputTags = inputStreamReader.readLine()) != null)
			    responseStrBuilder.append(inputTags);

			inputStreamReader.close();
			JSONObject jsonObject = objectMapper.readValue(objectMapper.writeValueAsString(responseStrBuilder.toString()),
					JSONObject.class);
			metaMap = objectMapper.readValue(jsonObject.toString(), HashMap.class);
			}
		} catch (Exception e) {
			LOGGER.error("exception occured to get tags for id - " + container, e);
		}
		return metaMap;
	}

	/**
	 * Merges {@code tags} with tags already stored for the container.
	 *
	 * @param account   account directory under {@link #baseLocation}
	 * @param container container name used in the tags file
	 * @param tags      tags to add
	 * @return merged tag JSON
	 */
	private JSONObject containterTagging(String account, String container, Map<String, String> tags) {
		JSONObject jsonObject = new JSONObject(tags);
		Map<String, String> existingTags = getTags(account, container);
		if (!CollectionUtils.isEmpty(existingTags))
			existingTags.entrySet().forEach(entry -> {
				try {
					jsonObject.put(entry.getKey(), entry.getValue());
				} catch (JSONException e) {
					LOGGER.error("exception occured to add tags for id - " + container, e);
				}
			});
		return jsonObject;
	}

	/**
	 * Writes {@code data} to {@code {account}/{container}_tags.json}, creating the account directory when needed.
	 *
	 * @param account   account directory under {@link #baseLocation}
	 * @param container container name used in the tags file
	 * @param data      tag JSON bytes
	 * @throws IOException when the tags file cannot be written
	 */
	private void createContainerWithTagging(String account, String container, InputStream data) throws IOException {

		File accountLocation = new File(baseLocation + SEPARATOR + account);
		if (!accountLocation.exists())
			accountLocation.mkdir();
		File tagFile = new File(accountLocation.getPath() + SEPARATOR + container + TAGS + JSON);
		OutputStream outStream = new FileOutputStream(tagFile);
		outStream.write(IOUtils.toByteArray(data));
		outStream.close();

	}

    /**
     * Not implemented. Always returns {@code null}.
     *
     * @param account   account directory under {@link #baseLocation}
     * @param container container zip name, without {@code .zip}
     * @return {@code null}
     */
    public List<ObjectDto> getAllObjects(String account, String container) {
        return null;
    }

	/**
	 * Removes the named tags from {@code {account}/{container}_tags.json}.
	 *
	 * @param account   account directory under {@link #baseLocation}
	 * @param container container name used in the tags file
	 * @param tags      tag names to remove
	 */
	@Override
	public void deleteTags(String account, String container, List<String> tags) {
		try {
			JSONObject jsonObject = containterRemoveTagging(account, container, tags);
			createContainerWithTagging(account, container, new ByteArrayInputStream(jsonObject.toString().getBytes()));
			} catch (Exception e) {
				LOGGER.error("exception occured to delete tags for id - " + container, e);
			}
		

	}

	/**
	 * Copies current tags and drops every name in {@code tags}.
	 *
	 * @param account   account directory under {@link #baseLocation}
	 * @param container container name used in the tags file
	 * @param tags      tag names to remove
	 * @return remaining tags as JSON
	 */
	private JSONObject containterRemoveTagging(String account, String container,List<String> tags) {
	
		Map<String, String> existingTags = getTags(account, container);
		tags.stream()
		.forEach(m -> existingTags.remove(m));
		JSONObject jsonObject = new JSONObject(existingTags);
		return jsonObject;
	}
}
