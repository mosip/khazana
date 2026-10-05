package io.mosip.commons.khazana.spi;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import io.mosip.commons.khazana.dto.ObjectDto;
import io.mosip.commons.khazana.dto.ObjectStoreReference;

/**
 * SPI for Khazana object storage.
 * <p>
 * Callers select an implementation with a Spring {@code @Qualifier}:
 * {@code S3Adapter} (primary), {@code PosixAdapter} (one zip per container),
 * or {@code SwiftAdapter} (OpenStack Swift, not tested). Paths are account,
 * container, source, process, and object name. Null or empty source and process
 * segments are skipped when a key is built.
 * {@link #moveObject} and {@link #listObjectsByPrefix} are implemented by
 * {@code S3Adapter}. The SPI defaults return {@code false} and an empty list.
 * {@code PosixAdapter} and {@code SwiftAdapter} keep those defaults. Storage
 * failures from {@code S3Adapter} are thrown as
 * {@link io.mosip.commons.khazana.exception.ObjectStoreAdapterException}.
 * An empty list is not a failure.
 */
public interface ObjectStoreAdapter {

    /**
     * Reads the object at the given path.
     * <p>
     * {@code S3Adapter} returns a {@code SafeS3InputStream} and throws if S3 cannot be read.
     * {@code PosixAdapter} reads an entry named {@code source/process/objectName.zip} from
     * the container zip and returns {@code null} when it is missing.
     *
     * @param account    object-store account
     * @param container  container, or the S3 bucket when account-as-bucket mode is off
     * @param source     source path segment; skipped when null or empty
     * @param process    process path segment; skipped when null or empty
     * @param objectName object name
     * @return object contents, or {@code null} when the object is absent
     * @throws io.mosip.commons.khazana.exception.ObjectStoreAdapterException
     *         when {@code S3Adapter} cannot read the object
     */
    public InputStream getObject(String account, String container, String source, String process, String objectName);

    /**
     * Reports whether the object exists.
     * <p>
     * {@code S3Adapter} calls {@code doesObjectExist}. {@code PosixAdapter} treats a
     * non-null {@link #getObject} result as existence.
     *
     * @param account    object-store account
     * @param container  container, or the S3 bucket when account-as-bucket mode is off
     * @param source     source path segment; skipped when null or empty
     * @param process    process path segment; skipped when null or empty
     * @param objectName object name
     * @return {@code true} when the object is present
     * @throws io.mosip.commons.khazana.exception.ObjectStoreAdapterException
     *         when {@code S3Adapter} cannot query the object
     */
    public boolean exists(String account, String container, String source, String process, String objectName);

    /**
     * Writes {@code data} as the object at the given path.
     * <p>
     * {@code S3Adapter} creates the bucket when it is missing and closes {@code data}.
     * {@code PosixAdapter} stores the bytes as a zip entry. {@code SwiftAdapter} uploads
     * the stream and creates the container when it is missing.
     *
     * @param account    object-store account
     * @param container  container, or the S3 bucket when account-as-bucket mode is off
     * @param source     source path segment; skipped when null or empty
     * @param process    process path segment; skipped when null or empty
     * @param objectName object name
     * @param data       object contents; {@code S3Adapter} closes this stream
     * @return {@code true} when the write is accepted; {@code PosixAdapter} returns {@code false} on failure
     * @throws io.mosip.commons.khazana.exception.ObjectStoreAdapterException
     *         when {@code S3Adapter} cannot write the object
     */
    public boolean putObject(String account, String container, String source, String process, String objectName, InputStream data);

    /**
     * Merges {@code metadata} into the user metadata of the object.
     * <p>
     * {@code S3Adapter} rewrites the object with updated user metadata.
     * {@code PosixAdapter} stores metadata as a {@code .json} zip entry.
     * {@code SwiftAdapter} saves JOSS object metadata and returns {@code null}
     * when the container does not exist.
     *
     * @param account    object-store account
     * @param container  container, or the S3 bucket when account-as-bucket mode is off
     * @param source     source path segment; skipped when null or empty
     * @param process    process path segment; skipped when null or empty
     * @param objectName object name
     * @param metadata   metadata entries to add; values are stored as strings on S3
     * @return the metadata that was written, or {@code null} when Swift cannot find the container
     * @throws io.mosip.commons.khazana.exception.ObjectStoreAdapterException
     *         when {@code S3Adapter} cannot update metadata
     */
    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process, String objectName, Map<String, Object> metadata);

    /**
     * Adds one metadata entry. Equivalent to {@link #addObjectMetaData(String, String, String, String, String, Map)}
     * with a single-entry map.
     *
     * @param account    object-store account
     * @param container  container, or the S3 bucket when account-as-bucket mode is off
     * @param source     source path segment; skipped when null or empty
     * @param process    process path segment; skipped when null or empty
     * @param objectName object name
     * @param key        metadata key
     * @param value      metadata value
     * @return the metadata map after the update, or {@code null} when the object or container is missing
     * @throws io.mosip.commons.khazana.exception.ObjectStoreAdapterException
     *         when {@code S3Adapter} cannot update metadata
     */
    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process, String objectName, String key, String value);

    /**
     * Reads user metadata for the object.
     * <p>
     * {@code S3Adapter} uses a metadata-only request and returns an empty map when the
     * object is not found (HTTP 404). {@code PosixAdapter} reads the {@code .json} zip
     * entry. {@code SwiftAdapter} returns {@code null} when the container does not exist;
     * a null {@code objectName} lists metadata for every object in the container.
     *
     * @param account    object-store account
     * @param container  container, or the S3 bucket when account-as-bucket mode is off
     * @param source     source path segment; skipped when null or empty
     * @param process    process path segment; skipped when null or empty
     * @param objectName object name; {@code null} on Swift lists the container
     * @return metadata map; empty when S3 has no user metadata or the object is absent.
     *         {@code null} when POSIX or Swift cannot find the container or entry
     * @throws io.mosip.commons.khazana.exception.ObjectStoreAdapterException
     *         when {@code S3Adapter} fails for a reason other than HTTP 404
     * @throws io.mosip.commons.khazana.exception.FileNotFoundInDestinationException
     *         when {@code PosixAdapter} cannot find the container zip
     */
    public Map<String, Object> getMetaData(String account, String container, String source, String process, String objectName);

    /**
     * Increments a numeric metadata value by one and writes it back.
     * <p>
     * {@code S3Adapter} implements this. {@code PosixAdapter} and {@code SwiftAdapter}
     * return {@code 0} without changing storage.
     *
     * @param account     object-store account
     * @param container   container, or the S3 bucket when account-as-bucket mode is off
     * @param source      source path segment; skipped when null or empty
     * @param process     process path segment; skipped when null or empty
     * @param objectName  object name
     * @param metaDataKey metadata key whose integer value is incremented
     * @return the new value, {@code null} when {@code S3Adapter} does not find the key,
     *         or {@code 0} from the POSIX and Swift adapters
     * @throws io.mosip.commons.khazana.exception.ObjectStoreAdapterException
     *         when {@code S3Adapter} cannot read or write the metadata
     */
    public Integer incMetadata(String account, String container, String source, String process, String objectName, String metaDataKey);

    /**
     * Decrements a numeric metadata value by one and writes it back.
     * <p>
     * {@code S3Adapter} implements this. {@code PosixAdapter} and {@code SwiftAdapter}
     * return {@code 0} without changing storage.
     *
     * @param account     object-store account
     * @param container   container, or the S3 bucket when account-as-bucket mode is off
     * @param source      source path segment; skipped when null or empty
     * @param process     process path segment; skipped when null or empty
     * @param objectName  object name
     * @param metaDataKey metadata key whose integer value is decremented
     * @return the new value, {@code null} when {@code S3Adapter} does not find the key,
     *         or {@code 0} from the POSIX and Swift adapters
     * @throws io.mosip.commons.khazana.exception.ObjectStoreAdapterException
     *         when {@code S3Adapter} cannot read or write the metadata
     */
    public Integer decMetadata(String account, String container, String source, String process, String objectName, String metaDataKey);

    /**
     * Deletes the object at the given path.
     * <p>
     * {@code S3Adapter} deletes the S3 key. {@code PosixAdapter} and {@code SwiftAdapter}
     * return {@code true} without deleting.
     *
     * @param account    object-store account
     * @param container  container, or the S3 bucket when account-as-bucket mode is off
     * @param source     source path segment; skipped when null or empty
     * @param process    process path segment; skipped when null or empty
     * @param objectName object name
     * @return {@code true} when the delete is accepted
     * @throws io.mosip.commons.khazana.exception.ObjectStoreAdapterException
     *         when {@code S3Adapter} cannot delete the object
     */
    public boolean deleteObject(String account, String container, String source, String process, String objectName);

    /**
     * Removes the container.
     * <p>
     * {@code PosixAdapter} deletes {@code {base}/{account}/{container}.zip}.
     * {@code S3Adapter} and {@code SwiftAdapter} return {@code false}.
     *
     * @param account   object-store account
     * @param container container to remove
     * @param source    source path segment; unused by the bundled adapters
     * @param process   process path segment; unused by the bundled adapters
     * @return {@code true} when the POSIX container zip was deleted; {@code false} otherwise
     */
    public boolean removeContainer(String account, String container, String source, String process);

    /**
     * Encrypts the container in place.
     * <p>
     * {@code PosixAdapter} encrypts {@code {base}/{account}/{container}.zip} through
     * {@code EncryptionHelper} using {@code refId}. {@code S3Adapter} and
     * {@code SwiftAdapter} return {@code false}.
     *
     * @param account   object-store account
     * @param container container whose zip is encrypted on POSIX
     * @param source    source path segment; unused by the bundled adapters
     * @param process   process path segment; unused by the bundled adapters
     * @param refId     reference id of the encryption key
     * @return {@code true} when POSIX encryption produced a packet; {@code false} otherwise
     */
    public boolean pack(String account, String container, String source, String process, String refId);

    /**
     * Lists objects in the container.
     * <p>
     * {@code S3Adapter} returns {@link ObjectDto} entries and skips tag objects.
     * {@code PosixAdapter} and {@code SwiftAdapter} return {@code null}.
     *
     * @param account   object-store account; S3 bucket when account-as-bucket mode is on
     * @param container container to list; S3 bucket when account-as-bucket mode is off
     * @return parsed objects, or {@code null} when none are found or the adapter does not list
     * @throws io.mosip.commons.khazana.exception.ObjectStoreAdapterException
     *         when {@code S3Adapter} cannot connect or list
     */
    public List<ObjectDto> getAllObjects(String account, String container);

	/**
	 * Adds container tags.
	 * <p>
	 * On S3, tags are objects under {@code Tags/}, not native S3 object tags.
	 * On POSIX they are stored in {@code {account}/{container}_tags.json} beside the zip.
	 * On Swift they are container metadata.
	 *
	 * @param account   object-store account
	 * @param container container to tag
	 * @param tags      tag names and values to add
	 * @return {@code tags}
	 * @throws io.mosip.commons.khazana.exception.ObjectStoreAdapterException
	 *         when {@code S3Adapter} cannot write a tag object
	 */
	public Map<String, String> addTags(String account, String container, Map<String, String> tags);

	/**
	 * Reads container tags written by {@link #addTags}.
	 *
	 * @param account   object-store account
	 * @param container container whose tags are read
	 * @return tag name to value; empty when no tag objects exist
	 * @throws io.mosip.commons.khazana.exception.ObjectStoreAdapterException
	 *         when {@code S3Adapter} cannot list or read tag objects
	 */
	public Map<String, String> getTags(String account, String container);
	
	/**
	 * Removes the named container tags.
	 *
	 * @param account   object-store account
	 * @param container container whose tags are removed
	 * @param tags      tag names to delete
	 * @throws io.mosip.commons.khazana.exception.ObjectStoreAdapterException
	 *         when {@code S3Adapter} cannot delete a tag object
	 */
	public void deleteTags(String account, String container, List<String> tags);

	/**
	 * Copies the object identified by {@code src} to the location identified by
	 * {@code dst}. If {@code deleteSourceAfterCopy} is {@code true}, the source
	 * object is deleted after a successful copy, effectively performing a move.
	 * <p>
	 * {@code S3Adapter} copies then optionally deletes and throws on a missing
	 * source. The default returns {@code false}. {@code PosixAdapter} and
	 * {@code SwiftAdapter} keep the default.
	 *
	 * @param src                  reference to the source object
	 * @param dst                  reference to the destination object
	 * @param deleteSourceAfterCopy whether to delete the source after a successful copy
	 * @return {@code true} if the operation succeeded; {@code false} otherwise
	 */
	public default boolean moveObject(ObjectStoreReference src, ObjectStoreReference dst,
			boolean deleteSourceAfterCopy) {
		return false;
	}

	/**
	 * Lists object keys under {@code prefix} in the given account/container.
	 *
	 * <p><b>Return format (adapter-specific):</b>
	 * <ul>
	 *   <li>When {@code object.store.s3.use.account.as.bucketname=false} (default):
	 *       returns full S3 keys relative to the bucket root, exactly as stored
	 *       (e.g. {@code "_draft/ridHash/Biometrics/bio.cbeff"}).
	 *       These keys can be used directly as {@code objectName} in
	 *       {@link ObjectStoreReference} for {@link #moveObject}.</li>
	 *   <li>When {@code object.store.s3.use.account.as.bucketname=true}:
	 *       objects are stored under {@code container/} inside the account bucket.
	 *       The {@code prefix} parameter is scoped to the container (e.g.
	 *       {@code "_draft/ridHash/Biometrics/"}), but returned keys have the
	 *       {@code container/} segment removed so they are <b>container-relative</b>
	 *       (e.g. {@code "_draft/ridHash/Biometrics/bio.cbeff"}, not
	 *       {@code "ridHash/_draft/ridHash/Biometrics/bio.cbeff"}).
	 *       Returned keys are intended for direct use as {@code objectName} in
	 *       {@link ObjectStoreReference} when calling {@link #moveObject}.</li>
	 * </ul>
	 *
	 * <p><b>Empty result:</b> Returns an empty list (never {@code null}) when no
	 * objects match the prefix. This is not an error.
	 *
	 * <p><b>Errors:</b> Throws {@link io.mosip.commons.khazana.exception.ObjectStoreAdapterException}
	 * or the underlying storage exception when the list operation fails (e.g. bucket
	 * inaccessible, permission denied). An empty list does not indicate failure.
	 *
	 * <p><b>Adapter support:</b> Fully implemented by {@code S3Adapter}. The default
	 * implementation returns an empty list; {@code PosixAdapter} and {@code SwiftAdapter}
	 * do not override this method.
	 *
	 * @param account   object-store account name; used as the S3 bucket when
	 *                  {@code use.account.as.bucketname=true}
	 * @param container bucket name (when account is not the bucket) or container
	 *                  segment inside the account bucket (when
	 *                  {@code use.account.as.bucketname=true})
	 * @param prefix    prefix to filter within the container scope (not the full
	 *                  bucket-root prefix when {@code use.account.as.bucketname=true})
	 * @return list of object keys in container-relative or bucket-root format as
	 *         described above; never {@code null}
	 * @throws io.mosip.commons.khazana.exception.ObjectStoreAdapterException
	 *         when an override cannot list the bucket; an empty list is not a failure
	 */
	default List<String> listObjectsByPrefix(String account, String container, String prefix) {
		return java.util.Collections.emptyList();
	}
}
