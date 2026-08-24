package io.mosip.commons.khazana.spi;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import io.mosip.commons.khazana.dto.ObjectDto;
import io.mosip.commons.khazana.dto.ObjectStoreReference;

public interface ObjectStoreAdapter {

    public InputStream getObject(String account, String container, String source, String process, String objectName);

    public boolean exists(String account, String container, String source, String process, String objectName);

    public boolean putObject(String account, String container, String source, String process, String objectName, InputStream data);

    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process, String objectName, Map<String, Object> metadata);

    public Map<String, Object> addObjectMetaData(String account, String container, String source, String process, String objectName, String key, String value);

    public Map<String, Object> getMetaData(String account, String container, String source, String process, String objectName);

    public Integer incMetadata(String account, String container, String source, String process, String objectName, String metaDataKey);

    public Integer decMetadata(String account, String container, String source, String process, String objectName, String metaDataKey);

    public boolean deleteObject(String account, String container, String source, String process, String objectName);

    public boolean removeContainer(String account, String container, String source, String process);

    public boolean pack(String account, String container, String source, String process, String refId);

    public List<ObjectDto> getAllObjects(String account, String container);

	public Map<String, String> addTags(String account, String container, Map<String, String> tags);

	public Map<String, String> getTags(String account, String container);
	
	public void deleteTags(String account, String container, List<String> tags);

	/**
	 * Copies the object identified by {@code src} to the location identified by
	 * {@code dst}. If {@code deleteSourceAfterCopy} is {@code true}, the source
	 * object is deleted after a successful copy, effectively performing a move.
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
	 * do not support this operation today.
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
	 */
	default List<String> listObjectsByPrefix(String account, String container, String prefix) {
		return java.util.Collections.emptyList();
	}
}
