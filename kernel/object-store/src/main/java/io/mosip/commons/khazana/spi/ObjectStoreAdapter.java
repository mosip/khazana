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
	 * Returns the full object keys (relative to the bucket root) for all objects
	 * whose key starts with {@code prefix} in the given container.
	 *
	 * <p>The default implementation returns an empty list so that existing adapter
	 * implementations remain source-compatible without needing to override this method.
	 *
	 * @param account   object-store account name
	 * @param container bucket / container name
	 * @param prefix    key prefix to filter on (e.g. {@code "_draft/ridHash/Biometrics/"})
	 * @return list of full object keys matching the prefix; never {@code null}
	 */
	public default List<String> listObjectsByPrefix(String account, String container, String prefix) {
		return java.util.Collections.emptyList();
	}
}
