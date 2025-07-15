package io.mosip.commons.khazana.spi;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import io.mosip.commons.khazana.dto.ObjectDto;

/**
 * {@code ObjectStoreAdapter} is a service provider interface (SPI) that defines the contract
 * for integrating with any object storage backend (e.g., OpenStack Swift, MinIO, AWS S3).
 * 
 * <p>This interface supports object lifecycle operations, metadata handling,
 * and tagging mechanisms to abstract underlying object storage implementations.</p>
 * 
 * <p>Implementing classes must ensure thread safety and handle storage-specific exceptions.</p>
 * 
 * @author MOSIP Team
 */
public interface ObjectStoreAdapter {

    /**
     * Retrieves an object from the object store.
     *
     * @param account     the tenant or project account name
     * @param container   the logical bucket/container name
     * @param source      the module or component requesting the object
     * @param process     the process or context the object belongs to
     * @param objectName  the unique identifier of the object
     * @return the input stream of the requested object, or {@code null} if not found
     */
    InputStream getObject(String account, String container, String source, String process, String objectName);

    /**
     * Checks if the specified object exists in the object store.
     *
     * @param account     the tenant account
     * @param container   the container name
     * @param source      the originating module
     * @param process     the process context
     * @param objectName  the name of the object
     * @return {@code true} if the object exists, otherwise {@code false}
     */
    boolean exists(String account, String container, String source, String process, String objectName);

    /**
     * Uploads or updates an object in the store.
     *
     * @param account     the tenant account
     * @param container   the container name
     * @param source      the originating module
     * @param process     the process context
     * @param objectName  the name of the object
     * @param data        the input stream to be uploaded
     * @return {@code true} if the upload succeeds, otherwise {@code false}
     */
    boolean putObject(String account, String container, String source, String process, String objectName, InputStream data);

    /**
     * Adds metadata to an existing object.
     *
     * @param account     the tenant account
     * @param container   the container name
     * @param source      the originating module
     * @param process     the process context
     * @param objectName  the name of the object
     * @param metadata    key-value pairs to add as metadata
     * @return the updated metadata map
     */
    Map<String, Object> addObjectMetaData(String account, String container, String source, String process,
                                          String objectName, Map<String, Object> metadata);

    /**
     * Adds a single metadata key-value pair to the object.
     *
     * @param account     the tenant account
     * @param container   the container name
     * @param source      the originating module
     * @param process     the process context
     * @param objectName  the name of the object
     * @param key         metadata key
     * @param value       metadata value
     * @return the updated metadata map
     */
    Map<String, Object> addObjectMetaData(String account, String container, String source, String process,
                                          String objectName, String key, String value);

    /**
     * Retrieves metadata for a given object or all objects in a container.
     *
     * @param account     the tenant account
     * @param container   the container name
     * @param source      the originating module
     * @param process     the process context
     * @param objectName  the name of the object (or {@code null} to retrieve all metadata)
     * @return metadata map keyed by object name
     */
    Map<String, Object> getMetaData(String account, String container, String source, String process,
                                    String objectName);

    /**
     * Increments a numeric metadata value associated with an object.
     *
     * @param account      the tenant account
     * @param container    the container name
     * @param source       the originating module
     * @param process      the process context
     * @param objectName   the name of the object
     * @param metaDataKey  the key whose value should be incremented
     * @return the new incremented value
     */
    Integer incMetadata(String account, String container, String source, String process,
                        String objectName, String metaDataKey);

    /**
     * Decrements a numeric metadata value associated with an object.
     *
     * @param account      the tenant account
     * @param container    the container name
     * @param source       the originating module
     * @param process      the process context
     * @param objectName   the name of the object
     * @param metaDataKey  the key whose value should be decremented
     * @return the new decremented value
     */
    Integer decMetadata(String account, String container, String source, String process,
                        String objectName, String metaDataKey);

    /**
     * Deletes a specific object from the object store.
     *
     * @param account     the tenant account
     * @param container   the container name
     * @param source      the originating module
     * @param process     the process context
     * @param objectName  the name of the object
     * @return {@code true} if deletion is successful
     */
    boolean deleteObject(String account, String container, String source, String process, String objectName);

    /**
     * Removes a container and all its contents from the object store.
     *
     * @param account   the tenant account
     * @param container the container name
     * @param source    the originating module
     * @param process   the process context
     * @return {@code true} if removal is successful
     */
    boolean removeContainer(String account, String container, String source, String process);

    /**
     * Optional method to package or archive contents based on a reference ID.
     *
     * @param account   the tenant account
     * @param container the container name
     * @param source    the originating module
     * @param process   the process context
     * @param refId     the reference ID for packaging
     * @return {@code true} if packaging succeeds
     */
    boolean pack(String account, String container, String source, String process, String refId);

    /**
     * Retrieves all stored object metadata within a given container.
     *
     * @param account   the tenant account
     * @param container the container name
     * @return a list of object metadata
     */
    List<ObjectDto> getAllObjects(String account, String container);

    /**
     * Adds tags to a container. Tags are key-value pairs applied at the container level.
     *
     * @param account   the tenant account
     * @param container the container name
     * @param tags      the map of tags to add
     * @return the resulting tag map after addition
     */
    Map<String, String> addTags(String account, String container, Map<String, String> tags);

    /**
     * Retrieves all tags associated with a container.
     *
     * @param account   the tenant account
     * @param container the container name
     * @return a map of tag key-value pairs
     */
    Map<String, String> getTags(String account, String container);

    /**
     * Deletes specified tags from a container.
     *
     * @param account   the tenant account
     * @param container the container name
     * @param tags      the list of tag keys to be deleted
     */
    void deleteTags(String account, String container, List<String> tags);
}
