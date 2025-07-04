package io.mosip.commons.khazana.util;

import java.io.File;

import io.mosip.kernel.core.util.StringUtils;

/**
 * {@code ObjectStoreUtil} provides helper methods to construct hierarchical
 * object names (keys) for storing and retrieving data from an object store.
 *
 * <p>
 * It combines various input fields (like container, source, process, and object name)
 * into a path-like structure using the system-specific file separator (usually "/").
 * </p>
 *
 * <p>Example output:</p>
 * <pre>
 * "registration/biometrics/fingerprint-1234"
 * </pre>
 *
 * <p>This format is useful when simulating folder structures in flat key-value
 * object stores such as OpenStack Swift or AWS S3.</p>
 * 
 * @author MOSIP Team
 */
public class ObjectStoreUtil {

    /**
     * System-specific file separator, used to construct object paths.
     */
    private static final String SEPARATOR = File.separator;

    /**
     * Constructs a storage object name using source, process, and object name.
     *
     * @param source     the module that generated the object (e.g., "registration")
     * @param process    the process or type of data (e.g., "biometrics")
     * @param objectName the base name of the object (e.g., "fingerprint-1234")
     * @return combined object path in the format: {@code source/process/objectName}
     */
    public static String getName(String source, String process, String objectName) {
        String finalObjectName = "";

        if (StringUtils.isNotEmpty(source)) {
            finalObjectName = source + SEPARATOR;
        }

        if (StringUtils.isNotEmpty(process)) {
            finalObjectName += process + SEPARATOR;
        }

        finalObjectName += objectName;

        return finalObjectName;
    }

    /**
     * Constructs a full object name using container, source, process, and object name.
     *
     * @param container  the container or bucket name (e.g., "mosip-dev-objstore")
     * @param source     the module name (e.g., "auth-service")
     * @param process    the process name (e.g., "tokens")
     * @param objectName the base object key
     * @return full object path in the format: {@code container/source/process/objectName}
     */
    public static String getName(String container, String source, String process, String objectName) {
        String finalObjectName = "";

        if (StringUtils.isNotEmpty(container)) {
            finalObjectName = container + SEPARATOR;
        }

        if (StringUtils.isNotEmpty(source)) {
            finalObjectName += source + SEPARATOR;
        }

        if (StringUtils.isNotEmpty(process)) {
            finalObjectName += process + SEPARATOR;
        }

        finalObjectName += objectName;

        return finalObjectName;
    }

    /**
     * Constructs a metadata tag path by appending a tag to an object name.
     *
     * @param objectName the base object name
     * @param tagName    the tag name to append (e.g., "Tags")
     * @return full tag path in the format: {@code objectName/tagName}
     */
    public static String getName(String objectName, String tagName) {
        String finalObjectName = "";

        if (StringUtils.isNotEmpty(objectName)) {
            finalObjectName = objectName + SEPARATOR;
        }

        if (StringUtils.isNotEmpty(tagName)) {
            finalObjectName += tagName;
        }

        return finalObjectName;
    }
}