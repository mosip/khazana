package io.mosip.commons.khazana.util;

/**
 * Utility for building object store paths.
 *
 * IMPORTANT:
 * - Preserves original MOSIP behavior
 * - Does NOT collapse mandatory path depth implicitly
 * - Performance-optimized (single StringBuilder, no concat chains)
 *
 * Domain guarantees (e.g., placeholder handling) must be enforced by caller
 * (S3Adapter), not by this utility.
 */
public final class ObjectStoreUtil {

    private static final String SEPARATOR = "/";

    private ObjectStoreUtil() {
        // prevent instantiation
    }

    /**
     * Build path with source, process, and objectName.
     * Original behavior preserved:
     *   source/process/objectName
     *   process omitted if null/empty
     *   objectName ALWAYS appended
     */
    public static String getName(String source, String process, String objectName) {
        StringBuilder sb = new StringBuilder(estimate(source, process, objectName));

        if (isNotEmpty(source)) {
            sb.append(source).append(SEPARATOR);
        }
        if (isNotEmpty(process)) {
            sb.append(process).append(SEPARATOR);
        }

        // objectName is mandatory (as per original contract)
        sb.append(objectName);

        return sb.toString();
    }

    /**
     * Build path with container, source, process, and objectName.
     * Original behavior preserved:
     *   container/source/process/objectName
     */
    public static String getName(String container, String source, String process, String objectName) {
        StringBuilder sb = new StringBuilder(estimate(container, source, process, objectName));

        if (isNotEmpty(container)) {
            sb.append(container).append(SEPARATOR);
        }
        if (isNotEmpty(source)) {
            sb.append(source).append(SEPARATOR);
        }
        if (isNotEmpty(process)) {
            sb.append(process).append(SEPARATOR);
        }

        sb.append(objectName);

        return sb.toString();
    }

    /**
     * Build path for tags.
     * Original behavior preserved:
     *   objectName/tagName
     */
    public static String getName(String objectName, String tagName) {
        StringBuilder sb = new StringBuilder(estimate(objectName, tagName));

        if (isNotEmpty(objectName)) {
            sb.append(objectName).append(SEPARATOR);
        }
        if (isNotEmpty(tagName)) {
            sb.append(tagName);
        }

        return sb.toString();
    }

    /* ───────────── Internal helpers ───────────── */

    private static boolean isNotEmpty(String s) {
        return s != null && !s.isEmpty();
    }

    /**
     * Rough capacity estimation to avoid StringBuilder resizing.
     */
    private static int estimate(String... parts) {
        int len = 0;
        for (String p : parts) {
            if (p != null) {
                len += p.length() + 1;
            }
        }
        return Math.max(len, 16);
    }
}
