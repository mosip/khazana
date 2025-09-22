package io.mosip.commons.khazana.util;

public final class ObjectStoreUtil {

    private static final String SEPARATOR = "/";

    private ObjectStoreUtil() {
        // Utility class – prevent instantiation
    }

    /**
     * Build path with source, process, and objectName.
     */
    public static String getName(String source, String process, String objectName) {
        return join(source, process, objectName);
    }

    /**
     * Build path with container, source, process, and objectName.
     */
    public static String getName(String container, String source, String process, String objectName) {
        return join(container, source, process, objectName);
    }

    /**
     * Build path with objectName and tagName.
     */
    public static String getName(String objectName, String tagName) {
        return join(objectName, tagName);
    }

    /**
     * High-performance joiner that skips null or empty parts and adds SEPARATOR between them.
     */
    private static String join(String... parts) {
        // Pre-size StringBuilder roughly to avoid repeated growth
        int estimatedLength = 0;
        for (String part : parts) {
            if (part != null && !part.isEmpty()) {
                estimatedLength += part.length() + 1; // +1 for separator
            }
        }

        StringBuilder sb = new StringBuilder(Math.max(estimatedLength, 16));
        for (String part : parts) {
            if (part != null && !part.isEmpty()) {
                if (sb.length() > 0) {
                    sb.append(SEPARATOR);
                }
                sb.append(part);
            }
        }
        return sb.toString();
    }
}
