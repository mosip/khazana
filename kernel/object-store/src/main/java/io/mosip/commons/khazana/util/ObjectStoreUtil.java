package io.mosip.commons.khazana.util;

import io.mosip.kernel.core.util.StringUtils;

/**
 * Builds object keys from Khazana path segments.
 * <p>
 * Adapters call these methods instead of concatenating paths themselves.
 * A null or empty segment is skipped; the remaining segments are joined with {@code /}.
 * The object name or tag name is always appended, even when it is empty.
 * <pre>
 * getName("src", "", "a")           -&gt; src/a
 * getName("acct", "src", "p", "a")  -&gt; acct/src/p/a
 * </pre>
 */
public class ObjectStoreUtil {

    /**
     * Separator placed between non-empty path segments.
     */
    private static final String SEPARATOR = "/";

    /**
     * Joins source, process, and object name into a key.
     * <p>
     * Used when the bucket is the container ({@code use.account.as.bucketname=false})
     * and for POSIX zip entry names.
     *
     * @param source     source segment; skipped when null or empty
     * @param process    process segment; skipped when null or empty
     * @param objectName object name; always appended
     * @return key such as {@code src/process/objectName}, with empty segments omitted
     */
    public static String getName(String source, String process, String objectName) {
        String finalObjectName = "";
        if (StringUtils.isNotEmpty(source))
            finalObjectName = source + SEPARATOR;
        if (StringUtils.isNotEmpty(process))
            finalObjectName = finalObjectName + process + SEPARATOR;

        finalObjectName = finalObjectName + objectName;

        return finalObjectName;
    }

    /**
     * Joins container, source, process, and object name into a key.
     * <p>
     * Used when the account is the S3 bucket ({@code use.account.as.bucketname=true})
     * so the container becomes the first key segment.
     *
     * @param container  container segment; skipped when null or empty
     * @param source     source segment; skipped when null or empty
     * @param process    process segment; skipped when null or empty
     * @param objectName object name; always appended
     * @return key such as {@code container/src/process/objectName}, with empty segments omitted
     */
    public static String getName(String container,String source, String process, String objectName) {
        String finalObjectName = "";
        if (StringUtils.isNotEmpty(container))
            finalObjectName = container + SEPARATOR;
        if (StringUtils.isNotEmpty(source))
            finalObjectName = finalObjectName + source + SEPARATOR;
        if (StringUtils.isNotEmpty(process))
            finalObjectName = finalObjectName + process + SEPARATOR;

        finalObjectName = finalObjectName + objectName;

        return finalObjectName;
    }
    
    /**
     * Joins an object prefix and a tag name.
     * <p>
     * {@link io.mosip.commons.khazana.impl.S3Adapter} uses this to store each tag as
     * {@code Tags/<tag>} or {@code <container>/Tags/<tag>}.
     *
     * @param objectName prefix; skipped when null or empty
     * @param tagName    tag name; appended when not null or empty
     * @return {@code objectName/tagName}, or whichever argument is non-empty
     */
    public static String getName(String objectName,String tagName) {
 	   String finalObjectName = "";
 	   if (StringUtils.isNotEmpty(objectName))
            finalObjectName = objectName + SEPARATOR;
 	   if (StringUtils.isNotEmpty(tagName))
            finalObjectName = finalObjectName + tagName;
 	   return finalObjectName;
 }
}
