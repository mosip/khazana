package io.mosip.commons.khazana.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;
import java.io.Serializable;

/**
 * One object returned by {@link io.mosip.commons.khazana.spi.ObjectStoreAdapter#getAllObjects}.
 * <p>
 * {@link io.mosip.commons.khazana.impl.S3Adapter} fills this from the object key:
 * one path segment is the object name, two segments are source and object name,
 * and three segments are source, process, and object name. Tag objects are skipped.
 * {@code PosixAdapter} and {@code SwiftAdapter} do not list objects and return {@code null}.
 * <p>
 * Lombok generates a no-args constructor, an all-args constructor, accessors, and
 * {@code equals}, {@code hashCode}, and {@code toString}.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ObjectDto implements Serializable {

    /**
     * Source path segment, or {@code null} when the key has only an object name.
     */
    private String source;

    /**
     * Process path segment, or {@code null} when the key has fewer than three segments.
     */
    private String process;

    /**
     * Object name, the last segment of the stored key.
     */
    private String objectName;

    /**
     * Last-modified time reported by the object store.
     */
    private Date lastModified;
}
