package io.mosip.commons.khazana.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Location of one object in Khazana, used by
 * {@link io.mosip.commons.khazana.spi.ObjectStoreAdapter#moveObject}.
 * <p>
 * The five fields match the path arguments on the other adapter methods.
 * The default {@code moveObject} returns {@code false}; keys from
 * {@code listObjectsByPrefix} are intended to be placed in {@link #objectName}.
 * <p>
 * Lombok generates a no-args constructor, an all-args constructor, accessors, and
 * {@code equals}, {@code hashCode}, and {@code toString}.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ObjectStoreReference {

    /**
     * Object-store account. For S3 this is the bucket when
     * {@code object.store.s3.use.account.as.bucketname} is {@code true}.
     */
    private String account;

    /**
     * Container. For S3 this is the bucket when account-as-bucket mode is off,
     * and a key prefix when that mode is on.
     */
    private String container;

    /**
     * Source path segment. Empty and {@code null} segments are skipped by
     * {@link io.mosip.commons.khazana.util.ObjectStoreUtil#getName(String, String, String)}.
     */
    private String source;

    /**
     * Process path segment. Empty and {@code null} segments are skipped when the key is built.
     */
    private String process;

    /**
     * Object name. Listing results are container-relative or bucket-root keys
     * meant to be used here.
     */
    private String objectName;
}
