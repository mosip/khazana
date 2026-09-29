package io.mosip.commons.khazana.constant;

/**
 * Shared lengths and names used by Khazana encryption and tag storage.
 * <p>
 * {@link io.mosip.commons.khazana.util.OnlineCryptoUtil},
 * {@link io.mosip.commons.khazana.util.OfflineEncryptionUtil}, and
 * {@link io.mosip.commons.khazana.util.EncryptionUtil} size the GCM nonce and
 * additional authenticated data from these lengths and lay the encrypted packet
 * out as nonce, then AAD, then ciphertext.
 * {@link io.mosip.commons.khazana.impl.S3Adapter} stores tags as objects under
 * {@link #TAGS_FILENAME}.
 */
public class KhazanaConstant {

    /**
     * Length, in bytes, of the GCM nonce (salt) prepended to an encrypted packet.
     */
    public static final int GCM_NONCE_LENGTH = 12;

    /**
     * Length, in bytes, of the GCM additional authenticated data written after the nonce.
     */
    public static final int GCM_AAD_LENGTH = 32;

    /**
     * Status value recorded when a signature operation succeeds.
     */
    public static final String SIGNATURES_SUCCESS = "success";

    /**
     * Path segment under which S3 tag payloads are stored. Not a native S3 object tag.
     */
    public static String TAGS_FILENAME="Tags";
}
