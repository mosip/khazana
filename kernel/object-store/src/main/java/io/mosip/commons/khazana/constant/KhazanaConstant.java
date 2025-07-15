package io.mosip.commons.khazana.constant;

/**
 * {@code KhazanaConstant} contains static constant values used across the Khazana module.
 * These constants define cryptographic parameters, response strings, and metadata file naming conventions
 * used primarily in the context of object storage operations such as uploading metadata, tagging, and encryption.
 * 
 * <p>
 * This class is not meant to be instantiated.
 * </p>
 * 
 * <p>
 * Example usage:
 * <pre>
 *     byte[] nonce = new byte[KhazanaConstant.GCM_NONCE_LENGTH];
 *     String result = KhazanaConstant.SIGNATURES_SUCCESS;
 * </pre>
 * </p>
 * 
 * @author MOSIP Team
 */
public class KhazanaConstant {

    /**
     * Length (in bytes) of the Galois/Counter Mode (GCM) Nonce (also known as IV).
     * GCM is used in AES encryption to ensure data confidentiality and integrity.
     * <p>Standard recommended value is 12 bytes (96 bits).</p>
     */
    public static final int GCM_NONCE_LENGTH = 12;

    /**
     * Length (in bytes) of the Additional Authenticated Data (AAD) used in GCM encryption.
     * AAD is authenticated but not encrypted and is typically used to bind context (e.g., headers) to the encrypted data.
     */
    public static final int GCM_AAD_LENGTH = 32;

    /**
     * Constant string representing a successful result in digital signature or tagging operations.
     * Commonly used to validate and acknowledge completion of cryptographic or metadata processes.
     */
    public static final String SIGNATURES_SUCCESS = "success";

    /**
     * Default filename used when storing or retrieving metadata tags associated with a stored object or container.
     * This may refer to a logical or physical file that maintains key-value tags applied to stored data.
     */
    public static final String TAGS_FILENAME = "Tags";

    // Private constructor to prevent instantiation
    private KhazanaConstant() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }
}
