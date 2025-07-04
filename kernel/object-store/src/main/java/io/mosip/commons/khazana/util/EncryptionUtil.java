package io.mosip.commons.khazana.util;

import io.mosip.commons.khazana.constant.KhazanaConstant;

/**
 * {@code EncryptionUtil} provides low-level utility methods for handling
 * encryption output packaging, particularly in AES-GCM mode.
 *
 * <p>
 * This class focuses on merging the nonce, AAD (Additional Authenticated Data),
 * and encrypted data into a single byte array, which is commonly required for
 * compact storage or transmission of encrypted payloads.
 * </p>
 *
 * <p>Structure of final byte array:</p>
 * <pre>
 * [ nonce (12 bytes) ][ aad (32 bytes) ][ encrypted data (variable length) ]
 * </pre>
 *
 * <p>Example usage:</p>
 * <pre>
 * byte[] combined = EncryptionUtil.mergeEncryptedData(ciphertext, nonce, aad);
 * </pre>
 *
 * @author MOSIP Team
 * @see KhazanaConstant#GCM_NONCE_LENGTH
 * @see KhazanaConstant#GCM_AAD_LENGTH
 */
public class EncryptionUtil {

    /**
     * Merges nonce, AAD, and encrypted payload into a single byte array.
     *
     * <p>This format is typically used for transmitting or storing encrypted
     * data along with its associated cryptographic parameters in a compact form.</p>
     *
     * @param encryptedData the AES-GCM encrypted data (ciphertext)
     * @param nonce         the nonce (IV) used during encryption, typically 12 bytes
     * @param aad           additional authenticated data used in GCM authentication, typically 32 bytes
     * @return the final byte array combining nonce, AAD, and encrypted data
     */
    public static byte[] mergeEncryptedData(byte[] encryptedData, byte[] nonce, byte[] aad) {
        byte[] finalEncData = new byte[
            encryptedData.length + KhazanaConstant.GCM_AAD_LENGTH + KhazanaConstant.GCM_NONCE_LENGTH
        ];

        // Copy nonce at the beginning
        System.arraycopy(nonce, 0, finalEncData, 0, nonce.length);

        // Copy AAD after nonce
        System.arraycopy(aad, 0, finalEncData, nonce.length, aad.length);

        // Copy encrypted data after nonce and AAD
        System.arraycopy(encryptedData, 0, finalEncData, nonce.length + aad.length, encryptedData.length);

        return finalEncData;
    }
}
