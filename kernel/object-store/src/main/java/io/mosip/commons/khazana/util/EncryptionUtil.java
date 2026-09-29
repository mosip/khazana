package io.mosip.commons.khazana.util;

import io.mosip.commons.khazana.constant.KhazanaConstant;

/**
 * Packet layout helper shared by offline encryption.
 * <p>
 * {@link OfflineEncryptionUtil} calls {@link #mergeEncryptedData(byte[], byte[], byte[])}
 * after the kernel cryptomanager returns ciphertext. The on-disk form is nonce, then
 * additional authenticated data, then ciphertext, using the lengths in {@link KhazanaConstant}.
 * {@link OnlineCryptoUtil} builds the same layout itself.
 */
public class EncryptionUtil {

    /**
     * Concatenates nonce, AAD, and ciphertext into one packet.
     * <p>
     * The nonce occupies the first {@link KhazanaConstant#GCM_NONCE_LENGTH} bytes,
     * the AAD the next {@link KhazanaConstant#GCM_AAD_LENGTH} bytes, and the ciphertext
     * the remainder.
     *
     * @param encryptedData ciphertext returned by cryptomanager
     * @param nonce         GCM nonce (salt); expected length {@link KhazanaConstant#GCM_NONCE_LENGTH}
     * @param aad           GCM additional authenticated data; expected length {@link KhazanaConstant#GCM_AAD_LENGTH}
     * @return nonce, AAD, and ciphertext in that order
     */
    public static byte[] mergeEncryptedData(byte[] encryptedData, byte[] nonce, byte[] aad) {
        byte[] finalEncData = new byte[encryptedData.length + KhazanaConstant.GCM_AAD_LENGTH + KhazanaConstant.GCM_NONCE_LENGTH];
        System.arraycopy(nonce, 0, finalEncData, 0, nonce.length);
        System.arraycopy(aad, 0, finalEncData, nonce.length, aad.length);
        System.arraycopy(encryptedData, 0, finalEncData, nonce.length + aad.length,	encryptedData.length);
        return finalEncData;
    }
}
