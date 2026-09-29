package io.mosip.commons.khazana.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Selects offline or online packet encryption for {@link io.mosip.commons.khazana.impl.PosixAdapter#pack}.
 * <p>
 * When {@code objectstore.crypto.name} equals {@code OfflinePacketCryptoServiceImpl}
 * (the default), encryption stays in-process through {@link OfflineEncryptionUtil}.
 * Any other name calls cryptomanager over HTTP through {@link OnlineCryptoUtil}.
 */
@Component
public class EncryptionHelper {

    /**
     * Crypto implementation name that selects in-process encryption.
     */
    private static final String CRYPTO = "OfflinePacketCryptoServiceImpl";

    /**
     * Active crypto implementation name.
     * Property {@code objectstore.crypto.name}, default {@code OfflinePacketCryptoServiceImpl}.
     */
    @Value("${objectstore.crypto.name:OfflinePacketCryptoServiceImpl}")
    private String cryptoName;

    /**
     * In-process encryptor used when {@link #cryptoName} matches {@link #CRYPTO}.
     */
    @Autowired
    private OfflineEncryptionUtil offlineEncryptionUtil;

    /**
     * HTTP cryptomanager encryptor used for any other {@link #cryptoName}.
     */
    @Autowired
    private OnlineCryptoUtil onlineCryptoUtil;


    /**
     * Encrypts a packet with the configured crypto implementation.
     *
     * @param refId  reference id of the key passed to cryptomanager
     * @param packet plaintext packet bytes
     * @return encrypted packet (nonce, AAD, and ciphertext)
     * @throws io.mosip.commons.khazana.exception.ObjectStoreAdapterException
     *         when online encryption fails
     */
    public byte[] encrypt(String refId, byte[] packet) {
        if (cryptoName.equalsIgnoreCase(CRYPTO))
            return offlineEncryptionUtil.encrypt(refId, packet);
        else
            return onlineCryptoUtil.encrypt(refId, packet);
    }

}
