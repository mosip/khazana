package io.mosip.commons.khazana.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * {@code EncryptionHelper} is a utility component responsible for abstracting
 * the decision logic between online and offline encryption mechanisms.
 *
 * <p>
 * Based on the configured `objectstore.crypto.name` property, it delegates encryption
 * requests to either {@link OfflineEncryptionUtil} or {@link OnlineCryptoUtil}.
 * This design enables pluggable cryptographic behavior depending on the deployment
 * scenario (e.g., offline field devices vs. online processing centers).
 * </p>
 *
 * <p>Example usage:</p>
 * <pre>
 *     byte[] encryptedData = encryptionHelper.encrypt("ref123", packetBytes);
 * </pre>
 * 
 * <p>Spring Boot configuration example:</p>
 * <pre>
 * objectstore.crypto.name=OfflinePacketCryptoServiceImpl
 * </pre>
 * 
 * @author MOSIP Team
 * @see OfflineEncryptionUtil
 * @see OnlineCryptoUtil
 */
@Component
public class EncryptionHelper {

    /**
     * Constant representing the default crypto implementation name for offline encryption.
     */
    private static final String CRYPTO = "OfflinePacketCryptoServiceImpl";

    /**
     * Configurable crypto implementation name. Defaults to {@code OfflinePacketCryptoServiceImpl}.
     * Determines which encryption strategy to use.
     */
    @Value("${objectstore.crypto.name:OfflinePacketCryptoServiceImpl}")
    private String cryptoName;

    /**
     * Offline encryption utility (typically used in field registration kits or offline mode).
     */
    @Autowired
    private OfflineEncryptionUtil offlineEncryptionUtil;

    /**
     * Online encryption utility (used when connectivity and central encryption are required).
     */
    @Autowired
    private OnlineCryptoUtil onlineCryptoUtil;

    /**
     * Encrypts the given byte array using the configured crypto strategy.
     *
     * @param refId  the reference ID for the packet (used in key derivation or audit)
     * @param packet the raw data to be encrypted
     * @return encrypted byte array
     */
    public byte[] encrypt(String refId, byte[] packet) {
        if (cryptoName.equalsIgnoreCase(CRYPTO)) {
            return offlineEncryptionUtil.encrypt(refId, packet);
        } else {
            return onlineCryptoUtil.encrypt(refId, packet);
        }
    }
}
