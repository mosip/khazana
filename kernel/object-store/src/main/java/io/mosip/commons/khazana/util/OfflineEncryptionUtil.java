package io.mosip.commons.khazana.util;

import io.mosip.commons.khazana.constant.KhazanaConstant;
import io.mosip.kernel.core.util.CryptoUtil;
import io.mosip.kernel.core.util.DateUtils;
import io.mosip.kernel.cryptomanager.dto.CryptomanagerRequestDto;
import io.mosip.kernel.cryptomanager.service.CryptomanagerService;
import io.mosip.kernel.cryptomanager.service.impl.CryptomanagerServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.security.SecureRandom;

/**
 * {@code OfflineEncryptionUtil} provides a utility to perform offline encryption
 * using the {@link CryptomanagerService}, generating random nonce and AAD values
 * and packaging them with the encrypted data using GCM mode.
 *
 * <p>
 * This is typically used in offline or air-gapped registration kits where data
 * must be encrypted locally before transmission or storage.
 * </p>
 *
 * <p>The final payload layout is:</p>
 * <pre>
 * [nonce (12 bytes)] + [AAD (32 bytes)] + [encrypted payload (variable)]
 * </pre>
 *
 * <p>Configuration properties:</p>
 * <ul>
 *     <li>{@code mosip.utc-datetime-pattern}</li>
 *     <li>{@code mosip.sign.applicationid}</li>
 *     <li>{@code mosip.sign.refid}</li>
 *     <li>{@code mosip.kernel.registrationcenterid.length}</li>
 *     <li>{@code mosip.kernel.machineid.length}</li>
 *     <li>{@code crypto.PrependThumbprint.enable}</li>
 * </ul>
 * 
 * @author MOSIP Team
 */
@Component
public class OfflineEncryptionUtil {

    /**
     * Default application ID used in the Cryptomanager request.
     */
    public static final String APPLICATION_ID = "REGISTRATION";

    @Autowired
    private ApplicationContext applicationContext;

    @Value("${mosip.utc-datetime-pattern:yyyy-MM-dd'T'HH:mm:ss.SSS'Z'}")
    private String DATETIME_PATTERN;

    /** Cryptomanager service bean (lazy-loaded). */
    private CryptomanagerService cryptomanagerService = null;

    @Value("${mosip.sign.applicationid:KERNEL}")
    private String signApplicationid;

    @Value("${mosip.sign.refid:SIGN}")
    private String signRefid;

    @Value("${mosip.kernel.registrationcenterid.length:5}")
    private int centerIdLength;

    @Value("${mosip.kernel.machineid.length:5}")
    private int machineIdLength;

    @Value("${crypto.PrependThumbprint.enable:true}")
    private boolean isPrependThumbprintEnabled;

    /**
     * Encrypts the given packet using AES-GCM with randomly generated nonce and AAD.
     * The encrypted data is Base64-decoded and combined with the nonce and AAD into a single byte array.
     *
     * @param refId  the reference ID used in the cryptographic context (e.g., transaction or packet ID)
     * @param packet the raw byte array to encrypt
     * @return the merged encrypted payload in the format: nonce + aad + ciphertext
     */
    public byte[] encrypt(String refId, byte[] packet) {
        String packetString = CryptoUtil.encodeBase64String(packet);

        CryptomanagerRequestDto cryptomanagerRequestDto = new CryptomanagerRequestDto();
        cryptomanagerRequestDto.setApplicationId(APPLICATION_ID);
        cryptomanagerRequestDto.setData(packetString);
        cryptomanagerRequestDto.setPrependThumbprint(isPrependThumbprintEnabled);
        cryptomanagerRequestDto.setReferenceId(refId);

        // Generate random nonce and AAD
        SecureRandom secureRandom = new SecureRandom();
        byte[] nonce = new byte[KhazanaConstant.GCM_NONCE_LENGTH];
        byte[] aad = new byte[KhazanaConstant.GCM_AAD_LENGTH];
        secureRandom.nextBytes(nonce);
        secureRandom.nextBytes(aad);

        cryptomanagerRequestDto.setSalt(CryptoUtil.encodeBase64String(nonce));
        cryptomanagerRequestDto.setAad(CryptoUtil.encodeBase64String(aad));
        cryptomanagerRequestDto.setTimeStamp(DateUtils.getUTCCurrentDateTime());

        // Perform encryption
        byte[] encryptedData = CryptoUtil
                .decodeBase64(getCryptomanagerService().encrypt(cryptomanagerRequestDto).getData());

        // Merge nonce + aad + encrypted payload
        return EncryptionUtil.mergeEncryptedData(encryptedData, nonce, aad);
    }

    /**
     * Lazily fetches the {@link CryptomanagerService} implementation from the Spring context.
     *
     * @return an instance of {@code CryptomanagerService}
     */
    private CryptomanagerService getCryptomanagerService() {
        if (cryptomanagerService == null) {
            cryptomanagerService = applicationContext.getBean(CryptomanagerServiceImpl.class);
        }
        return cryptomanagerService;
    }
}