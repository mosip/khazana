package io.mosip.commons.khazana.util;

import io.mosip.commons.khazana.constant.KhazanaConstant;
import io.mosip.commons.khazana.constant.KhazanaErrorCodes;
import io.mosip.commons.khazana.exception.ObjectStoreAdapterException;
import io.mosip.kernel.core.util.CryptoUtil;
import io.mosip.kernel.core.util.DateUtils2;
import io.mosip.kernel.cryptomanager.dto.CryptomanagerRequestDto;
import io.mosip.kernel.cryptomanager.service.CryptomanagerService;
import io.mosip.kernel.cryptomanager.service.impl.CryptomanagerServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.security.SecureRandom;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * In-process packet encryption used when {@link EncryptionHelper} selects
 * {@code OfflinePacketCryptoServiceImpl}.
 * <p>
 * {@code encrypt} builds a kernel cryptomanager request, calls
 * {@link CryptomanagerService#encrypt}, and returns nonce, AAD, and ciphertext
 * through {@link EncryptionUtil#mergeEncryptedData(byte[], byte[], byte[])}.
 * {@link io.mosip.commons.khazana.impl.PosixAdapter#pack} reaches this class only
 * through {@link EncryptionHelper}.
 */
@Component
public class OfflineEncryptionUtil {
    /**
     * Application id sent on every cryptomanager encrypt request.
     */
    public static final String APPLICATION_ID = "REGISTRATION";

    /**
     * Spring context used to look up {@link CryptomanagerServiceImpl} on first use.
     */
    @Autowired
    private ApplicationContext applicationContext;

    /**
     * UTC datetime pattern.
     * Property {@code mosip.utc-datetime-pattern}, default {@code yyyy-MM-dd'T'HH:mm:ss.SSS'Z'}.
     */
    @Value("${mosip.utc-datetime-pattern:yyyy-MM-dd'T'HH:mm:ss.SSS'Z'}")
    private String DATETIME_PATTERN;

    /** The cryptomanager service. Looked up once from {@link #applicationContext}. */
    private CryptomanagerService cryptomanagerService = null;

    /** The sign applicationid. Property {@code mosip.sign.applicationid}, default {@code KERNEL}. */
    @Value("${mosip.sign.applicationid:KERNEL}")
    private String signApplicationid;

    /** The sign refid. Property {@code mosip.sign.refid}, default {@code SIGN}. */
    @Value("${mosip.sign.refid:SIGN}")
    private String signRefid;

    /**
     * Registration-center id length.
     * Property {@code mosip.kernel.registrationcenterid.length}, default {@code 5}.
     */
    @Value("${mosip.kernel.registrationcenterid.length:5}")
    private int centerIdLength;

    /**
     * Machine id length.
     * Property {@code mosip.kernel.machineid.length}, default {@code 5}.
     */
    @Value("${mosip.kernel.machineid.length:5}")
    private int machineIdLength;

    /**
     * Whether cryptomanager prepends the certificate thumbprint.
     * Property {@code crypto.PrependThumbprint.enable}, default {@code true}.
     */
    @Value("${crypto.PrependThumbprint.enable:true}")
    private boolean isPrependThumbprintEnabled;

    /**
     * Encrypts a packet with the in-process cryptomanager service.
     * <p>
     * A random nonce and AAD are generated, the plaintext is sent as Base64, and the
     * decoded ciphertext is merged into nonce + AAD + ciphertext.
     *
     * @param refId  reference id of the encryption key
     * @param packet plaintext packet bytes
     * @return encrypted packet bytes
     */
    public byte[] encrypt(String refId, byte[] packet) {
        String packetString = CryptoUtil.encodeBase64String(packet);
        CryptomanagerRequestDto cryptomanagerRequestDto = new CryptomanagerRequestDto();
        cryptomanagerRequestDto.setApplicationId(APPLICATION_ID);
        cryptomanagerRequestDto.setData(packetString);
        cryptomanagerRequestDto.setPrependThumbprint(isPrependThumbprintEnabled);
        cryptomanagerRequestDto.setReferenceId(refId);

        SecureRandom sRandom = new SecureRandom();
        byte[] nonce = new byte[KhazanaConstant.GCM_NONCE_LENGTH];
        byte[] aad = new byte[KhazanaConstant.GCM_AAD_LENGTH];
        sRandom.nextBytes(nonce);
        sRandom.nextBytes(aad);
        cryptomanagerRequestDto.setAad(CryptoUtil.encodeBase64String(aad));
        cryptomanagerRequestDto.setSalt(CryptoUtil.encodeBase64String(nonce));
        cryptomanagerRequestDto.setTimeStamp(DateUtils2.getUTCCurrentDateTime());

        byte[] encryptedData = CryptoUtil.decodeBase64(getCryptomanagerService().encrypt(cryptomanagerRequestDto).getData());
        return EncryptionUtil.mergeEncryptedData(encryptedData, nonce, aad);
    }

    /**
     * Returns the kernel cryptomanager service, creating it from the Spring context on first use.
     *
     * @return shared {@link CryptomanagerServiceImpl} bean
     */
    private CryptomanagerService getCryptomanagerService() {
        if (cryptomanagerService == null)
            cryptomanagerService = applicationContext.getBean(CryptomanagerServiceImpl.class);
        return cryptomanagerService;
    }
}
