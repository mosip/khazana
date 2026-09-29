package io.mosip.commons.khazana.util;

import java.security.SecureRandom;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.mosip.commons.khazana.constant.KhazanaConstant;
import io.mosip.commons.khazana.dto.CryptomanagerRequestDto;
import io.mosip.commons.khazana.dto.CryptomanagerResponseDto;
import io.mosip.commons.khazana.exception.ObjectStoreAdapterException;
import io.mosip.kernel.core.exception.ServiceError;
import io.mosip.kernel.core.http.RequestWrapper;
import io.mosip.kernel.core.util.CryptoUtil;
import io.mosip.kernel.core.util.DateUtils2;

/**
 * HTTP cryptomanager client used when {@link EncryptionHelper} does not select offline encryption.
 * <p>
 * Encrypt and decrypt POST a {@link CryptomanagerRequestDto} and read a
 * {@link CryptomanagerResponseDto}. The packet layout is nonce
 * ({@link KhazanaConstant#GCM_NONCE_LENGTH} bytes), then AAD
 * ({@link KhazanaConstant#GCM_AAD_LENGTH} bytes), then ciphertext.
 * A cryptomanager error list, or any other failure, is thrown as
 * {@link ObjectStoreAdapterException}.
 */
@Component
public class OnlineCryptoUtil {

    /**
     * Application id sent on every cryptomanager request.
     */
    public static final String APPLICATION_ID = "REGISTRATION";

    /**
     * Request id sent as {@code request.id}. Value {@code mosip.cryptomanager.decrypt}.
     */
    private static final String DECRYPT_SERVICE_ID = "mosip.cryptomanager.decrypt";

    /**
     * Message used when encrypt fails while reading or calling cryptomanager.
     */
    private static final String IO_EXCEPTION = "Exception while reading packet inputStream";

    /**
     * Message reserved for a packet timestamp that cannot be parsed.
     */
    private static final String DATE_TIME_EXCEPTION = "Error while parsing packet timestamp";

    /**
     * UTC datetime pattern used for {@code requesttime}.
     * Property {@code mosip.utc-datetime-pattern}, default {@code yyyy-MM-dd'T'HH:mm:ss.SSS'Z'}.
     */
    @Value("${mosip.utc-datetime-pattern:yyyy-MM-dd'T'HH:mm:ss.SSS'Z'}")
    private String DATETIME_PATTERN;

    /**
     * Cryptomanager request version.
     * Property {@code mosip.kernel.cryptomanager.request_version}, default {@code v1}.
     */
    @Value("${mosip.kernel.cryptomanager.request_version:v1}")
    private String APPLICATION_VERSION;

    /**
     * Registration-center id length.
     * Property {@code mosip.kernel.registrationcenterid.length}, default {@code 5}.
     */
    @Value("${mosip.kernel.registrationcenterid.length:5}")
    private int centerIdLength;

    /**
     * Decrypt endpoint.
     * Property {@code CRYPTOMANAGER_DECRYPT}, default {@code null}.
     */
    @Value("${CRYPTOMANAGER_DECRYPT:null}")
    private String cryptomanagerDecryptUrl;

    /**
     * Machine id length.
     * Property {@code mosip.kernel.machineid.length}, default {@code 5}.
     */
    @Value("${mosip.kernel.machineid.length:5}")
    private int machineIdLength;

    /**
     * Encrypt endpoint.
     * Property {@code CRYPTOMANAGER_ENCRYPT}, default {@code null}.
     */
    @Value("${CRYPTOMANAGER_ENCRYPT:null}")
    private String cryptomanagerEncryptUrl;

    /**
     * Whether cryptomanager prepends the certificate thumbprint.
     * Property {@code crypto.PrependThumbprint.enable}, default {@code true}.
     */
    @Value("${crypto.PrependThumbprint.enable:true}")
    private boolean isPrependThumbprintEnabled;

    /**
     * Jackson mapper used to read the cryptomanager JSON body.
     */
    @Autowired
    private ObjectMapper mapper;

    /**
     * Spring context used to look up the {@code selfTokenRestTemplate} bean.
     */
    @Autowired
    private ApplicationContext applicationContext;

    /**
     * REST client for cryptomanager. Encrypt loads it through {@link #getRestTemplate()};
     * decrypt uses this field directly.
     */
    private RestTemplate restTemplate = null;

    /**
     * Encrypts a packet by calling the cryptomanager encrypt URL.
     * <p>
     * Generates a nonce and AAD, posts the Base64 plaintext, and returns
     * nonce + AAD + ciphertext.
     *
     * @param refId  reference id of the encryption key
     * @param packet plaintext packet bytes
     * @return encrypted packet bytes
     * @throws ObjectStoreAdapterException when the call fails or cryptomanager returns an error
     */
    public byte[] encrypt(String refId, byte[] packet) {
        byte[] encryptedPacket = null;

        try {
            String packetString = CryptoUtil.encodeBase64String(packet);
            CryptomanagerRequestDto cryptomanagerRequestDto = new CryptomanagerRequestDto();
            RequestWrapper<CryptomanagerRequestDto> request = new RequestWrapper<>();
            cryptomanagerRequestDto.setApplicationId(APPLICATION_ID);
            cryptomanagerRequestDto.setData(packetString);
            cryptomanagerRequestDto.setReferenceId(refId);
            cryptomanagerRequestDto.setPrependThumbprint(isPrependThumbprintEnabled);

            SecureRandom sRandom = new SecureRandom();
            byte[] nonce = new byte[KhazanaConstant.GCM_NONCE_LENGTH];
            byte[] aad = new byte[KhazanaConstant.GCM_AAD_LENGTH];
            sRandom.nextBytes(nonce);
            sRandom.nextBytes(aad);
            cryptomanagerRequestDto.setAad(CryptoUtil.encodeBase64String(aad));
            cryptomanagerRequestDto.setSalt(CryptoUtil.encodeBase64String(nonce));
            cryptomanagerRequestDto.setTimeStamp(DateUtils2.getUTCCurrentDateTime());

            request.setId(DECRYPT_SERVICE_ID);
            request.setMetadata(null);
            request.setRequest(cryptomanagerRequestDto);
            DateTimeFormatter format = DateTimeFormatter.ofPattern(DATETIME_PATTERN);
            LocalDateTime localdatetime = LocalDateTime
                    .parse(DateUtils2.getUTCCurrentDateTimeString(DATETIME_PATTERN), format);
            request.setRequesttime(localdatetime);
            request.setVersion(APPLICATION_VERSION);
            HttpEntity<RequestWrapper<CryptomanagerRequestDto>> httpEntity = new HttpEntity<>(request);

            ResponseEntity<String> response = getRestTemplate().exchange(cryptomanagerEncryptUrl, HttpMethod.POST, httpEntity, String.class);
            CryptomanagerResponseDto responseObject = mapper.readValue(response.getBody(), CryptomanagerResponseDto.class);
            if (responseObject != null &&
                    responseObject.getErrors() != null && !responseObject.getErrors().isEmpty()) {
                ServiceError error = responseObject.getErrors().get(0);
                throw new ObjectStoreAdapterException("", error.getMessage());
            }
            encryptedPacket = responseObject.getResponse().getData().getBytes();
            byte[] encryptedData = CryptoUtil.decodeBase64(responseObject.getResponse().getData());
            encryptedPacket = mergeEncryptedData(encryptedData, nonce, aad);
        } catch (Exception e) {
            throw new ObjectStoreAdapterException("", IO_EXCEPTION, e);
        }
        return encryptedPacket;
    }

    /**
     * Prefixes ciphertext with the nonce and AAD used for the encrypt request.
     *
     * @param encryptedData decoded ciphertext from cryptomanager
     * @param nonce         GCM nonce that was sent as the salt
     * @param aad           GCM additional authenticated data that was sent with the request
     * @return nonce, AAD, and ciphertext in that order
     */
    private byte[] mergeEncryptedData(byte[] encryptedData, byte[] nonce, byte[] aad) {
        byte[] finalEncData = new byte[encryptedData.length + KhazanaConstant.GCM_AAD_LENGTH + KhazanaConstant.GCM_NONCE_LENGTH];
        System.arraycopy(nonce, 0, finalEncData, 0, nonce.length);
        System.arraycopy(aad, 0, finalEncData, nonce.length, aad.length);
        System.arraycopy(encryptedData, 0, finalEncData, nonce.length + aad.length,	encryptedData.length);
        return finalEncData;
    }

    /**
     * Returns the authenticated REST client, loading bean {@code selfTokenRestTemplate} on first use.
     *
     * @return REST client used for the encrypt call
     */
    private RestTemplate getRestTemplate() {
        if (restTemplate == null)
			restTemplate = (RestTemplate) applicationContext.getBean("selfTokenRestTemplate");
        return restTemplate;
    }


    /**
     * Decrypts a packet by calling the cryptomanager decrypt URL.
     * <p>
     * Splits the packet into nonce, AAD, and ciphertext, posts those values as Base64,
     * and returns the decoded plaintext.
     *
     * @param refId  reference id of the decryption key
     * @param packet encrypted packet (nonce, AAD, ciphertext)
     * @return decrypted packet bytes
     * @throws ObjectStoreAdapterException when the call fails or cryptomanager returns an error
     */
    public byte[] decrypt(String refId, byte[] packet) {
        byte[] decryptedPacket = null;

        try {
            CryptomanagerRequestDto cryptomanagerRequestDto = new CryptomanagerRequestDto();
            RequestWrapper<CryptomanagerRequestDto> request = new RequestWrapper<>();
            cryptomanagerRequestDto.setApplicationId(APPLICATION_ID);
            cryptomanagerRequestDto.setReferenceId(refId);
            byte[] nonce = Arrays.copyOfRange(packet, 0, KhazanaConstant.GCM_NONCE_LENGTH);
            byte[] aad = Arrays.copyOfRange(packet, KhazanaConstant.GCM_NONCE_LENGTH,
                    KhazanaConstant.GCM_NONCE_LENGTH + KhazanaConstant.GCM_AAD_LENGTH);
            byte[] encryptedData = Arrays.copyOfRange(packet, KhazanaConstant.GCM_NONCE_LENGTH + KhazanaConstant.GCM_AAD_LENGTH,
                    packet.length);
            cryptomanagerRequestDto.setAad(CryptoUtil.encodeBase64String(aad));
            cryptomanagerRequestDto.setSalt(CryptoUtil.encodeBase64String(nonce));
            cryptomanagerRequestDto.setData(CryptoUtil.encodeBase64String(encryptedData));
            cryptomanagerRequestDto.setPrependThumbprint(isPrependThumbprintEnabled);
            cryptomanagerRequestDto.setTimeStamp(DateUtils2.getUTCCurrentDateTime());

            request.setId(DECRYPT_SERVICE_ID);
            request.setMetadata(null);
            request.setRequest(cryptomanagerRequestDto);
            DateTimeFormatter format = DateTimeFormatter.ofPattern(DATETIME_PATTERN);
            LocalDateTime localdatetime = LocalDateTime
                    .parse(DateUtils2.getUTCCurrentDateTimeString(DATETIME_PATTERN), format);
            request.setRequesttime(localdatetime);
            request.setVersion(APPLICATION_VERSION);
            HttpEntity<RequestWrapper<CryptomanagerRequestDto>> httpEntity = new HttpEntity<>(request);

            ResponseEntity<String> response = restTemplate.exchange(cryptomanagerDecryptUrl, HttpMethod.POST, httpEntity, String.class);

            CryptomanagerResponseDto responseObject = mapper.readValue(response.getBody(), CryptomanagerResponseDto.class);

            if (responseObject != null &&
                    responseObject.getErrors() != null && !responseObject.getErrors().isEmpty()) {
                ServiceError error = responseObject.getErrors().get(0);
                throw new ObjectStoreAdapterException("",error.getMessage());
            }
            decryptedPacket = CryptoUtil.decodeBase64(responseObject.getResponse().getData());

        } catch (Exception e) {
            throw new ObjectStoreAdapterException("", "",e);
        }
        return decryptedPacket;
    }
}
