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
import io.mosip.kernel.core.util.DateUtils;

/**
 * {@code OnlineCryptoUtil} handles encryption and decryption by invoking external
 * cryptographic services (e.g., MOSIP Cryptomanager APIs).
 *
 * <p>This class is typically used when operating in an online mode where
 * encryption/decryption operations are delegated to a centralized service.</p>
 *
 * <p>The data is encrypted using AES-GCM and structured as:</p>
 * <pre>
 * [ nonce (12 bytes) ][ aad (32 bytes) ][ encrypted payload ]
 * </pre>
 *
 * @author MOSIP Team
 */
@Component
public class OnlineCryptoUtil {

    public static final String APPLICATION_ID = "REGISTRATION";
    private static final String DECRYPT_SERVICE_ID = "mosip.cryptomanager.decrypt";
    private static final String IO_EXCEPTION = "Exception while reading packet inputStream";
	private static final String DATE_TIME_EXCEPTION = "Error while parsing packet timestamp";

    @Value("${mosip.utc-datetime-pattern:yyyy-MM-dd'T'HH:mm:ss.SSS'Z'}")
    private String DATETIME_PATTERN;

    @Value("${mosip.kernel.cryptomanager.request_version:v1}")
    private String APPLICATION_VERSION;

    @Value("${CRYPTOMANAGER_DECRYPT:null}")
    private String cryptomanagerDecryptUrl;

    @Value("${CRYPTOMANAGER_ENCRYPT:null}")
    private String cryptomanagerEncryptUrl;

    @Value("${crypto.PrependThumbprint.enable:true}")
    private boolean isPrependThumbprintEnabled;

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private ApplicationContext applicationContext;

    private RestTemplate restTemplate;

    /**
     * Encrypts the given data by invoking the Cryptomanager encrypt API.
     *
     * @param refId  a reference ID to trace the packet
     * @param packet the raw data to encrypt
     * @return the final encrypted byte array (nonce + aad + ciphertext)
     */
    public byte[] encrypt(String refId, byte[] packet) {
        try {
            String packetString = CryptoUtil.encodeBase64String(packet);

            CryptomanagerRequestDto requestDto = new CryptomanagerRequestDto();
            requestDto.setApplicationId(APPLICATION_ID);
            requestDto.setReferenceId(refId);
            requestDto.setData(packetString);
            requestDto.setPrependThumbprint(isPrependThumbprintEnabled);

            // Generate nonce and AAD
            SecureRandom random = new SecureRandom();
            byte[] nonce = new byte[KhazanaConstant.GCM_NONCE_LENGTH];
            byte[] aad = new byte[KhazanaConstant.GCM_AAD_LENGTH];
            random.nextBytes(nonce);
            random.nextBytes(aad);
            requestDto.setSalt(CryptoUtil.encodeBase64String(nonce));
            requestDto.setAad(CryptoUtil.encodeBase64String(aad));
            requestDto.setTimeStamp(DateUtils.getUTCCurrentDateTime());

            RequestWrapper<CryptomanagerRequestDto> requestWrapper = new RequestWrapper<>();
            requestWrapper.setId(DECRYPT_SERVICE_ID);
            requestWrapper.setMetadata(null);
            requestWrapper.setRequest(requestDto);
            requestWrapper.setRequesttime(LocalDateTime.parse(
                    DateUtils.getUTCCurrentDateTimeString(DATETIME_PATTERN),
                    DateTimeFormatter.ofPattern(DATETIME_PATTERN)
            ));
            requestWrapper.setVersion(APPLICATION_VERSION);

            HttpEntity<RequestWrapper<CryptomanagerRequestDto>> httpEntity = new HttpEntity<>(requestWrapper);
            ResponseEntity<String> response = getRestTemplate().exchange(
                    cryptomanagerEncryptUrl, HttpMethod.POST, httpEntity, String.class
            );

            CryptomanagerResponseDto responseObject = mapper.readValue(response.getBody(), CryptomanagerResponseDto.class);
            if (responseObject != null && responseObject.getErrors() != null && !responseObject.getErrors().isEmpty()) {
                ServiceError error = responseObject.getErrors().get(0);
                throw new ObjectStoreAdapterException("CM-ERR-001", error.getMessage());
            }

            byte[] encryptedData = CryptoUtil.decodeBase64(responseObject.getResponse().getData());
            return mergeEncryptedData(encryptedData, nonce, aad);
        } catch (Exception e) {
            throw new ObjectStoreAdapterException("CM-ERR-002", IO_EXCEPTION, e);
        }
    }

    /**
     * Decrypts the given encrypted payload using Cryptomanager's decrypt API.
     *
     * @param refId  a reference ID to trace the packet
     * @param packet the encrypted payload (nonce + aad + ciphertext)
     * @return the decrypted raw byte array
     */
    public byte[] decrypt(String refId, byte[] packet) {
        try {
            byte[] nonce = Arrays.copyOfRange(packet, 0, KhazanaConstant.GCM_NONCE_LENGTH);
            byte[] aad = Arrays.copyOfRange(packet, KhazanaConstant.GCM_NONCE_LENGTH,
                    KhazanaConstant.GCM_NONCE_LENGTH + KhazanaConstant.GCM_AAD_LENGTH);
            byte[] encryptedData = Arrays.copyOfRange(packet,
                    KhazanaConstant.GCM_NONCE_LENGTH + KhazanaConstant.GCM_AAD_LENGTH, packet.length);

            CryptomanagerRequestDto requestDto = new CryptomanagerRequestDto();
            requestDto.setApplicationId(APPLICATION_ID);
            requestDto.setReferenceId(refId);
            requestDto.setSalt(CryptoUtil.encodeBase64String(nonce));
            requestDto.setAad(CryptoUtil.encodeBase64String(aad));
            requestDto.setData(CryptoUtil.encodeBase64String(encryptedData));
            requestDto.setPrependThumbprint(isPrependThumbprintEnabled);
            requestDto.setTimeStamp(DateUtils.getUTCCurrentDateTime());

            RequestWrapper<CryptomanagerRequestDto> requestWrapper = new RequestWrapper<>();
            requestWrapper.setId(DECRYPT_SERVICE_ID);
            requestWrapper.setMetadata(null);
            requestWrapper.setRequest(requestDto);
            requestWrapper.setRequesttime(LocalDateTime.parse(
                    DateUtils.getUTCCurrentDateTimeString(DATETIME_PATTERN),
                    DateTimeFormatter.ofPattern(DATETIME_PATTERN)
            ));
            requestWrapper.setVersion(APPLICATION_VERSION);

            HttpEntity<RequestWrapper<CryptomanagerRequestDto>> httpEntity = new HttpEntity<>(requestWrapper);
            ResponseEntity<String> response = getRestTemplate().exchange(
                    cryptomanagerDecryptUrl, HttpMethod.POST, httpEntity, String.class
            );

            CryptomanagerResponseDto responseObject = mapper.readValue(response.getBody(), CryptomanagerResponseDto.class);
            if (responseObject != null && responseObject.getErrors() != null && !responseObject.getErrors().isEmpty()) {
                ServiceError error = responseObject.getErrors().get(0);
                throw new ObjectStoreAdapterException("CM-ERR-003", error.getMessage());
            }

            return CryptoUtil.decodeBase64(responseObject.getResponse().getData());
        } catch (Exception e) {
            throw new ObjectStoreAdapterException("CM-ERR-004", "Error during decryption", e);
        }
    }

    /**
     * Combines nonce, AAD, and encrypted data into a single byte array.
     *
     * @param encryptedData the AES-GCM encrypted data
     * @param nonce         the nonce/IV
     * @param aad           the additional authenticated data
     * @return combined byte array
     */
    private byte[] mergeEncryptedData(byte[] encryptedData, byte[] nonce, byte[] aad) {
        byte[] finalEncData = new byte[nonce.length + aad.length + encryptedData.length];
        System.arraycopy(nonce, 0, finalEncData, 0, nonce.length);
        System.arraycopy(aad, 0, finalEncData, nonce.length, aad.length);
        System.arraycopy(encryptedData, 0, finalEncData, nonce.length + aad.length, encryptedData.length);
        return finalEncData;
    }

    /**
     * Lazily retrieves the RestTemplate bean from the Spring context.
     *
     * @return a {@link RestTemplate} instance
     */
    private RestTemplate getRestTemplate() {
        if (restTemplate == null) {
            restTemplate = (RestTemplate) applicationContext.getBean("selfTokenRestTemplate");
        }
        return restTemplate;
    }
}