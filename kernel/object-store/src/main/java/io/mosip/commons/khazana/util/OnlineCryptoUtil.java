package io.mosip.commons.khazana.util;

import java.nio.ByteBuffer;
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

@Component
public class OnlineCryptoUtil {

    public static final String APPLICATION_ID = "REGISTRATION";
    private static final String DECRYPT_SERVICE_ID = "mosip.cryptomanager.decrypt";
    private static final String IO_EXCEPTION = "Exception while processing crypto operation";

    // ✅ Use static final SecureRandom instance (thread-safe in Java 8+)
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    // ✅ Cache DateTimeFormatter
    private static final DateTimeFormatter DEFAULT_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    @Value("${mosip.utc-datetime-pattern:yyyy-MM-dd'T'HH:mm:ss.SSS'Z'}")
    private String datetimePattern;

    @Value("${mosip.kernel.cryptomanager.request_version:v1}")
    private String applicationVersion;

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

    private RestTemplate restTemplate = null;

    public byte[] encrypt(String refId, byte[] packet) {
        try {
            String packetString = CryptoUtil.encodeToURLSafeBase64(packet);

            CryptomanagerRequestDto cryptomanagerRequestDto = new CryptomanagerRequestDto();
            RequestWrapper<CryptomanagerRequestDto> request = new RequestWrapper<>();

            cryptomanagerRequestDto.setApplicationId(APPLICATION_ID);
            cryptomanagerRequestDto.setData(packetString);
            cryptomanagerRequestDto.setReferenceId(refId);
            cryptomanagerRequestDto.setPrependThumbprint(isPrependThumbprintEnabled);

            byte[] nonce = new byte[KhazanaConstant.GCM_NONCE_LENGTH];
            byte[] aad = new byte[KhazanaConstant.GCM_AAD_LENGTH];
            SECURE_RANDOM.nextBytes(nonce);
            SECURE_RANDOM.nextBytes(aad);

            cryptomanagerRequestDto.setAad(CryptoUtil.encodeToPlainBase64(aad));
            cryptomanagerRequestDto.setSalt(CryptoUtil.encodeToPlainBase64(nonce));
            cryptomanagerRequestDto.setTimeStamp(DateUtils.getUTCCurrentDateTime());

            request.setId(DECRYPT_SERVICE_ID);
            request.setMetadata(null);
            request.setRequest(cryptomanagerRequestDto);
            request.setRequesttime(DateUtils.getUTCCurrentDateTime());
            request.setVersion(applicationVersion);

            HttpEntity<RequestWrapper<CryptomanagerRequestDto>> httpEntity = new HttpEntity<>(request);

            ResponseEntity<String> response =
                    getRestTemplate().exchange(cryptomanagerEncryptUrl, HttpMethod.POST, httpEntity, String.class);

            CryptomanagerResponseDto responseObject =
                    mapper.readValue(response.getBody(), CryptomanagerResponseDto.class);

            if (responseObject != null &&
                    responseObject.getErrors() != null &&
                    !responseObject.getErrors().isEmpty()) {
                ServiceError error = responseObject.getErrors().get(0);
                throw new ObjectStoreAdapterException("", error.getMessage());
            }

            byte[] encryptedData = CryptoUtil.decodePlainBase64(responseObject.getResponse().getData());
            return mergeEncryptedData(encryptedData, nonce, aad);

        } catch (Exception e) {
            throw new ObjectStoreAdapterException("", IO_EXCEPTION, e);
        }
    }

    private byte[] mergeEncryptedData(byte[] encryptedData, byte[] nonce, byte[] aad) {
        // ✅ Use ByteBuffer for cleaner and slightly faster copy
        ByteBuffer buffer = ByteBuffer.allocate(
                encryptedData.length + KhazanaConstant.GCM_AAD_LENGTH + KhazanaConstant.GCM_NONCE_LENGTH);
        buffer.put(nonce);
        buffer.put(aad);
        buffer.put(encryptedData);
        return buffer.array();
    }

    private RestTemplate getRestTemplate() {
        if (restTemplate == null) {
            restTemplate = (RestTemplate) applicationContext.getBean("selfTokenRestTemplate");
        }
        return restTemplate;
    }

    public byte[] decrypt(String refId, byte[] packet) {
        try {
            CryptomanagerRequestDto cryptomanagerRequestDto = new CryptomanagerRequestDto();
            RequestWrapper<CryptomanagerRequestDto> request = new RequestWrapper<>();

            cryptomanagerRequestDto.setApplicationId(APPLICATION_ID);
            cryptomanagerRequestDto.setReferenceId(refId);

            byte[] nonce = Arrays.copyOfRange(packet, 0, KhazanaConstant.GCM_NONCE_LENGTH);
            byte[] aad = Arrays.copyOfRange(packet, KhazanaConstant.GCM_NONCE_LENGTH,
                    KhazanaConstant.GCM_NONCE_LENGTH + KhazanaConstant.GCM_AAD_LENGTH);
            byte[] encryptedData = Arrays.copyOfRange(packet,
                    KhazanaConstant.GCM_NONCE_LENGTH + KhazanaConstant.GCM_AAD_LENGTH, packet.length);

            cryptomanagerRequestDto.setAad(CryptoUtil.encodeToPlainBase64(aad));
            cryptomanagerRequestDto.setSalt(CryptoUtil.encodeToPlainBase64(nonce));
            cryptomanagerRequestDto.setData(CryptoUtil.encodeToPlainBase64(encryptedData));
            cryptomanagerRequestDto.setPrependThumbprint(isPrependThumbprintEnabled);
            cryptomanagerRequestDto.setTimeStamp(DateUtils.getUTCCurrentDateTime());

            request.setId(DECRYPT_SERVICE_ID);
            request.setMetadata(null);
            request.setRequest(cryptomanagerRequestDto);
            request.setRequesttime(DateUtils.getUTCCurrentDateTime());
            request.setVersion(applicationVersion);

            HttpEntity<RequestWrapper<CryptomanagerRequestDto>> httpEntity = new HttpEntity<>(request);

            ResponseEntity<String> response =
                    getRestTemplate().exchange(cryptomanagerDecryptUrl, HttpMethod.POST, httpEntity, String.class);

            CryptomanagerResponseDto responseObject =
                    mapper.readValue(response.getBody(), CryptomanagerResponseDto.class);

            if (responseObject != null &&
                    responseObject.getErrors() != null &&
                    !responseObject.getErrors().isEmpty()) {
                ServiceError error = responseObject.getErrors().get(0);
                throw new ObjectStoreAdapterException("", error.getMessage());
            }

            return CryptoUtil.decodePlainBase64(responseObject.getResponse().getData());

        } catch (Exception e) {
            throw new ObjectStoreAdapterException("", IO_EXCEPTION, e);
        }
    }
}
