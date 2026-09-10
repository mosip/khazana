package io.mosip.commons.khazana.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.mosip.commons.khazana.dto.DecryptResponseDto;


@ExtendWith(MockitoExtension.class)
class OnlineCryptoUtilTest {

    private OnlineCryptoUtil util = new OnlineCryptoUtil();

    @Mock
    private ApplicationContext applicationContext;

    @Mock
    private RestTemplate restTemplate;

    private ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setup() throws Exception {
        Field ac = OnlineCryptoUtil.class.getDeclaredField("applicationContext");
        ac.setAccessible(true);
        ac.set(util, applicationContext);

        Field rt = OnlineCryptoUtil.class.getDeclaredField("restTemplate");
        rt.setAccessible(true);
        rt.set(util, restTemplate);

        Field m = OnlineCryptoUtil.class.getDeclaredField("mapper");
        m.setAccessible(true);
        m.set(util, mapper);

        // set DATETIME_PATTERN and cryptomanagerEncryptUrl so encrypt() can proceed
        Field dt = OnlineCryptoUtil.class.getDeclaredField("DATETIME_PATTERN");
        dt.setAccessible(true);
        dt.set(util, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

        Field url = OnlineCryptoUtil.class.getDeclaredField("cryptomanagerEncryptUrl");
        url.setAccessible(true);
        url.set(util, "http://localhost/encrypt");

        Field decryptUrl = OnlineCryptoUtil.class.getDeclaredField("cryptomanagerDecryptUrl");
        decryptUrl.setAccessible(true);
        decryptUrl.set(util, "http://localhost/decrypt");
    }

    @Test
    void encryptShouldReturnMergedBytesOnSuccess() throws Exception {
        byte[] plain = "payload-data".getBytes(StandardCharsets.UTF_8);
        byte[] encryptedData = "cipher-bytes".getBytes(StandardCharsets.UTF_8);
        String base64 = Base64.getEncoder().encodeToString(encryptedData);

        // build minimal JSON that maps to CryptomanagerResponseDto { response: { data: "..." } }
        String body = "{\"response\":{\"data\":\"" + base64 + "\"}}";
        ResponseEntity<String> responseEntity = ResponseEntity.ok(body);

        when(restTemplate.exchange(any(String.class), any(HttpMethod.class), any(HttpEntity.class), any(Class.class))).thenReturn(responseEntity);

        byte[] merged = util.encrypt("refId", plain);

        int startEncrypted = io.mosip.commons.khazana.constant.KhazanaConstant.GCM_NONCE_LENGTH
                + io.mosip.commons.khazana.constant.KhazanaConstant.GCM_AAD_LENGTH;
        byte[] gotEncrypted = java.util.Arrays.copyOfRange(merged, startEncrypted, merged.length);
        assertArrayEquals(encryptedData, gotEncrypted);
    }

    @Test
    void encryptShouldThrowObjectStoreAdapterExceptionWhenServiceReturnsError() throws Exception {
        byte[] plain = "payload-data".getBytes(StandardCharsets.UTF_8);

        // build minimal JSON with errors array: { errors: [ { errorCode: "ERR", message: "failed" } ] }
        String body = "{\"errors\":[{\"errorCode\":\"ERR\",\"message\":\"failed\"}]}";
        ResponseEntity<String> responseEntity = ResponseEntity.ok(body);
        when(restTemplate.exchange(any(String.class), any(HttpMethod.class), any(HttpEntity.class), any(Class.class))).thenReturn(responseEntity);

        assertThrows(io.mosip.commons.khazana.exception.ObjectStoreAdapterException.class, () -> util.encrypt("refId", plain));
    }

    @Test
    void decryptShouldReturnDecodedBytesOnSuccess() throws Exception {
        // build nonce + aad + encryptedData
        byte[] nonce = new byte[io.mosip.commons.khazana.constant.KhazanaConstant.GCM_NONCE_LENGTH];
        byte[] aad = new byte[io.mosip.commons.khazana.constant.KhazanaConstant.GCM_AAD_LENGTH];
        byte[] encryptedData = "cipher-bytes".getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < nonce.length; i++) {
            nonce[i] = (byte) (i + 1);
        }
        for (int i = 0; i < aad.length; i++) {
            aad[i] = (byte) (i + 10);
        }
        byte[] packet = new byte[nonce.length + aad.length + encryptedData.length];
        System.arraycopy(nonce, 0, packet, 0, nonce.length);
        System.arraycopy(aad, 0, packet, nonce.length, aad.length);
        System.arraycopy(encryptedData, 0, packet, nonce.length + aad.length, encryptedData.length);

        String base64 = Base64.getEncoder().encodeToString(encryptedData);
        String body = "{\"response\":{\"data\":\"" + base64 + "\"}}";
        ResponseEntity<String> responseEntity = ResponseEntity.ok(body);

        when(restTemplate.exchange(any(String.class), any(HttpMethod.class), any(HttpEntity.class), any(Class.class))).thenReturn(responseEntity);

        byte[] result = util.decrypt("ref", packet);
        assertArrayEquals(encryptedData, result);
    }

    @Test
    void getRestTemplateShouldFetchFromApplicationContextWhenNull() throws Exception {
        // prepare a fresh util with null restTemplate
        OnlineCryptoUtil newUtil = new OnlineCryptoUtil();
        Field ac = OnlineCryptoUtil.class.getDeclaredField("applicationContext");
        ac.setAccessible(true);
        ac.set(newUtil, applicationContext);

        // ensure restTemplate field null
        Field rt = OnlineCryptoUtil.class.getDeclaredField("restTemplate");
        rt.setAccessible(true);
        rt.set(newUtil, null);

        RestTemplate bean = new RestTemplate();
        when(applicationContext.getBean("selfTokenRestTemplate")).thenReturn(bean);

        Method getter = OnlineCryptoUtil.class.getDeclaredMethod("getRestTemplate");
        getter.setAccessible(true);
        Object got = getter.invoke(newUtil);
        assertSame(bean, got);
    }
}
