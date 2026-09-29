package io.mosip.commons.khazana.test.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.mosip.commons.khazana.exception.ObjectStoreAdapterException;
import io.mosip.commons.khazana.util.EncryptionHelper;
import io.mosip.commons.khazana.util.EncryptionUtil;
import io.mosip.commons.khazana.util.ObjectStoreUtil;
import io.mosip.commons.khazana.util.OfflineEncryptionUtil;
import io.mosip.commons.khazana.util.OnlineCryptoUtil;
import io.mosip.kernel.core.util.CryptoUtil;
import io.mosip.kernel.cryptomanager.dto.CryptomanagerRequestDto;
import io.mosip.kernel.cryptomanager.service.impl.CryptomanagerServiceImpl;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestTemplate;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KhazanaUtilTest {

    @Test
    public void objectNamesSkipEmptySegments() {
        assertEquals("src/a", ObjectStoreUtil.getName("src", "", "a"));
        assertEquals("a", ObjectStoreUtil.getName(null, null, "a"));
        assertEquals("acct/src/p/a", ObjectStoreUtil.getName("acct", "src", "p", "a"));
        assertEquals("obj/tag", ObjectStoreUtil.getName("obj", "tag"));
        assertEquals("tag", ObjectStoreUtil.getName("", "tag"));
    }

    @Test
    public void mergeEncryptedDataPlacesNonceThenAad() {
        byte[] nonce = new byte[12];
        byte[] aad = new byte[32];
        nonce[0] = 7;
        aad[0] = 9;
        byte[] merged = EncryptionUtil.mergeEncryptedData(new byte[]{1, 2}, nonce, aad);
        assertEquals(46, merged.length);
        assertEquals(7, merged[0]);
        assertEquals(9, merged[12]);
        assertEquals(1, merged[44]);
    }

    @Test
    public void encryptionHelperSelectsOfflineOrOnline() {
        EncryptionHelper helper = new EncryptionHelper();
        OfflineEncryptionUtil offline = mock(OfflineEncryptionUtil.class);
        OnlineCryptoUtil online = mock(OnlineCryptoUtil.class);
        when(offline.encrypt(anyString(), any())).thenReturn(new byte[]{1});
        when(online.encrypt(anyString(), any())).thenReturn(new byte[]{2});
        ReflectionTestUtils.setField(helper, "offlineEncryptionUtil", offline);
        ReflectionTestUtils.setField(helper, "onlineCryptoUtil", online);
        ReflectionTestUtils.setField(helper, "cryptoName", "OfflinePacketCryptoServiceImpl");
        assertArrayEquals(new byte[]{1}, helper.encrypt("r", new byte[]{3}));
        ReflectionTestUtils.setField(helper, "cryptoName", "online");
        assertArrayEquals(new byte[]{2}, helper.encrypt("r", new byte[]{3}));
    }

    @Test
    public void offlineEncryptUsesCryptomanagerBean() {
        OfflineEncryptionUtil util = new OfflineEncryptionUtil();
        ApplicationContext context = mock(ApplicationContext.class);
        CryptomanagerServiceImpl service = mock(CryptomanagerServiceImpl.class);
        io.mosip.kernel.cryptomanager.dto.CryptomanagerResponseDto response =
                new io.mosip.kernel.cryptomanager.dto.CryptomanagerResponseDto();
        response.setData(CryptoUtil.encodeBase64String(new byte[]{4, 5}));
        when(context.getBean(CryptomanagerServiceImpl.class)).thenReturn(service);
        when(service.encrypt(any(CryptomanagerRequestDto.class))).thenReturn(response);
        ReflectionTestUtils.setField(util, "applicationContext", context);
        ReflectionTestUtils.setField(util, "isPrependThumbprintEnabled", true);
        byte[] out = util.encrypt("1000110001", new byte[]{1});
        assertEquals(46, out.length);
        assertNotNull(util.encrypt("1000110001", new byte[]{1}));
    }

    @Test
    public void onlineEncryptAndDecrypt() throws Exception {
        OnlineCryptoUtil util = new OnlineCryptoUtil();
        RestTemplate rest = mock(RestTemplate.class);
        ObjectMapper mapper = new ObjectMapper();
        ReflectionTestUtils.setField(util, "restTemplate", rest);
        ReflectionTestUtils.setField(util, "mapper", mapper);
        ReflectionTestUtils.setField(util, "DATETIME_PATTERN", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        ReflectionTestUtils.setField(util, "APPLICATION_VERSION", "v1");
        ReflectionTestUtils.setField(util, "cryptomanagerEncryptUrl", "http://encrypt");
        ReflectionTestUtils.setField(util, "cryptomanagerDecryptUrl", "http://decrypt");
        ReflectionTestUtils.setField(util, "isPrependThumbprintEnabled", true);
        ReflectionTestUtils.setField(util, "centerIdLength", 5);
        ReflectionTestUtils.setField(util, "machineIdLength", 5);

        String cipher = CryptoUtil.encodeBase64String(new byte[]{8});
        String json = "{\"response\":{\"data\":\"" + cipher + "\"},\"errors\":[]}";
        when(rest.exchange(anyString(), eq(HttpMethod.POST), any(), eq(String.class)))
                .thenReturn(ResponseEntity.ok(json));

        byte[] encrypted = util.encrypt("ref", new byte[]{1, 2});
        assertTrue(encrypted.length > 44);
        byte[] decrypted = util.decrypt("ref", encrypted);
        assertArrayEquals(new byte[]{8}, decrypted);
    }

    @Test
    public void onlineEncrypt_errorPayloadAndRestTemplateLookup() throws Exception {
        OnlineCryptoUtil util = new OnlineCryptoUtil();
        RestTemplate rest = mock(RestTemplate.class);
        ApplicationContext context = mock(ApplicationContext.class);
        when(context.getBean("selfTokenRestTemplate")).thenReturn(rest);
        ObjectMapper mapper = new ObjectMapper();
        ReflectionTestUtils.setField(util, "applicationContext", context);
        ReflectionTestUtils.setField(util, "mapper", mapper);
        ReflectionTestUtils.setField(util, "DATETIME_PATTERN", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        ReflectionTestUtils.setField(util, "APPLICATION_VERSION", "v1");
        ReflectionTestUtils.setField(util, "cryptomanagerEncryptUrl", "http://encrypt");
        ReflectionTestUtils.setField(util, "isPrependThumbprintEnabled", false);

        String json = "{\"errors\":[{\"errorCode\":\"E\",\"message\":\"bad\"}]}";
        when(rest.exchange(anyString(), eq(HttpMethod.POST), any(), eq(String.class)))
                .thenReturn(ResponseEntity.ok(json));
        try {
            util.encrypt("ref", new byte[]{1});
        } catch (ObjectStoreAdapterException expected) {
            assertNotNull(expected.getCause());
        }
    }
}
