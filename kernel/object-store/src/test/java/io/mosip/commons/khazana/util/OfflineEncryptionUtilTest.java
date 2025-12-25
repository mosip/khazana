package io.mosip.commons.khazana.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationContext;

import io.mosip.commons.khazana.dto.DecryptResponseDto;
import io.mosip.kernel.cryptomanager.dto.CryptomanagerResponseDto;
import io.mosip.kernel.cryptomanager.service.impl.CryptomanagerServiceImpl;

@ExtendWith(MockitoExtension.class)
class OfflineEncryptionUtilTest {

    private OfflineEncryptionUtil util = new OfflineEncryptionUtil();

    @Mock
    private ApplicationContext applicationContext;

    @Mock
    private CryptomanagerServiceImpl cryptomanagerService;

    @BeforeEach
    void setup() throws Exception {
        // inject mocked applicationContext
        Field ac = OfflineEncryptionUtil.class.getDeclaredField("applicationContext");
        ac.setAccessible(true);
        ac.set(util, applicationContext);
    }

    @Test
    void encryptShouldReturnMergedBytesWithExpectedTail() throws Exception {
        byte[] plain = "payload-data".getBytes(StandardCharsets.UTF_8);
        byte[] encryptedData = "cipher-bytes".getBytes(StandardCharsets.UTF_8);
        String base64 = Base64.getEncoder().encodeToString(encryptedData);

        // return CryptomanagerResponseDto from kernel service (kernel DTO)
        CryptomanagerResponseDto kernelResp = new CryptomanagerResponseDto();
        kernelResp.setData(base64);

        when(applicationContext.getBean(CryptomanagerServiceImpl.class)).thenReturn(cryptomanagerService);
        when(cryptomanagerService.encrypt(any())).thenReturn(kernelResp);

        byte[] merged = util.encrypt("refId", plain);

        int expectedLen = encryptedData.length + io.mosip.commons.khazana.constant.KhazanaConstant.GCM_AAD_LENGTH
                + io.mosip.commons.khazana.constant.KhazanaConstant.GCM_NONCE_LENGTH;
        assertEquals(expectedLen, merged.length);

        // last bytes should be the encryptedData
        int startEncrypted = io.mosip.commons.khazana.constant.KhazanaConstant.GCM_NONCE_LENGTH
                + io.mosip.commons.khazana.constant.KhazanaConstant.GCM_AAD_LENGTH;
        byte[] gotEncrypted = java.util.Arrays.copyOfRange(merged, startEncrypted, merged.length);
        assertArrayEquals(encryptedData, gotEncrypted);
    }

    @Test
    void encryptShouldPropagateWhenServiceThrows() throws Exception {
        byte[] plain = "payload".getBytes(StandardCharsets.UTF_8);
        when(applicationContext.getBean(CryptomanagerServiceImpl.class)).thenReturn(cryptomanagerService);
        when(cryptomanagerService.encrypt(any())).thenThrow(new RuntimeException("boom"));
        assertThrows(RuntimeException.class, () -> util.encrypt("refId", plain));
    }
}
