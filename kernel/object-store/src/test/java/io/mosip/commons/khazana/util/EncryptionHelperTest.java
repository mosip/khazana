package io.mosip.commons.khazana.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EncryptionHelperTest {

    private EncryptionHelper helper = new EncryptionHelper();

    @Mock
    private OfflineEncryptionUtil offlineEncryptionUtil;

    @Mock
    private OnlineCryptoUtil onlineCryptoUtil;

    @BeforeEach
    void setup() throws Exception {
        // inject mocks
        Field f1 = EncryptionHelper.class.getDeclaredField("offlineEncryptionUtil");
        f1.setAccessible(true);
        f1.set(helper, offlineEncryptionUtil);

        Field f2 = EncryptionHelper.class.getDeclaredField("onlineCryptoUtil");
        f2.setAccessible(true);
        f2.set(helper, onlineCryptoUtil);

        // set cryptoName to Offline by default
        Field crypto = EncryptionHelper.class.getDeclaredField("cryptoName");
        crypto.setAccessible(true);
        crypto.set(helper, "OfflinePacketCryptoServiceImpl");
    }

    @Test
    void encryptShouldUseOfflineWhenCryptoNameIsOffline() {
        byte[] input = "hello".getBytes();
        byte[] expected = "offline".getBytes();
        when(offlineEncryptionUtil.encrypt("ref", input)).thenReturn(expected);

        byte[] actual = helper.encrypt("ref", input);

        assertArrayEquals(expected, actual);
    }

    @Test
    void encryptShouldUseOnlineWhenCryptoNameIsNotOffline() throws Exception {
        // set cryptoName to some other value
        Field crypto = EncryptionHelper.class.getDeclaredField("cryptoName");
        crypto.setAccessible(true);
        crypto.set(helper, "OnlineSomething");

        byte[] input = "hello".getBytes();
        byte[] expected = "online".getBytes();
        when(onlineCryptoUtil.encrypt("ref", input)).thenReturn(expected);

        byte[] actual = helper.encrypt("ref", input);

        assertArrayEquals(expected, actual);
    }
}
