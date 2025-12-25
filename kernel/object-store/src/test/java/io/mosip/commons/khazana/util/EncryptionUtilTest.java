package io.mosip.commons.khazana.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import io.mosip.commons.khazana.constant.KhazanaConstant;

class EncryptionUtilTest {

    @Test
    void mergeEncryptedData_shouldConcatenate_nonce_aad_and_encryptedData_in_order() {
        byte[] encrypted = "enc-payload".getBytes(StandardCharsets.UTF_8);
        byte[] nonce = new byte[KhazanaConstant.GCM_NONCE_LENGTH];
        byte[] aad = new byte[KhazanaConstant.GCM_AAD_LENGTH];
        for (int i = 0; i < nonce.length; i++) {
            nonce[i] = (byte) (i + 1);
        }
        for (int i = 0; i < aad.length; i++) {
            aad[i] = (byte) (i + 10);
        }

        byte[] merged = EncryptionUtil.mergeEncryptedData(encrypted, nonce, aad);

        int expectedLen = encrypted.length + KhazanaConstant.GCM_NONCE_LENGTH + KhazanaConstant.GCM_AAD_LENGTH;
        assertEquals(expectedLen, merged.length);

        // nonce at beginning
        byte[] gotNonce = Arrays.copyOfRange(merged, 0, KhazanaConstant.GCM_NONCE_LENGTH);
        assertArrayEquals(nonce, gotNonce);

        // aad follows nonce
        byte[] gotAad = Arrays.copyOfRange(merged, KhazanaConstant.GCM_NONCE_LENGTH,
                KhazanaConstant.GCM_NONCE_LENGTH + KhazanaConstant.GCM_AAD_LENGTH);
        assertArrayEquals(aad, gotAad);

        // encrypted data at the end
        byte[] gotEncrypted = Arrays.copyOfRange(merged,
                KhazanaConstant.GCM_NONCE_LENGTH + KhazanaConstant.GCM_AAD_LENGTH, merged.length);
        assertArrayEquals(encrypted, gotEncrypted);
    }

    @Test
    void mergeEncryptedData_withEmptyEncryptedData_shouldStillReturn_nonce_and_aad() {
        byte[] encrypted = new byte[0];
        byte[] nonce = new byte[KhazanaConstant.GCM_NONCE_LENGTH];
        byte[] aad = new byte[KhazanaConstant.GCM_AAD_LENGTH];
        Arrays.fill(nonce, (byte) 1);
        Arrays.fill(aad, (byte) 2);

        byte[] merged = EncryptionUtil.mergeEncryptedData(encrypted, nonce, aad);

        int expectedLen = KhazanaConstant.GCM_NONCE_LENGTH + KhazanaConstant.GCM_AAD_LENGTH;
        assertEquals(expectedLen, merged.length);

        assertArrayEquals(nonce, Arrays.copyOfRange(merged, 0, nonce.length));
        assertArrayEquals(aad, Arrays.copyOfRange(merged, nonce.length, nonce.length + aad.length));
    }

    @Test
    void mergeEncryptedData_shouldThrow_NPE_when_encryptedData_is_null() {
        byte[] nonce = new byte[KhazanaConstant.GCM_NONCE_LENGTH];
        byte[] aad = new byte[KhazanaConstant.GCM_AAD_LENGTH];
        assertThrows(NullPointerException.class, () -> EncryptionUtil.mergeEncryptedData(null, nonce, aad));
    }

    @Test
    void mergeEncryptedData_shouldThrow_NPE_when_nonce_is_null() {
        byte[] encrypted = "x".getBytes(StandardCharsets.UTF_8);
        byte[] aad = new byte[KhazanaConstant.GCM_AAD_LENGTH];
        assertThrows(NullPointerException.class, () -> EncryptionUtil.mergeEncryptedData(encrypted, null, aad));
    }

    @Test
    void mergeEncryptedData_shouldThrow_NPE_when_aad_is_null() {
        byte[] encrypted = "x".getBytes(StandardCharsets.UTF_8);
        byte[] nonce = new byte[KhazanaConstant.GCM_NONCE_LENGTH];
        assertThrows(NullPointerException.class, () -> EncryptionUtil.mergeEncryptedData(encrypted, nonce, null));
    }

    @Test
    void mergeEncryptedData_shouldThrow_when_nonce_length_exceeds_constant() {
        byte[] encrypted = "enc".getBytes(StandardCharsets.UTF_8);
        byte[] nonce = new byte[KhazanaConstant.GCM_NONCE_LENGTH + 1];
        byte[] aad = new byte[KhazanaConstant.GCM_AAD_LENGTH];
        // copying a nonce longer than KhazanaConstant.GCM_NONCE_LENGTH should cause an index error
        assertThrows(IndexOutOfBoundsException.class, () -> EncryptionUtil.mergeEncryptedData(encrypted, nonce, aad));
    }

    @Test
    void mergeEncryptedData_shouldThrow_when_aad_length_exceeds_constant() {
        byte[] encrypted = "enc".getBytes(StandardCharsets.UTF_8);
        byte[] nonce = new byte[KhazanaConstant.GCM_NONCE_LENGTH];
        byte[] aad = new byte[KhazanaConstant.GCM_AAD_LENGTH + 1];
        assertThrows(IndexOutOfBoundsException.class, () -> EncryptionUtil.mergeEncryptedData(encrypted, nonce, aad));
    }
}

