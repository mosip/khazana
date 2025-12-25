package io.mosip.commons.khazana.impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.mosip.commons.khazana.util.EncryptionHelper;

@ExtendWith(MockitoExtension.class)
public class PosixAdapterTest {

    private PosixAdapter adapter = new PosixAdapter();

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private EncryptionHelper helper;

    private Path tempDir;

    @BeforeEach
    void setup() throws Exception {
        tempDir = Files.createTempDirectory("posix-test");
        // inject baseLocation
        Field baseLocation = PosixAdapter.class.getDeclaredField("baseLocation");
        baseLocation.setAccessible(true);
        baseLocation.set(adapter, tempDir.toString());

        // inject objectMapper
        Field om = PosixAdapter.class.getDeclaredField("objectMapper");
        om.setAccessible(true);
        om.set(adapter, objectMapper);

        // inject helper
        Field h = PosixAdapter.class.getDeclaredField("helper");
        h.setAccessible(true);
        h.set(adapter, helper);
    }

    @Test
    void putAndGetObject_shouldStoreAndRetrieveContent() throws IOException {
        String account = "acct";
        String container = "cont";
        String source = "src";
        String process = "proc";
        String objectName = "obj";

        byte[] payload = "hello-posix".getBytes();

        boolean put = adapter.putObject(account, container, source, process, objectName, new ByteArrayInputStream(payload));
        assertTrue(put);

        assertTrue(adapter.exists(account, container, source, process, objectName));

        byte[] read = adapter.getObject(account, container, source, process, objectName).readAllBytes();
        assertArrayEquals(payload, read);
    }
}

