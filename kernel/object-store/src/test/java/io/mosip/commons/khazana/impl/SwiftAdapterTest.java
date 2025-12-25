package io.mosip.commons.khazana.impl;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doNothing;
import static org.mockito.ArgumentMatchers.any;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;

@ExtendWith(MockitoExtension.class)
class SwiftAdapterTest {

    private SwiftAdapter adapter = new SwiftAdapter();

    @Mock
    private Account account;

    @Mock
    private Container container;

    @Mock
    private StoredObject storedObject;

    @BeforeEach
    void setup() throws Exception {
        // inject account to accounts map
        Field accounts = SwiftAdapter.class.getDeclaredField("accounts");
        accounts.setAccessible(true);
        Map<String, Account> map = new HashMap<>();
        map.put("acct", account);
        accounts.set(adapter, map);

        when(account.getContainer("cont")).thenReturn(container);
    }

    @Test
    void getObject_shouldCreateContainerIfNotExists_andReturnStream() throws Exception {
        when(container.exists()).thenReturn(false);
        when(container.create()).thenReturn(container);
        InputStream in = new ByteArrayInputStream("payload".getBytes());
        when(container.getObject("obj")).thenReturn(storedObject);
        when(storedObject.downloadObjectAsInputStream()).thenReturn(in);

        InputStream result = adapter.getObject("acct", "cont", "src", "proc", "obj");
        byte[] read = result.readAllBytes();
        assertEquals("payload", new String(read));
    }

    @Test
    void putObject_shouldUploadAndReturnTrue() throws Exception {
        when(container.exists()).thenReturn(false);
        when(container.create()).thenReturn(container);
        when(container.getObject("obj")).thenReturn(storedObject);
        // be permissive on the InputStream argument to avoid strict stubbing mismatch
        doNothing().when(storedObject).uploadObject(any(InputStream.class));
        boolean ok = adapter.putObject("acct", "cont", "src", "proc", "obj", new ByteArrayInputStream("x".getBytes()));
        assertTrue(ok);
    }
}
