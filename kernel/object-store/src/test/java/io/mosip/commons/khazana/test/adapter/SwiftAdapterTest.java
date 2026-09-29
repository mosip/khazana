package io.mosip.commons.khazana.test.adapter;

import io.mosip.commons.khazana.impl.SwiftAdapter;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Covers {@link SwiftAdapter} with a cached mock {@link Account} so JOSS is not contacted.
 */
public class SwiftAdapterTest {

    private SwiftAdapter adapter;
    private Account account;
    private Container container;
    private StoredObject stored;

    @Before
    public void setUp() {
        adapter = new SwiftAdapter();
        account = mock(Account.class);
        container = mock(Container.class);
        stored = mock(StoredObject.class);
        when(account.getContainer("box")).thenReturn(container);
        when(container.exists()).thenReturn(true);
        when(container.getObject("obj")).thenReturn(stored);
        Map<String, Account> accounts = new HashMap<>();
        accounts.put("acct", account);
        ReflectionTestUtils.setField(adapter, "accounts", accounts);
    }

    @Test
    public void objectAndMetadataPaths() throws Exception {
        when(stored.downloadObjectAsInputStream()).thenReturn(new ByteArrayInputStream("z".getBytes(StandardCharsets.UTF_8)));
        when(stored.exists()).thenReturn(true);
        when(stored.getMetadata()).thenReturn(new HashMap<>(Map.of("a", "b")));
        when(stored.getName()).thenReturn("obj");

        try (InputStream in = adapter.getObject("acct", "box", "s", "p", "obj")) {
            assertEquals("z", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
        assertTrue(adapter.putObject("acct", "box", "s", "p", "obj", new ByteArrayInputStream(new byte[]{1})));
        verify(stored).uploadObject(org.mockito.ArgumentMatchers.any(java.io.InputStream.class));
        assertTrue(adapter.exists("acct", "box", "s", "p", "obj"));

        Map<String, Object> meta = new HashMap<>();
        meta.put("k", "v");
        assertEquals(meta, adapter.addObjectMetaData("acct", "box", "s", "p", "obj", meta));
        assertTrue(adapter.addObjectMetaData("acct", "box", "s", "p", "obj", "n", "1").containsKey("n"));
        assertTrue(adapter.getMetaData("acct", "box", "s", "p", "obj").get("obj") instanceof Map);

        when(container.list()).thenReturn(List.of(stored));
        assertNotNullMeta(adapter.getMetaData("acct", "box", "s", "p", null));
    }

    @Test
    public void missingContainerReturnsNullAndCreatesOnGet() {
        when(container.exists()).thenReturn(false);
        when(container.create()).thenReturn(container);
        when(stored.downloadObjectAsInputStream()).thenReturn(new ByteArrayInputStream(new byte[]{1}));
        assertNull(adapter.addObjectMetaData("acct", "box", "s", "p", "obj", Map.of("k", "v")));
        assertNull(adapter.addObjectMetaData("acct", "box", "s", "p", "obj", "k", "v"));
        assertNull(adapter.getMetaData("acct", "box", "s", "p", "obj"));
        when(container.exists()).thenReturn(false, true);
        adapter.getObject("acct", "box", "s", "p", "obj");
        verify(container).create();
    }

    @Test
    public void tagsAndStubs() {
        when(container.getMetadata()).thenReturn(new HashMap<>(Map.of("color", "blue")));
        when(container.create()).thenReturn(container);
        Map<String, String> tags = new HashMap<>();
        tags.put("size", "1");
        assertEquals(tags, adapter.addTags("acct", "box", tags));
        assertEquals("blue", adapter.getTags("acct", "box").get("color"));
        adapter.deleteTags("acct", "box", List.of("color"));
        verify(container, org.mockito.Mockito.atLeastOnce()).saveMetadata();

        assertEquals(Integer.valueOf(0), adapter.incMetadata("a", "b", "s", "p", "o", "k"));
        assertEquals(Integer.valueOf(0), adapter.decMetadata("a", "b", "s", "p", "o", "k"));
        assertTrue(adapter.deleteObject("a", "b", "s", "p", "o"));
        assertFalse(adapter.removeContainer("a", "b", "s", "p"));
        assertFalse(adapter.pack("a", "b", "s", "p", "r"));
        assertNull(adapter.getAllObjects("a", "b"));
    }

    private static void assertNotNullMeta(Map<String, Object> meta) {
        assertTrue(meta.containsKey("obj"));
    }
}
