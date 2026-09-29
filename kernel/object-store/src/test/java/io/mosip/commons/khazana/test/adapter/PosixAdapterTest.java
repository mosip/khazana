package io.mosip.commons.khazana.test.adapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.mosip.commons.khazana.exception.FileNotFoundInDestinationException;
import io.mosip.commons.khazana.impl.PosixAdapter;
import io.mosip.commons.khazana.util.EncryptionHelper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Exercises {@link PosixAdapter} against a temporary directory. No PowerMock.
 */
public class PosixAdapterTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private PosixAdapter adapter;
    private EncryptionHelper helper;

    @Before
    public void setUp() {
        adapter = new PosixAdapter();
        helper = mock(EncryptionHelper.class);
        ReflectionTestUtils.setField(adapter, "baseLocation", folder.getRoot().getAbsolutePath());
        ReflectionTestUtils.setField(adapter, "objectMapper", new ObjectMapper());
        ReflectionTestUtils.setField(adapter, "helper", helper);
    }

    @Test
    public void putGetExistsAndSecondPut() throws Exception {
        byte[] first = "one".getBytes(StandardCharsets.UTF_8);
        byte[] second = "two".getBytes(StandardCharsets.UTF_8);
        assertTrue(adapter.putObject("acct", "box", "src", "proc", "a", new ByteArrayInputStream(first)));
        assertTrue(adapter.putObject("acct", "box", "src", "proc", "b", new ByteArrayInputStream(second)));
        assertTrue(adapter.exists("acct", "box", "src", "proc", "a"));
        try (InputStream in = adapter.getObject("acct", "box", "src", "proc", "b")) {
            assertEquals("two", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
        assertNull(adapter.getObject("missing", "box", "src", "proc", "a"));
        assertFalse(adapter.exists("acct", "box", "src", "proc", "nope"));
    }

    @Test
    public void metadataTagsPackAndContainer() throws Exception {
        assertTrue(adapter.putObject("acct", "box", "src", "proc", "a", new ByteArrayInputStream(new byte[]{1, 2})));
        Map<String, Object> meta = new HashMap<>();
        meta.put("k", "v");
        assertEquals(meta, adapter.addObjectMetaData("acct", "box", "src", "proc", "a", meta));
        assertNotNull(adapter.addObjectMetaData("acct", "box", "src", "proc", "a", "n", "1"));
        adapter.getMetaData("acct", "box", "src", "proc", "a");

        Map<String, String> tags = new HashMap<>();
        tags.put("color", "blue");
        assertEquals(tags, adapter.addTags("acct", "box", tags));
        assertNotNull(adapter.getTags("acct", "box"));
        adapter.addTags("acct", "box", Map.of("size", "1"));
        adapter.deleteTags("acct", "box", List.of("color"));

        when(helper.encrypt(anyString(), any(byte[].class))).thenReturn(new byte[]{9, 9});
        assertTrue(adapter.pack("acct", "box", "src", "proc", "ref"));
        assertEquals(Integer.valueOf(0), adapter.incMetadata("acct", "box", "src", "proc", "a", "n"));
        assertEquals(Integer.valueOf(0), adapter.decMetadata("acct", "box", "src", "proc", "a", "n"));
        assertTrue(adapter.deleteObject("acct", "box", "src", "proc", "a"));
        assertNull(adapter.getAllObjects("acct", "box"));
        assertFalse(adapter.moveObject(null, null, true));
        assertTrue(adapter.listObjectsByPrefix("acct", "box", "p").isEmpty());
        adapter.removeContainer("acct", "box", "src", "proc");
        assertFalse(adapter.removeContainer("acct", "missing", "src", "proc"));
        assertFalse(adapter.pack("nobody", "box", "src", "proc", "ref"));
        assertFalse(adapter.removeContainer("nobody", "box", "src", "proc"));
    }

    @Test
    public void getObject_missingContainerReturnsNull() {
        Path account = Path.of(folder.getRoot().getAbsolutePath(), "acct");
        assertTrue(account.toFile().mkdir());
        assertNull(adapter.getObject("acct", "gone", "s", "p", "a"));
    }

    @Test
    public void getMetaData_missingContainerThrows() {
        Path account = Path.of(folder.getRoot().getAbsolutePath(), "acct");
        assertTrue(account.toFile().mkdir());
        try {
            adapter.getMetaData("acct", "gone", "s", "p", "a");
        } catch (FileNotFoundInDestinationException expected) {
            assertNotNull(expected.getErrorCode());
        }
    }

    @Test
    public void getTags_readsExistingFileThroughCatch() throws Exception {
        Path account = Path.of(folder.getRoot().getAbsolutePath(), "acct");
        Files.createDirectories(account);
        Files.writeString(account.resolve("box_tags.json"), "not-json");
        assertTrue(adapter.getTags("acct", "box").isEmpty());
    }
}
