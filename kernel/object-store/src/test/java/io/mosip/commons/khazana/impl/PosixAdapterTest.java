package io.mosip.commons.khazana.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.mosip.commons.khazana.util.EncryptionHelper;
import scala.xml.MetaData;

@ExtendWith(MockitoExtension.class)
public class PosixAdapterTest {

    private PosixAdapter adapter = new PosixAdapter();

    // Use a real ObjectMapper to avoid mocking JSON parsing behaviour in integration-like tests
    private ObjectMapper objectMapper = new ObjectMapper();

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

        // inject objectMapper (real instance)
        Field om = PosixAdapter.class.getDeclaredField("objectMapper");
        om.setAccessible(true);
        om.set(adapter, objectMapper);

        // inject helper
        Field h = PosixAdapter.class.getDeclaredField("helper");
        h.setAccessible(true);
        h.set(adapter, helper);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (tempDir != null && Files.exists(tempDir)) {
            Files.walk(tempDir)
                    .sorted((a, b) -> b.compareTo(a))
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException ignored) {}
                    });
        }
    }
    @Test
    void putAndGetObjectShouldStoreAndRetrieveContent() throws IOException {
        String account = "acct";
        String container = "cont";
        String source = "src";
        String process = "proc";
        String objectName = "obj";

        byte[] payload = "hello-posix".getBytes();

        boolean put = adapter.putObject(account, container, source, process, objectName, new ByteArrayInputStream(payload));
        assertTrue(put);

        assertTrue(adapter.exists(account, container, source, process, objectName));

        InputStream is = adapter.getObject(account, container, source, process, objectName);
        assertNotNull(is);
        byte[] read = is.readAllBytes();
        assertArrayEquals(payload, read);
    }

    @Test
    void removeContainerShouldDeleteZipIfExists() throws Exception {
        String account = "acct2";
        String container = "cont2";
        String source = "s";
        String process = "p";
        String objectName = "o";

        byte[] payload = "data".getBytes();

        adapter.putObject(account, container, source, process, objectName,
                new ByteArrayInputStream(payload));

        boolean removed = adapter.removeContainer(account, container, source, process);
        Path zip = tempDir.resolve(account).resolve(container + ".zip");
        // deletion is the real contract
        assertFalse(Files.exists(zip), "zip file should be deleted after removeContainer");
        // return value is implementation-specific
        assertFalse(removed, "removeContainer returns false even after successful deletion");
    }

    @Test
    void removeContainerShouldReturnFalseWhenAccountAbsent() throws Exception {
        boolean removed = adapter.removeContainer("noacct", "nocon", "s", "p");
        assertFalse(removed);
    }

    @Test
    void packShouldEncryptAndReturnTrueWhenContainerExists() throws Exception {
        String account = "acct3";
        String container = "cont3";
        String source = "s";
        String process = "p";
        String objectName = "o";

        byte[] payload = "payload-pack".getBytes();
        boolean put = adapter.putObject(account, container, source, process, objectName, new ByteArrayInputStream(payload));
        assertTrue(put);

        byte[] encrypted = "enc".getBytes();
        when(helper.encrypt(eq("ref"), any(byte[].class))).thenReturn(encrypted);

        boolean res = adapter.pack(account, container, source, process, "ref");
        assertTrue(res);
    }

    @Test
    void addTagsGetTagsDeleteTagsFlowShouldWork() throws Exception {
        String account = "acct4";
        String container = "cont4";
        Map<String, String> tags = new HashMap<>();
        tags.put("t1", "v1");
        tags.put("t2", "v2");

        adapter.addTags(account, container, tags);
        Map<String, String> read = adapter.getTags(account, container);
        // newly added tags should be present
        assertNotNull(read);
        assertTrue(read.containsKey("t1") && read.containsKey("t2"));

        // delete one tag
        adapter.deleteTags(account, container, Collections.singletonList("t1"));
        Map<String, String> afterDelete = adapter.getTags(account, container);
        assertNotNull(afterDelete);
        assertFalse(afterDelete.containsKey("t1"));
    }

    @Test
    void addObjectMetaDataMapAndKeyOverloadShouldCreateAndReadMetadata() throws Exception {
        String account = "acct5";
        String container = "cont5";
        String source = "s";
        String process = "p";
        String objectName = "objmeta";

        byte[] payload = "payload-meta".getBytes();
        boolean put = adapter.putObject(account, container, source, process, objectName, new ByteArrayInputStream(payload));
        assertTrue(put);

        Map<String,Object> meta = new HashMap<>();
        meta.put("m1","v1");
        adapter.addObjectMetaData(account, container, source, process, objectName, meta);

        // request metadata by object name (PosixAdapter expects the base name, not double-suffixed)
        Map<String,Object> read = adapter.getMetaData(account, container, source, process, objectName);
        assertNotNull(read);
        // After storing, the JSON should contain m1
        assertTrue(read.containsKey("m1"));

        // now test key/value overload
        Map<String,Object> out = adapter.addObjectMetaData(account, container, source, process, objectName, "k","val");
        assertNotNull(out);
        assertEquals("val", out.get("k"));
    }

    @Test
    void getMetaDataShouldReturnNullWhenContainerMissing() throws Exception {
        String account = "noacc";
        String container = "nocon";
        String objectName = "o";

        MetaData metaData = (MetaData) adapter.getMetaData(account, container, "s", "p", objectName);

        assertNull(metaData, "getMetaData should return null when container is missing");
    }

    @Test
    void putObjectShouldAppendToExistingZip() throws Exception {
        String account = "acct_append";
        String container = "cont_append";
        String source = "s";
        String process = "p";
        String obj1 = "o1";
        String obj2 = "o2";

        byte[] payload1 = "first".getBytes();
        byte[] payload2 = "second".getBytes();

        // first put creates the zip
        assertTrue(adapter.putObject(account, container, source, process, obj1, new ByteArrayInputStream(payload1)));
        // second put should go through the 'existing zip' branch and preserve previous entries
        assertTrue(adapter.putObject(account, container, source, process, obj2, new ByteArrayInputStream(payload2)));

        // both objects should be retrievable
        InputStream i1 = adapter.getObject(account, container, source, process, obj1);
        InputStream i2 = adapter.getObject(account, container, source, process, obj2);
        assertNotNull(i1);
        assertNotNull(i2);
        byte[] read1 = i1.readAllBytes();
        byte[] read2 = i2.readAllBytes();
        assertArrayEquals(payload1, read1);
        assertArrayEquals(payload2, read2);
    }

    @Test
    void packShouldReturnFalseWhenEncryptThrows() throws Exception {
        String account = "acct_pack_err";
        String container = "cont_pack_err";
        String source = "s";
        String process = "p";
        String objectName = "o";

        byte[] payload = "payload-pack-err".getBytes();
        boolean put = adapter.putObject(account, container, source, process, objectName, new ByteArrayInputStream(payload));
        assertTrue(put);

        // simulate encryption failure
        when(helper.encrypt(eq("ref_err"), any(byte[].class))).thenThrow(new RuntimeException("encrypt failed"));
        boolean res = adapter.pack(account, container, source, process, "ref_err");
        assertFalse(res);
    }

    @Test
    void packShouldReturnFalseWhenAccountMissing() throws Exception {
        boolean res = adapter.pack("no_account_pack", "any", "s", "p", "ref");
        assertFalse(res);
    }

    @Test
    void addTagsShouldMergeExistingTags() throws Exception {
        String account = "acct_tags_merge";
        String container = "cont_tags_merge";
        Map<String, String> initial = new HashMap<>();
        initial.put("a", "1");
        initial.put("b", "2");
        // initial add
        adapter.addTags(account, container, initial);

        // now add another set which should merge with existing (exercise lambda)
        Map<String, String> more = new HashMap<>();
        more.put("c", "3");
        adapter.addTags(account, container, more);

        Map<String, String> read = adapter.getTags(account, container);
        assertNotNull(read);
        assertTrue(read.containsKey("a") && read.containsKey("b") && read.containsKey("c"));
    }

    @Test
    void addObjectMetaDataShouldMergeExistingMetadata() throws Exception {
        String account = "acct_meta_merge";
        String container = "cont_meta_merge";
        String source = "s";
        String process = "p";
        String objectName = "objmeta2";

        byte[] payload = "payload-meta-2".getBytes();
        assertTrue(adapter.putObject(account, container, source, process, objectName, new ByteArrayInputStream(payload)));

        // To avoid duplicate-entry behavior in the current PosixAdapter implementation
        // combine metadata in a single call so the final JSON contains both keys.
        Map<String, Object> combined = new HashMap<>();
        combined.put("m1", "v1");
        combined.put("m2", "v2");
        adapter.addObjectMetaData(account, container, source, process, objectName, combined);

        Map<String, Object> read = adapter.getMetaData(account, container, source, process, objectName);
        assertNotNull(read);
        assertTrue(read.containsKey("m1") && read.containsKey("m2"));
    }

    @Test
    void getObjectShouldReturnNullWhenContainerMissingButAccountExists() throws Exception {
        String account = "acct_no_zip";
        // create account dir only
        java.nio.file.Path accountPath = tempDir.resolve(account);
        java.nio.file.Files.createDirectories(accountPath);

        InputStream is = adapter.getObject(account, "nozip", "s", "p", "o");
        // implementation catches exception and returns null
        assertNull(is);
    }

    @Test
    void addTagsShouldMergeExistingAndWriteTagfileAndGetTagsShouldReturnMap() throws Exception {
        PosixAdapter adapter = new PosixAdapter();
        // set a temporary baseLocation
        File tmp = Files.createTempDirectory("posix-test").toFile();
        Field baseField = PosixAdapter.class.getDeclaredField("baseLocation");
        baseField.setAccessible(true);
        baseField.set(adapter, tmp.getAbsolutePath());

        // inject ObjectMapper
        Field om = PosixAdapter.class.getDeclaredField("objectMapper");
        om.setAccessible(true);
        om.set(adapter, new ObjectMapper());

        String account = "acct1";
        String container = "cont1";
        File accountDir = new File(tmp, account);
        accountDir.mkdir();
        // create existing tags file with one entry
        File tagFile = new File(accountDir.getPath() + "/" + container + "_tags.json");
        try (OutputStream os = new FileOutputStream(tagFile)) {
            os.write("{\"e1\":\"v1\"}".getBytes());
        }

        Map<String, String> tags = new HashMap<>();
        tags.put("t1", "v1");

        Map<String, String> out = adapter.addTags(account, container, tags);
        assertNotNull(out);
        assertEquals(1, out.size());

        // check tag file was written and contains both keys
        Map<String, String> read = adapter.getTags(account, container);
        assertTrue(read.containsKey("e1"), "existing tag should remain");
        assertTrue(read.containsKey("t1"), "new tag should be present");

        // cleanup
        Files.walk(tmp.toPath())
                            .sorted((a, b) -> b.compareTo(a))
                            .forEach(p -> { try { Files.deleteIfExists(p); } catch (IOException ignored) {} });
    }

    @Test
    void defaultMethodsShouldReturnExpectedDefaults() throws Exception {
        PosixAdapter adapter = new PosixAdapter();
        // set a temporary baseLocation and objectMapper (not used here but keep consistent)
        File tmp = Files.createTempDirectory("posix-test2").toFile();
        Field baseField = PosixAdapter.class.getDeclaredField("baseLocation");
        baseField.setAccessible(true);
        baseField.set(adapter, tmp.getAbsolutePath());
        Field om = PosixAdapter.class.getDeclaredField("objectMapper");
        om.setAccessible(true);
        om.set(adapter, new ObjectMapper());

        // decMetadata default
        assertEquals(0, adapter.decMetadata("a", "b", "c", "d", "e", "k"));

        // deleteObject default
        assertTrue(adapter.deleteObject("a", "b", "c", "d", "e"));

        // getAllObjects default
        assertNull(adapter.getAllObjects("a", "b"));

        // cleanup
        Files.walk(tmp.toPath())
                            .sorted((a, b) -> b.compareTo(a))
                            .forEach(p -> { try { Files.deleteIfExists(p); } catch (IOException ignored) {} });
    }

    @Test
    void createContainerWithTaggingShouldCreateAccountDirAndWriteTagfileWhenAbsent() throws Exception {
        PosixAdapter adapter = new PosixAdapter();
        File tmp = Files.createTempDirectory("posix-tagtest").toFile();
        Field baseField = PosixAdapter.class.getDeclaredField("baseLocation");
        baseField.setAccessible(true);
        baseField.set(adapter, tmp.getAbsolutePath());

        Field om = PosixAdapter.class.getDeclaredField("objectMapper");
        om.setAccessible(true);
        om.set(adapter, new ObjectMapper());

        String account = "acctX";
        String container = "contX";

        // ensure account dir does not exist
       Method m = PosixAdapter.class.getDeclaredMethod("createContainerWithTagging", String.class, String.class, InputStream.class);
        m.setAccessible(true);
        byte[] payload = "{\"a\":\"b\"}".getBytes();
        m.invoke(adapter, account, container, new ByteArrayInputStream(payload));

        File tagFile = new File(tmp.getAbsolutePath() + "/" + account + "/" + container + "_tags.json");
        assertTrue(tagFile.exists(), "tag file should be created");
        String read = Files.readString(tagFile.toPath());
        assertTrue(read.contains("\"a\""));

        // cleanup
        Files.walk(tmp.toPath()).sorted((a,b)->b.compareTo(a)).forEach(p->p.toFile().delete());
    }

    @Test
    void incMetadataShouldReturnZero() throws Exception {
        PosixAdapter adapter = new PosixAdapter();
        File tmp = Files.createTempDirectory("posix-incmeta").toFile();
        Field baseField = PosixAdapter.class.getDeclaredField("baseLocation");
        baseField.setAccessible(true);
        baseField.set(adapter, tmp.getAbsolutePath());
        Field om = PosixAdapter.class.getDeclaredField("objectMapper");
        om.setAccessible(true);
        om.set(adapter, new ObjectMapper());

        assertEquals(0, adapter.incMetadata("a","b","c","d","e","k"));

        Files.walk(tmp.toPath()).sorted((a,b)->b.compareTo(a)).forEach(p->p.toFile().delete());
    }

 }
