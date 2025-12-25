package io.mosip.commons.khazana.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.*;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import io.mosip.commons.khazana.dto.ObjectDto;

@ExtendWith(MockitoExtension.class)
class S3AdapterExtendedTest {

    private S3Adapter adapter = new S3Adapter();

    @Mock
    private AmazonS3 amazonS3;

    @Mock
    private S3Object s3Object;

    @BeforeEach
    void setup() throws Exception {
        // inject mocked AmazonS3 into the adapter
        Field conn = S3Adapter.class.getDeclaredField("connection");
        conn.setAccessible(true);
        conn.set(adapter, amazonS3);

        // ensure defaults for flags
        Field useAccount = S3Adapter.class.getDeclaredField("useAccountAsBucketname");
        useAccount.setAccessible(true);
        useAccount.set(adapter, false);

        Field bucketPrefix = S3Adapter.class.getDeclaredField("bucketNamePrefix");
        bucketPrefix.setAccessible(true);
        bucketPrefix.set(adapter, "");

        // clear existingBuckets
        Field existing = S3Adapter.class.getDeclaredField("existingBuckets");
        existing.setAccessible(true);
        List<String> list = (List<String>) existing.get(adapter);
        list.clear();
    }

    @Test
    void addTags_shouldUploadAll_tags_and_return_map() throws Exception {
        String account = "acct";
        String container = "cont";

        Map<String, String> tags = new HashMap<>();
        tags.put("t1", "v1");
        tags.put("t2", "v2");

        // bucket exists
        when(amazonS3.doesBucketExistV2(container)).thenReturn(true);
        when(amazonS3.putObject(anyString(), anyString(), any(InputStream.class), any())).thenReturn(new PutObjectResult());

        Map<String, String> out = adapter.addTags(account, container, tags);
        assertEquals(2, out.size());
        verify(amazonS3, times(2)).putObject(eq(container), anyString(), any(InputStream.class), any());
    }

    @Test
    void addTags_shouldCreateBucket_when_missing() throws Exception {
        String account = "acct";
        String container = "cont";
        Map<String, String> tags = Map.of("t1", "v1");

        // bucket missing
        when(amazonS3.doesBucketExistV2(container)).thenReturn(false);
        when(amazonS3.putObject(anyString(), anyString(), any(InputStream.class), any())).thenReturn(new PutObjectResult());

        Map<String, String> out = adapter.addTags(account, container, tags);
        assertEquals(1, out.size());
        verify(amazonS3).createBucket(eq(container));
        verify(amazonS3).putObject(eq(container), anyString(), any(InputStream.class), any());
    }

    @Test
    void addTags_shouldHandle_backwardCompatibility_error_and_retry() throws Exception {
        String account = "a";
        String container = "c";
        Map<String, String> tags = Map.of("t1", "v1");

        // simulate putObject throws first with backward-compat error, then succeeds
        AmazonS3Exception ex = new AmazonS3Exception("Object-prefix is already an object, please choose a different object-prefix name");
        when(amazonS3.doesBucketExistV2(container)).thenReturn(true);
        // when checking existence of finalObject
        when(amazonS3.doesObjectExist(eq(container), anyString())).thenReturn(true);
        when(amazonS3.putObject(eq(container), anyString(), any(InputStream.class), any()))
                .thenThrow(ex)
                .thenReturn(new PutObjectResult());

        Map<String, String> out = adapter.addTags(account, container, tags);
        assertEquals(1, out.size());
        // verify deleteObject called on finalObject when backward-compatibility path taken
        verify(amazonS3).deleteObject(eq(container), anyString());
        // and eventual putObject called at least twice (first failed, second success)
        verify(amazonS3, atLeast(2)).putObject(eq(container), anyString(), any(InputStream.class), any());
    }

    @Test
    void addTags_should_useAccountPath_when_flag_true() throws Exception {
        Field useAccount = S3Adapter.class.getDeclaredField("useAccountAsBucketname");
        useAccount.setAccessible(true);
        useAccount.set(adapter, true);

        String account = "acct";
        String container = "cont";
        Map<String,String> tags = Map.of("t1","v1");

        // bucket missing for account -> should create
        when(amazonS3.doesBucketExistV2(account)).thenReturn(false);
        when(amazonS3.putObject(anyString(), anyString(), any(InputStream.class), any())).thenReturn(new PutObjectResult());

        Map<String,String> out = adapter.addTags(account, container, tags);
        assertEquals(1, out.size());
        verify(amazonS3).createBucket(eq(account));
    }

    @Test
    void addTags_shouldThrow_ObjectStoreAdapterException_on_non_backward_error() throws Exception {
        String account = "a";
        String container = "c";
        Map<String,String> tags = Map.of("t1","v1");

        AmazonS3Exception ex = new AmazonS3Exception("some-other-error");
        when(amazonS3.doesBucketExistV2(container)).thenReturn(true);
        when(amazonS3.putObject(eq(container), anyString(), any(InputStream.class), any())).thenThrow(ex);

        assertThrows(io.mosip.commons.khazana.exception.ObjectStoreAdapterException.class,
                () -> adapter.addTags(account, container, tags));
    }

    @Test
    void addObjectMetaData_shouldRead_existing_metadata_and_put_new() throws Exception {
        String account = "a";
        String container = "c";
        String objectName = "obj";

        byte[] content = "hello".getBytes(StandardCharsets.UTF_8);
        doReturn(s3Object).when(amazonS3).getObject(eq(container), anyString());
        ObjectMetadata existingMeta = new ObjectMetadata();
        existingMeta.addUserMetadata("k1", "v1");
        when(s3Object.getObjectMetadata()).thenReturn(existingMeta);
        when(s3Object.getObjectContent()).thenReturn(new S3ObjectInputStream(new ByteArrayInputStream(content), null));
        when(amazonS3.putObject(any(PutObjectRequest.class))).thenReturn(new PutObjectResult());

        Map<String, Object> meta = new HashMap<>();
        meta.put("k2", "v2");

        Map<String, Object> out = adapter.addObjectMetaData(account, container, "s", "p", objectName, meta);
        assertNotNull(out);
        assertEquals("v2", out.get("k2"));
        verify(amazonS3).putObject(any(PutObjectRequest.class));
        verify(s3Object).close();
    }

    @Test
    void addObjectMetaData_shouldThrow_ObjectStoreAdapterException_when_getObject_fails() throws Exception {
        String account = "a";
        String container = "c";
        String objectName = "o";
        doThrow(new RuntimeException("boom")).when(amazonS3).getObject(eq(container), anyString());

        assertThrows(io.mosip.commons.khazana.exception.ObjectStoreAdapterException.class,
                () -> adapter.addObjectMetaData(account, container, "s", "p", objectName, Map.of("k","v")));
    }

    @Test
    void addObjectMetaData_when_no_existing_metadata_should_put_new() throws Exception {
        String account = "a";
        String container = "c";
        String objectName = "obj";

        byte[] content = "data".getBytes(StandardCharsets.UTF_8);
        doReturn(s3Object).when(amazonS3).getObject(eq(container), anyString());
        when(s3Object.getObjectMetadata()).thenReturn(null);
        when(s3Object.getObjectContent()).thenReturn(new S3ObjectInputStream(new ByteArrayInputStream(content), null));
        when(amazonS3.putObject(any(PutObjectRequest.class))).thenReturn(new PutObjectResult());

        Map<String,Object> meta = Map.of("k","v");
        Map<String,Object> out = adapter.addObjectMetaData(account, container, "s","p", objectName, meta);
        assertEquals("v", out.get("k"));
        verify(amazonS3).putObject(any(PutObjectRequest.class));
        verify(s3Object).close();
    }

    @Test
    void getMetaData_shouldReturn_map_from_s3objectmetadata() throws Exception {
        String account = "a";
        String container = "c";
        String objectName = "obj";

        doReturn(s3Object).when(amazonS3).getObject(eq(container), anyString());
        ObjectMetadata meta = new ObjectMetadata();
        Map<String, String> um = new HashMap<>();
        um.put("x","1");
        meta.setUserMetadata(um);
        when(s3Object.getObjectMetadata()).thenReturn(meta);

        Map<String, Object> res = adapter.getMetaData(account, container, "s", "p", objectName);
        assertEquals(1, res.size());
        assertEquals("1", res.get("x"));
    }

    @Test
    void getTags_shouldReturn_tag_map() throws Exception {
        String account = "a";
        String container = "c";

        S3ObjectSummary summ = new S3ObjectSummary();
        // structure: TAGS_FILENAME + "/" + tagName
        summ.setKey("Tags/tag1");
        ObjectListing listing = mock(ObjectListing.class);
        when(listing.getObjectSummaries()).thenReturn(Collections.singletonList(summ));
        when(amazonS3.listObjects(eq(container))).thenReturn(listing);
        when(amazonS3.getObjectAsString(eq(container), eq("Tags/tag1"))).thenReturn("val1");

        Map<String, String> tags = adapter.getTags(account, container);
        assertEquals(1, tags.size());
        assertEquals("val1", tags.get("tag1"));
    }

    @Test
    void getTags_shouldHandle_useAccountAsBucketname_true() throws Exception {
        Field useAccount = S3Adapter.class.getDeclaredField("useAccountAsBucketname");
        useAccount.setAccessible(true);
        useAccount.set(adapter, true);

        String account = "acct";
        String container = "cont";

        S3ObjectSummary summ = new S3ObjectSummary();
        summ.setKey("packet/Tags/tagA");
        ObjectListing listing = mock(ObjectListing.class);
        when(listing.getObjectSummaries()).thenReturn(Collections.singletonList(summ));
        // when listing with prefix
        doReturn(listing).when(amazonS3).listObjects(eq(account), anyString());
        doReturn("valA").when(amazonS3).getObjectAsString(eq(account), anyString());

        Map<String, String> tags = adapter.getTags(account, container);
        assertEquals(1, tags.size());
        assertEquals("valA", tags.get("tagA"));
    }

    @Test
    void deleteTags_shouldCall_delete_for_each_tag() throws Exception {
        String account = "a";
        String container = "c";
        List<String> tags = Arrays.asList("t1","t2");

        // assume bucket exists
        when(amazonS3.doesBucketExistV2(container)).thenReturn(true);

        adapter.deleteTags(account, container, tags);
        verify(amazonS3, times(2)).deleteObject(anyString(), anyString());
    }

    @Test
    void getAllObjects_shouldReturn_objectDtos() throws Exception {
        String account = "a";
        String id = "bucket";

        S3ObjectSummary s = new S3ObjectSummary();
        s.setKey("k1/k2/k3");
        Date now = new Date();
        s.setLastModified(now);
        ObjectListing listing = mock(ObjectListing.class);
        when(listing.getObjectSummaries()).thenReturn(Collections.singletonList(s));
        when(amazonS3.listObjects(eq(id.toLowerCase()))).thenReturn(listing);

        List<ObjectDto> objects = adapter.getAllObjects(account, id);
        assertNotNull(objects);
        assertEquals(1, objects.size());
        ObjectDto dto = objects.get(0);
        assertEquals("k1", dto.getSource());
        assertEquals("k2", dto.getProcess());
        assertEquals("k3", dto.getObjectName());
    }

    @Test
    void getAllObjects_shouldHandle_useAccountAsBucketname_true() throws Exception {
        // set useAccount true
        Field useAccount = S3Adapter.class.getDeclaredField("useAccountAsBucketname");
        useAccount.setAccessible(true);
        useAccount.set(adapter, true);

        String account = "acct";
        String id = "packetid";

        S3ObjectSummary s = new S3ObjectSummary();
        s.setKey("packet/k1/k2/k3");
        s.setLastModified(new Date());
        ObjectListing listing = mock(ObjectListing.class);
        when(listing.getObjectSummaries()).thenReturn(Collections.singletonList(s));
        // listObjects(account, searchPattern)
        doReturn(listing).when(amazonS3).listObjects(eq(account), anyString());

        List<ObjectDto> objects = adapter.getAllObjects(account, id);
        assertNotNull(objects);
        assertEquals(1, objects.size());
        ObjectDto dto = objects.get(0);
        assertEquals("k1", dto.getSource());
        assertEquals("k2", dto.getProcess());
        assertEquals("k3", dto.getObjectName());
    }

    @Test
    void getAllObjects_should_handle_single_and_two_segment_keys() throws Exception {
        String account = "a";
        String id = "bucket";

        // single segment
        S3ObjectSummary s1 = new S3ObjectSummary();
        s1.setKey("onlyone");
        s1.setLastModified(new Date());
        ObjectListing listing1 = mock(ObjectListing.class);
        when(listing1.getObjectSummaries()).thenReturn(Collections.singletonList(s1));
        when(amazonS3.listObjects(eq(id.toLowerCase()))).thenReturn(listing1);

        List<ObjectDto> o1 = adapter.getAllObjects(account, id);
        assertNotNull(o1);
        assertEquals(1, o1.size());
        assertNull(o1.get(0).getSource());
        assertNull(o1.get(0).getProcess());
        assertEquals("onlyone", o1.get(0).getObjectName());

        // two segments
        S3ObjectSummary s2 = new S3ObjectSummary();
        s2.setKey("k1/k2");
        s2.setLastModified(new Date());
        ObjectListing listing2 = mock(ObjectListing.class);
        when(listing2.getObjectSummaries()).thenReturn(Collections.singletonList(s2));
        when(amazonS3.listObjects(eq(id.toLowerCase()))).thenReturn(listing2);

        List<ObjectDto> o2 = adapter.getAllObjects(account, id);
        assertNotNull(o2);
        assertEquals(1, o2.size());
        assertEquals("k1", o2.get(0).getSource());
        assertNull(o2.get(0).getProcess());
        assertEquals("k2", o2.get(0).getObjectName());
    }

    @Test
    void getMetaData_shouldThrow_when_getObject_returns_null() throws Exception {
        String account = "a";
        String container = "c";
        String objectName = "o";
        // simulate getObject returns null -> leads to NPE handled -> ObjectStoreAdapterException
        doReturn(null).when(amazonS3).getObject(eq(container), anyString());
        assertThrows(io.mosip.commons.khazana.exception.ObjectStoreAdapterException.class,
                () -> adapter.getMetaData(account, container, "s","p", objectName));
    }

    @Test
    void inc_dec_metadata_should_return_null_when_key_missing() throws Exception {
        S3Adapter spy = spy(adapter);
        doReturn(Collections.emptyMap()).when(spy).getMetaData(anyString(), anyString(), anyString(), anyString(), anyString());
        Integer inc = spy.incMetadata("a","b","s","p","o","missing");
        Integer dec = spy.decMetadata("a","b","s","p","o","missing");
        assertNull(inc);
        assertNull(dec);
    }

    @Test
    void deleteObject_shouldCall_delete_and_return_true() throws Exception {
        boolean ok = adapter.deleteObject("a","b","s","p","o");
        verify(amazonS3).deleteObject(anyString(), anyString());
        assertTrue(ok);
    }

    @Test
    void addObjectMetaData_overload_should_delegate_to_map_version() throws Exception {
        S3Adapter spy = spy(adapter);
        Map<String,Object> returned = new HashMap<>();
        returned.put("k","v");
        doReturn(returned).when(spy).addObjectMetaData(anyString(), anyString(), anyString(), anyString(), anyString(), anyMap());

        Map<String,Object> out = spy.addObjectMetaData("a","b","s","p","o","key","val");
        assertEquals("v", out.get("k"));
    }

    @Test
    void doesBucketExists_branches_and_removeContainer_pack() throws Exception {
        // make useAccount true and populate existingBuckets
        Field useAccount = S3Adapter.class.getDeclaredField("useAccountAsBucketname");
        useAccount.setAccessible(true);
        useAccount.set(adapter, true);

        Field existing = S3Adapter.class.getDeclaredField("existingBuckets");
        existing.setAccessible(true);
        List<String> list = (List<String>) existing.get(adapter);
        list.add("bucket1");

        // when present should return true
        Method does = S3Adapter.class.getDeclaredMethod("doesBucketExists", String.class);
        does.setAccessible(true);
        boolean ok1 = (boolean) does.invoke(adapter, "bucket1");
        assertTrue(ok1);

        // when not present the adapter will call connection.doesBucketExistV2
        when(amazonS3.doesBucketExistV2("bucketX")).thenReturn(true);
        boolean ok2 = (boolean) does.invoke(adapter, "bucketX");
        assertTrue(ok2);

        // removeContainer & pack are not supported
        assertFalse(adapter.removeContainer("a","b","s","p"));
        assertFalse(adapter.pack("a","b","s","p","r"));
    }

    @Test
    void removeIdFromObjectPath_should_remove_first_element_when_flag_set() throws Exception {
        Field useAccount = S3Adapter.class.getDeclaredField("useAccountAsBucketname");
        useAccount.setAccessible(true);
        useAccount.set(adapter, true);

        Method m = S3Adapter.class.getDeclaredMethod("removeIdFromObjectPath", boolean.class, String[].class);
        m.setAccessible(true);
        String[] keys = new String[]{"id","a","b"};
        String[] out = (String[]) m.invoke(adapter, true, keys);
        assertArrayEquals(new String[]{"a","b"}, out);

        String[] out2 = (String[]) m.invoke(adapter, false, keys);
        assertArrayEquals(keys, out2);
    }

    @Test
    void getConnection_shouldReturn_existing_connection_when_set() throws Exception {
        // ensure connection field set to our mock and getConnection returns it
        Field connField = S3Adapter.class.getDeclaredField("connection");
        connField.setAccessible(true);
        connField.set(adapter, amazonS3);

        Method getConn = S3Adapter.class.getDeclaredMethod("getConnection", String.class);
        getConn.setAccessible(true);
        Object returned = getConn.invoke(adapter, "anyBucket");
        assertSame(amazonS3, returned);
    }

    @Test
    void getConnection_shouldThrow_ObjectStoreAdapterException_on_build_failure() throws Exception {
        // ensure connection null
        Field connField = S3Adapter.class.getDeclaredField("connection");
        connField.setAccessible(true);
        connField.set(adapter, null);
        // set maxRetry to 0 so it throws immediately on exception
        Field maxRetryField = S3Adapter.class.getDeclaredField("maxRetry");
        maxRetryField.setAccessible(true);
        maxRetryField.set(adapter, 0);
        // set invalid credentials/url to cause client to throw when doing doesBucketExistV2
        Field accessKeyF = S3Adapter.class.getDeclaredField("accessKey");
        accessKeyF.setAccessible(true);
        accessKeyF.set(adapter, "");
        Field secretKeyF = S3Adapter.class.getDeclaredField("secretKey");
        secretKeyF.setAccessible(true);
        secretKeyF.set(adapter, "");
        Field urlF = S3Adapter.class.getDeclaredField("url");
        urlF.setAccessible(true);
        urlF.set(adapter, "http://invalid:1");
        Field regionF = S3Adapter.class.getDeclaredField("region");
        regionF.setAccessible(true);
        regionF.set(adapter, "us-east-1");

        Method getConn = S3Adapter.class.getDeclaredMethod("getConnection", String.class);
        getConn.setAccessible(true);
        try {
            getConn.invoke(adapter, "bucketX");
            fail("expected InvocationTargetException");
        } catch (java.lang.reflect.InvocationTargetException ite) {
            assertTrue(ite.getCause() instanceof io.mosip.commons.khazana.exception.ObjectStoreAdapterException);
        }
    }


    @Test
    void getObject_shouldReturnStream_whenS3ReturnsObject() throws Exception {
        String account = "a";
        String container = "c";
        String source = "s";
        String process = "p";
        String objectName = "o";

        byte[] data = "payload".getBytes(StandardCharsets.UTF_8);
        when(amazonS3.getObject(anyString(), anyString())).thenReturn(s3Object);
        when(s3Object.getObjectContent()).thenReturn(new S3ObjectInputStream(new ByteArrayInputStream(data), null));

        InputStream is = adapter.getObject(account, container, source, process, objectName);
        assertNotNull(is);
        byte[] got = is.readAllBytes();
        assertArrayEquals(data, got);
        verify(s3Object).close();
    }

    @Test
    void getObject_shouldReturnStream_when_useAccount_true() throws Exception {
        Field useAccount = S3Adapter.class.getDeclaredField("useAccountAsBucketname");
        useAccount.setAccessible(true);
        useAccount.set(adapter, true);

        String account = "acct";
        String container = "cont";
        String finalKey = "cont/s/p/o";

        byte[] data = "payload2".getBytes(StandardCharsets.UTF_8);
        when(amazonS3.getObject(eq(account), anyString())).thenReturn(s3Object);
        when(s3Object.getObjectContent()).thenReturn(new S3ObjectInputStream(new ByteArrayInputStream(data), null));

        InputStream is = adapter.getObject(account, container, "s", "p", "o");
        assertNotNull(is);
        assertArrayEquals(data, is.readAllBytes());
        verify(s3Object).close();
    }

    @Test
    void exists_shouldReturn_true_when_object_exists() throws Exception {
        when(amazonS3.doesObjectExist(anyString(), anyString())).thenReturn(true);
        boolean ok = adapter.exists("a","c","s","p","o");
        assertTrue(ok);
    }

    @Test
    void exists_shouldUse_account_bucket_when_useAccount_true() throws Exception {
        Field useAccount = S3Adapter.class.getDeclaredField("useAccountAsBucketname");
        useAccount.setAccessible(true);
        useAccount.set(adapter, true);

        when(amazonS3.doesObjectExist(eq("acct"), anyString())).thenReturn(true);
        boolean ok = adapter.exists("acct", "cont", "s", "p", "o");
        assertTrue(ok);
    }

    @Test
    void putObject_shouldCreateBucketAndPut_whenBucketMissing() throws Exception {
        Field connField = S3Adapter.class.getDeclaredField("connection");
        connField.setAccessible(true);
        connField.set(adapter, amazonS3);

        when(amazonS3.doesBucketExistV2("cont")).thenReturn(false);
        // createBucket returns a Bucket object; stub it accordingly
        when(amazonS3.createBucket("cont")).thenReturn(new com.amazonaws.services.s3.model.Bucket("cont"));
        doReturn(new PutObjectResult()).when(amazonS3).putObject(anyString(), anyString(), any(InputStream.class), any());

        boolean res = adapter.putObject("a","cont","s","p","o", new ByteArrayInputStream(new byte[0]));
        assertTrue(res);
        verify(amazonS3).createBucket("cont");
        verify(amazonS3).putObject(eq("cont"), anyString(), any(InputStream.class), any());
    }

    @Test
    void putObject_shouldCreateBucketAndPut_when_useAccount_true() throws Exception {
        Field useAccount = S3Adapter.class.getDeclaredField("useAccountAsBucketname");
        useAccount.setAccessible(true);
        useAccount.set(adapter, true);

        // ensure connection mocked
        Field connField = S3Adapter.class.getDeclaredField("connection");
        connField.setAccessible(true);
        connField.set(adapter, amazonS3);

        when(amazonS3.doesBucketExistV2("acct")).thenReturn(false);
        when(amazonS3.createBucket("acct")).thenReturn(new com.amazonaws.services.s3.model.Bucket("acct"));
        doReturn(new PutObjectResult()).when(amazonS3).putObject(anyString(), anyString(), any(InputStream.class), any());

        boolean res = adapter.putObject("acct", "cont", "s", "p", "o", new ByteArrayInputStream(new byte[0]));
        assertTrue(res);
        verify(amazonS3).createBucket("acct");
        verify(amazonS3).putObject(eq("acct"), anyString(), any(InputStream.class), any());
    }

    @Test
    void getMetaData_shouldReturn_empty_when_objectMetadata_null() throws Exception {
        String account = "a";
        String container = "c";
        String objectName = "obj";

        doReturn(s3Object).when(amazonS3).getObject(eq(container), anyString());
        when(s3Object.getObjectMetadata()).thenReturn(null);

        Map<String, Object> res = adapter.getMetaData(account, container, "s", "p", objectName);
        assertNotNull(res);
        assertTrue(res.isEmpty());
    }

    @Test
    void getAllObjects_should_filter_out_tag_file_and_return_empty_for_4_segments() throws Exception {
        String account = "a";
        String id = "bucket";

        // tag file (should be ignored and lead to empty result)
        S3ObjectSummary tag = new S3ObjectSummary();
        tag.setKey("Tags/someTag");
        ObjectListing listingTag = mock(ObjectListing.class);
        when(listingTag.getObjectSummaries()).thenReturn(Collections.singletonList(tag));
        when(amazonS3.listObjects(eq(id.toLowerCase()))).thenReturn(listingTag);

        List<ObjectDto> objectsTag = adapter.getAllObjects(account, id);
        assertNull(objectsTag);

        // 4-segment key -> no ObjectDto added, expect empty list
        S3ObjectSummary s4 = new S3ObjectSummary();
        s4.setKey("a/b/c/d");
        s4.setLastModified(new Date());
        ObjectListing listing4 = mock(ObjectListing.class);
        when(listing4.getObjectSummaries()).thenReturn(Collections.singletonList(s4));
        when(amazonS3.listObjects(eq(id.toLowerCase()))).thenReturn(listing4);

        List<ObjectDto> objects4 = adapter.getAllObjects(account, id);
        assertNotNull(objects4);
        assertEquals(0, objects4.size());
    }

    @Test
    void addTags_shouldHandle_AccessDenied_backwardCompatibility() throws Exception {
        String account = "a";
        String container = "c";
        Map<String,String> tags = Map.of("t1","v1");

        AmazonS3Exception ex = new AmazonS3Exception("Access Denied");
        when(amazonS3.doesBucketExistV2(container)).thenReturn(true);
        when(amazonS3.doesObjectExist(eq(container), anyString())).thenReturn(true);
        when(amazonS3.putObject(eq(container), anyString(), any(InputStream.class), any())).thenThrow(ex).thenReturn(new PutObjectResult());

        Map<String,String> out = adapter.addTags(account, container, tags);
        assertEquals(1, out.size());
        verify(amazonS3).deleteObject(eq(container), anyString());
    }

    @Test
    void deleteTags_shouldCreateBucket_when_missing() throws Exception {
        String account = "a";
        String container = "c";
        List<String> tags = List.of("t1");

        when(amazonS3.doesBucketExistV2(container)).thenReturn(false);
        adapter.deleteTags(account, container, tags);
        verify(amazonS3).createBucket(container);
        verify(amazonS3).deleteObject(anyString(), anyString());
    }

    @Test
    void addBucketPrefix_shouldAdd_andNotDuplicate() throws Exception {
        Field bp = S3Adapter.class.getDeclaredField("bucketNamePrefix");
        bp.setAccessible(true);
        bp.set(adapter, "pre-");

        Method m = S3Adapter.class.getDeclaredMethod("addBucketPrefix", String.class);
        m.setAccessible(true);
        String out1 = (String) m.invoke(adapter, "name");
        assertEquals("pre-name", out1);
        String out2 = (String) m.invoke(adapter, "pre-name");
        assertEquals("pre-name", out2);
    }

    @Test
    void addTags_backwardCompatibility_shouldThrow_when_finalObject_missing() throws Exception {
        String account = "a";
        String container = "c";
        Map<String,String> tags = Map.of("t1","v1");

        AmazonS3Exception ex = new AmazonS3Exception("Object-prefix is already an object, please choose a different object-prefix name");
        when(amazonS3.doesBucketExistV2(container)).thenReturn(true);
        // finalObject doesn't exist
        when(amazonS3.doesObjectExist(eq(container), anyString())).thenReturn(false);
        when(amazonS3.putObject(eq(container), anyString(), any(InputStream.class), any())).thenThrow(ex);

        assertThrows(io.mosip.commons.khazana.exception.ObjectStoreAdapterException.class,
                () -> adapter.addTags(account, container, tags));
    }

    @Test
    void deleteTags_should_create_bucket_when_useAccount_true() throws Exception {
        Field useAccount = S3Adapter.class.getDeclaredField("useAccountAsBucketname");
        useAccount.setAccessible(true);
        useAccount.set(adapter, true);

        String account = "acct";
        String container = "cont";
        List<String> tags = List.of("t1");

        when(amazonS3.doesBucketExistV2(account)).thenReturn(false);

        adapter.deleteTags(account, container, tags);
        // createBucket invoked for container as code calls getConnection(container) internally
        verify(amazonS3).createBucket(eq(container));
        verify(amazonS3).deleteObject(anyString(), anyString());
    }

    @Test
    void getTags_shouldReturn_empty_when_no_summaries() throws Exception {
        String account = "a";
        String container = "c";

        ObjectListing listing = mock(ObjectListing.class);
        when(listing.getObjectSummaries()).thenReturn(Collections.emptyList());
        when(amazonS3.listObjects(eq(container))).thenReturn(listing);

        Map<String,String> tags = adapter.getTags(account, container);
        assertNotNull(tags);
        assertTrue(tags.isEmpty());
    }

    @Test
    void getAllObjects_shouldReturn_null_when_no_objects() throws Exception {
        String account = "a";
        String id = "bucket";

        ObjectListing listing = mock(ObjectListing.class);
        when(listing.getObjectSummaries()).thenReturn(Collections.emptyList());
        when(amazonS3.listObjects(eq(id.toLowerCase()))).thenReturn(listing);

        List<ObjectDto> objects = adapter.getAllObjects(account, id);
        assertNull(objects);
    }

    @Test
    void getObject_shouldReturn_null_when_s3object_null() throws Exception {
        when(amazonS3.getObject(anyString(), anyString())).thenReturn(null);
        InputStream is = adapter.getObject("a","c","s","p","o");
        assertNull(is);
    }

}
