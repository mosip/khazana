package io.mosip.commons.khazana.test.adapter;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.mosip.commons.khazana.dto.ObjectDto;
import io.mosip.commons.khazana.dto.ObjectStoreReference;
import io.mosip.commons.khazana.exception.ObjectStoreAdapterException;
import io.mosip.commons.khazana.impl.S3Adapter;
import org.apache.http.client.methods.HttpGet;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Covers {@link S3Adapter} against a mocked AWS SDK 1.x {@link AmazonS3} client.
 */
public class S3AdapterTest {

    private static final String ACCOUNT = "testaccount";
    private static final String CONTAINER = "testbucket";
    private static final String SRC_KEY = "_draft/abc123/Biometrics/bio.cbeff";
    private static final String DEST_KEY = "uinHash/Biometrics/bio.cbeff";

    private S3Adapter adapter;
    private AmazonS3 s3;

    @Before
    public void setUp() {
        adapter = new S3Adapter();
        s3 = mock(AmazonS3.class);
        ReflectionTestUtils.setField(adapter, "connection", s3);
        ReflectionTestUtils.setField(adapter, "useAccountAsBucketname", false);
        ReflectionTestUtils.setField(adapter, "bucketNamePrefix", "");
        ReflectionTestUtils.setField(adapter, "readlimit", 1000);
        ReflectionTestUtils.setField(adapter, "maxRetry", 1);
        ReflectionTestUtils.setField(adapter, "accessKey", "ak");
        ReflectionTestUtils.setField(adapter, "secretKey", "sk");
        ReflectionTestUtils.setField(adapter, "url", "http://127.0.0.1:1");
        ReflectionTestUtils.setField(adapter, "region", "us-east-1");
        ReflectionTestUtils.setField(adapter, "maxConnection", 2);
        ReflectionTestUtils.setField(adapter, "connectionTimeout", 100);
        ReflectionTestUtils.setField(adapter, "socketTimeout", 100);
        ReflectionTestUtils.setField(adapter, "clientExecutionTimeout", 100);
        when(s3.doesBucketExistV2(anyString())).thenReturn(true);
    }

    @Test
    public void getExistsPutDelete_useContainerAsBucket() throws Exception {
        S3Object object = s3Object("hello");
        when(s3.getObject(anyString(), anyString())).thenReturn(object);
        when(s3.doesObjectExist(anyString(), anyString())).thenReturn(true);

        InputStream in = adapter.getObject("acct", "Bucket", "src", "proc", "a.bin");
        assertEquals("hello", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        in.close();
        assertTrue(adapter.exists("acct", "Bucket", "src", "proc", "a.bin"));
        assertTrue(adapter.putObject("acct", "Bucket", "src", "proc", "a.bin", new ByteArrayInputStream(new byte[]{1})));
        assertTrue(adapter.deleteObject("acct", "Bucket", "src", "proc", "a.bin"));
        verify(s3).getObject("bucket", "src/proc/a.bin");
    }

    @Test
    public void getObject_accountIsBucket_andNullObjectReturnsNull() {
        ReflectionTestUtils.setField(adapter, "useAccountAsBucketname", true);
        ReflectionTestUtils.setField(adapter, "bucketNamePrefix", "mosip-");
        when(s3.getObject(anyString(), anyString())).thenReturn(null);
        assertNull(adapter.getObject("Account", "box", "src", "", "a.bin"));
        verify(s3).getObject("mosip-account", "box/src/a.bin");
    }

    @Test
    public void getObject_metadataFailureClosesObject() {
        S3Object object = mock(S3Object.class);
        when(object.getObjectMetadata()).thenThrow(new RuntimeException("meta"));
        when(s3.getObject(anyString(), anyString())).thenReturn(object);
        try {
            adapter.getObject("a", "b", "s", "p", "o");
        } catch (ObjectStoreAdapterException expected) {
            assertNotNull(expected.getCause());
        }
    }

    @Test
    public void existsAndPut_throwWhenClientFails() {
        when(s3.doesObjectExist(anyString(), anyString())).thenThrow(new RuntimeException("down"));
        try {
            adapter.exists("a", "b", "s", "p", "o");
        } catch (ObjectStoreAdapterException expected) {
            assertNotNull(expected);
        }
        when(s3.doesBucketExistV2(anyString())).thenThrow(new RuntimeException("down"));
        try {
            adapter.putObject("a", "b", "s", "p", "o", new ByteArrayInputStream(new byte[]{1}));
        } catch (ObjectStoreAdapterException expected) {
            assertNotNull(expected);
        }
    }

    @Test
    public void putObject_createsBucketWhenMissing() {
        when(s3.doesBucketExistV2(anyString())).thenReturn(false);
        assertTrue(adapter.putObject("a", "Box", "s", "p", "o", new ByteArrayInputStream(new byte[]{1})));
        verify(s3).createBucket("box");
    }

    @Test
    public void metadata_addGetIncDec() throws Exception {
        ObjectMetadata head = new ObjectMetadata();
        head.addUserMetadata("n", "2");
        when(s3.getObjectMetadata(anyString(), anyString())).thenReturn(head);
        S3Object object = s3Object("body");
        object.setObjectMetadata(head);
        when(s3.getObject(anyString(), anyString())).thenReturn(object);

        Map<String, Object> meta = new HashMap<>();
        meta.put("k", "v");
        meta.put("nil", null);
        assertEquals(meta, adapter.addObjectMetaData("a", "b", "s", "p", "o", meta));
        verify(s3).putObject(any(PutObjectRequest.class));

        Map<String, Object> one = adapter.addObjectMetaData("a", "b", "s", "p", "o", "k", "v");
        assertEquals("v", one.get("k"));

        Map<String, Object> read = adapter.getMetaData("a", "b", "s", "p", "o");
        assertEquals("2", read.get("n"));
        assertEquals(Integer.valueOf(3), adapter.incMetadata("a", "b", "s", "p", "o", "n"));
        assertEquals(Integer.valueOf(1), adapter.decMetadata("a", "b", "s", "p", "o", "n"));
        assertNull(adapter.incMetadata("a", "b", "s", "p", "o", "missing"));
        assertNull(adapter.decMetadata("a", "b", "s", "p", "o", "missing"));
    }

    @Test
    public void getMetaData_notFoundReturnsEmpty_otherErrorsThrow() {
        AmazonS3Exception missing = new AmazonS3Exception("no");
        missing.setStatusCode(404);
        org.mockito.Mockito.doThrow(missing).when(s3).getObjectMetadata(anyString(), anyString());
        assertTrue(adapter.getMetaData("a", "b", "s", "p", "o").isEmpty());

        AmazonS3Exception denied = new AmazonS3Exception("no");
        denied.setStatusCode(403);
        org.mockito.Mockito.doThrow(denied).when(s3).getObjectMetadata(anyString(), anyString());
        try {
            adapter.getMetaData("a", "b", "s", "p", "o");
        } catch (ObjectStoreAdapterException expected) {
            assertNotNull(expected);
        }
        org.mockito.Mockito.doThrow(new RuntimeException("boom")).when(s3).getObjectMetadata(anyString(), anyString());
        try {
            adapter.getMetaData("a", "b", "s", "p", "o");
        } catch (ObjectStoreAdapterException expected) {
            assertNotNull(expected);
        }
    }

    @Test
    public void getMetaData_accountBucketAndPrefixAlreadyPresent() {
        ReflectionTestUtils.setField(adapter, "useAccountAsBucketname", true);
        ReflectionTestUtils.setField(adapter, "bucketNamePrefix", "mosip-");
        when(s3.getObjectMetadata(anyString(), anyString())).thenReturn(new ObjectMetadata());
        assertTrue(adapter.getMetaData("mosip-acct", "box", "s", "p", "o").isEmpty());
        verify(s3).getObjectMetadata("mosip-acct", "box/s/p/o");
    }

    @Test
    public void removeContainerAndPack_returnFalse() {
        assertFalse(adapter.removeContainer("a", "b", "s", "p"));
        assertFalse(adapter.pack("a", "b", "s", "p", "ref"));
    }

    @Test
    public void moveObject_copiesAndDoesNotDeleteWhenFlagFalse() {
        when(s3.copyObject(anyString(), anyString(), anyString(), anyString()))
                .thenReturn(new CopyObjectResult());
        ObjectStoreReference src = new ObjectStoreReference(ACCOUNT, CONTAINER, null, null, SRC_KEY);
        ObjectStoreReference dst = new ObjectStoreReference(ACCOUNT, CONTAINER, null, null, DEST_KEY);

        assertTrue(adapter.moveObject(src, dst, false));
        verify(s3).copyObject(CONTAINER, SRC_KEY, CONTAINER, DEST_KEY);
        verify(s3, never()).deleteObject(anyString(), anyString());
    }

    @Test
    public void moveObject_deletesSourceWhenFlagTrue() {
        when(s3.copyObject(anyString(), anyString(), anyString(), anyString()))
                .thenReturn(new CopyObjectResult());
        ObjectStoreReference src = new ObjectStoreReference(ACCOUNT, CONTAINER, null, null, SRC_KEY);
        ObjectStoreReference dst = new ObjectStoreReference(ACCOUNT, CONTAINER, null, null, DEST_KEY);

        assertTrue(adapter.moveObject(src, dst, true));
        verify(s3).copyObject(CONTAINER, SRC_KEY, CONTAINER, DEST_KEY);
        verify(s3).deleteObject(CONTAINER, SRC_KEY);
    }

    @Test
    public void moveObject_accountAsBucketPrefixesContainerOnKey() {
        ReflectionTestUtils.setField(adapter, "useAccountAsBucketname", true);
        when(s3.copyObject(anyString(), anyString(), anyString(), anyString()))
                .thenReturn(new CopyObjectResult());
        ObjectStoreReference src = new ObjectStoreReference(ACCOUNT, CONTAINER, null, null, SRC_KEY);
        ObjectStoreReference dst = new ObjectStoreReference(ACCOUNT, CONTAINER, null, null, DEST_KEY);

        assertTrue(adapter.moveObject(src, dst, false));
        verify(s3).copyObject(ACCOUNT, CONTAINER + "/" + SRC_KEY, ACCOUNT, CONTAINER + "/" + DEST_KEY);
    }

    @Test
    public void moveObject_wrapsAmazonS3Exception() {
        AmazonS3Exception missing = new AmazonS3Exception("no");
        missing.setStatusCode(404);
        when(s3.copyObject(anyString(), anyString(), anyString(), anyString())).thenThrow(missing);
        ObjectStoreReference src = new ObjectStoreReference(ACCOUNT, CONTAINER, null, null, SRC_KEY);
        ObjectStoreReference dst = new ObjectStoreReference(ACCOUNT, CONTAINER, null, null, DEST_KEY);
        try {
            adapter.moveObject(src, dst, false);
        } catch (ObjectStoreAdapterException expected) {
            assertSame(missing, expected.getCause());
        }
    }

    @Test
    public void moveObject_wrapsUnexpectedException() {
        when(s3.copyObject(anyString(), anyString(), anyString(), anyString()))
                .thenThrow(new RuntimeException("connection reset"));
        ObjectStoreReference src = new ObjectStoreReference(ACCOUNT, CONTAINER, null, null, SRC_KEY);
        ObjectStoreReference dst = new ObjectStoreReference(ACCOUNT, CONTAINER, null, null, DEST_KEY);
        try {
            adapter.moveObject(src, dst, false);
        } catch (ObjectStoreAdapterException expected) {
            assertNotNull(expected.getCause());
        }
    }

    @Test
    public void listObjectsByPrefix_returnsMatchingKeys() {
        String prefix = "_draft/abc123/Biometrics/";
        ListObjectsV2Result page = mock(ListObjectsV2Result.class);
        when(page.getObjectSummaries()).thenReturn(List.of(
                summary(prefix + "bio1.cbeff"),
                summary(prefix + "bio2.cbeff")));
        when(page.isTruncated()).thenReturn(false);
        when(s3.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(page);

        List<String> result = adapter.listObjectsByPrefix(ACCOUNT, CONTAINER, prefix);
        assertEquals(2, result.size());
        assertTrue(result.contains(prefix + "bio1.cbeff"));
        assertTrue(result.contains(prefix + "bio2.cbeff"));

        ArgumentCaptor<ListObjectsV2Request> captor = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        verify(s3).listObjectsV2(captor.capture());
        assertEquals(CONTAINER, captor.getValue().getBucketName());
        assertEquals(prefix, captor.getValue().getPrefix());
    }

    @Test
    public void listObjectsByPrefix_emptyWhenNoMatch() {
        ListObjectsV2Result page = mock(ListObjectsV2Result.class);
        when(page.getObjectSummaries()).thenReturn(List.of());
        when(page.isTruncated()).thenReturn(false);
        when(s3.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(page);
        assertTrue(adapter.listObjectsByPrefix(ACCOUNT, CONTAINER, "_draft/missing/").isEmpty());
    }

    @Test
    public void listObjectsByPrefix_stripsContainerWhenAccountIsBucket() {
        ReflectionTestUtils.setField(adapter, "useAccountAsBucketname", true);
        String prefix = "_draft/abc123/Biometrics/";
        ListObjectsV2Result page = mock(ListObjectsV2Result.class);
        when(page.getObjectSummaries()).thenReturn(List.of(
                summary(CONTAINER + "/" + prefix + "face.cbeff")));
        when(page.isTruncated()).thenReturn(false);
        when(s3.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(page);

        List<String> result = adapter.listObjectsByPrefix(ACCOUNT, CONTAINER, prefix);
        assertEquals(List.of(prefix + "face.cbeff"), result);

        ArgumentCaptor<ListObjectsV2Request> captor = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        verify(s3).listObjectsV2(captor.capture());
        assertEquals(ACCOUNT, captor.getValue().getBucketName());
        assertEquals(CONTAINER + "/" + prefix, captor.getValue().getPrefix());
    }

    @Test
    public void listThenMove_accountAsBucketUsesStrippedKey() {
        ReflectionTestUtils.setField(adapter, "useAccountAsBucketname", true);
        when(s3.copyObject(anyString(), anyString(), anyString(), anyString()))
                .thenReturn(new CopyObjectResult());
        ObjectStoreReference src = new ObjectStoreReference(ACCOUNT, CONTAINER, null, null, SRC_KEY);
        ObjectStoreReference dst = new ObjectStoreReference(ACCOUNT, CONTAINER, null, null, DEST_KEY);
        assertTrue(adapter.moveObject(src, dst, false));
        verify(s3).copyObject(ACCOUNT, CONTAINER + "/" + SRC_KEY, ACCOUNT, CONTAINER + "/" + DEST_KEY);
    }

    @Test
    public void listObjectsByPrefix_collectsPages() {
        String prefix = "_draft/abc123/Biometrics/";
        ListObjectsV2Result page1 = mock(ListObjectsV2Result.class);
        when(page1.getObjectSummaries()).thenReturn(List.of(summary(prefix + "bio1.cbeff")));
        when(page1.isTruncated()).thenReturn(true);
        when(page1.getNextContinuationToken()).thenReturn("tok");
        ListObjectsV2Result page2 = mock(ListObjectsV2Result.class);
        when(page2.getObjectSummaries()).thenReturn(List.of(summary(prefix + "bio2.cbeff")));
        when(page2.isTruncated()).thenReturn(false);
        when(s3.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(page1, page2);

        List<String> result = adapter.listObjectsByPrefix(ACCOUNT, CONTAINER, prefix);
        assertEquals(2, result.size());
        assertTrue(result.contains(prefix + "bio1.cbeff"));
        assertTrue(result.contains(prefix + "bio2.cbeff"));
    }

    @Test
    public void listObjectsByPrefix_wrapsFailure() {
        AmazonS3Exception denied = new AmazonS3Exception("denied");
        denied.setStatusCode(500);
        when(s3.listObjectsV2(any(ListObjectsV2Request.class))).thenThrow(denied);
        try {
            adapter.listObjectsByPrefix(ACCOUNT, CONTAINER, "_draft/");
        } catch (ObjectStoreAdapterException expected) {
            assertSame(denied, expected.getCause());
        }
    }

    @Test
    public void getAllObjects_bothBucketModes() {
        ObjectListing listing = mock(ObjectListing.class);
        List<S3ObjectSummary> summaries = new ArrayList<>();
        summaries.add(summary("src/proc/file.bin"));
        summaries.add(summary("src/file.bin"));
        summaries.add(summary("file.bin"));
        summaries.add(summary("Tags/skip"));
        when(listing.getObjectSummaries()).thenReturn(summaries);
        when(s3.listObjects(anyString())).thenReturn(listing);
        when(s3.listObjects(anyString(), anyString())).thenReturn(listing);

        List<ObjectDto> objects = adapter.getAllObjects("acct", "Bucket");
        assertEquals(3, objects.size());

        ReflectionTestUtils.setField(adapter, "useAccountAsBucketname", true);
        List<S3ObjectSummary> accountSummaries = new ArrayList<>();
        accountSummaries.add(summary("packet/src/proc/file.bin"));
        accountSummaries.add(summary("packet/Tags/skip"));
        when(listing.getObjectSummaries()).thenReturn(accountSummaries);
        List<ObjectDto> accountObjects = adapter.getAllObjects("Account", "packet");
        assertEquals(1, accountObjects.size());
        assertEquals("src", accountObjects.get(0).getSource());
    }

    @Test
    public void getAllObjects_emptyReturnsNull() {
        ObjectListing listing = mock(ObjectListing.class);
        when(listing.getObjectSummaries()).thenReturn(List.of());
        when(s3.listObjects(anyString())).thenReturn(listing);
        assertNull(adapter.getAllObjects("a", "b"));
    }

    @Test
    public void tags_addGetDelete() {
        ObjectListing listing = mock(ObjectListing.class);
        when(listing.getObjectSummaries()).thenReturn(List.of(summary("Tags/color")));
        when(s3.listObjects(anyString())).thenReturn(listing);
        when(s3.getObjectAsString(anyString(), anyString())).thenReturn("blue");

        Map<String, String> tags = new HashMap<>();
        tags.put("color", "blue");
        assertEquals(tags, adapter.addTags("a", "Bucket", tags));
        assertEquals("blue", adapter.getTags("a", "Bucket").get("color"));
        adapter.deleteTags("a", "Bucket", List.of("color"));
        verify(s3).deleteObject("bucket", "Tags/color");
    }

    @Test
    public void tags_accountBucketAndBackwardCompatibleRetry() {
        ReflectionTestUtils.setField(adapter, "useAccountAsBucketname", true);
        ReflectionTestUtils.setField(adapter, "existingBuckets", new ArrayList<>(List.of("account")));
        AmazonS3Exception denied = new AmazonS3Exception("Access Denied");
        when(s3.putObject(anyString(), anyString(), any(InputStream.class), any(ObjectMetadata.class))).thenThrow(denied);
        when(s3.doesObjectExist(anyString(), anyString())).thenReturn(true, false);
        Map<String, String> tags = Map.of("k", "v");
        try {
            adapter.addTags("account", "box", tags);
        } catch (ObjectStoreAdapterException expected) {
            assertNotNull(expected);
        }
    }

    @Test
    public void accountAsBucket_coversMutationsAndFailures() {
        ReflectionTestUtils.setField(adapter, "useAccountAsBucketname", true);
        ReflectionTestUtils.setField(adapter, "existingBuckets", new ArrayList<String>());
        when(s3.doesBucketExistV2(anyString())).thenReturn(false, true);
        when(s3.doesObjectExist(anyString(), anyString())).thenReturn(true);
        S3Object object = s3Object("z");
        when(s3.getObject(anyString(), anyString())).thenReturn(object);

        assertTrue(adapter.putObject("Account", "box", "src", "proc", "o", new ByteArrayInputStream(new byte[]{1})));
        assertTrue(adapter.exists("Account", "box", "src", "proc", "o"));
        adapter.addObjectMetaData("Account", "box", "src", "proc", "o", Map.of("k", "v"));

        ObjectListing listing = mock(ObjectListing.class);
        when(listing.getObjectSummaries()).thenReturn(List.of(summary("box/Tags/color")));
        when(s3.listObjects(anyString(), anyString())).thenReturn(listing);
        when(s3.getObjectAsString(anyString(), anyString())).thenReturn("blue");
        assertEquals("blue", adapter.getTags("Account", "box").get("color"));

        when(s3.doesBucketExistV2(anyString())).thenReturn(false);
        adapter.deleteTags("Account", "box", List.of("color"));
        Map<String, String> tags = Map.of("k", "v");
        adapter.addTags("Account", "fresh", tags);

        org.mockito.Mockito.doThrow(new RuntimeException("x")).when(s3).deleteObject(anyString(), anyString());
        try {
            adapter.deleteObject("Account", "box", "src", "proc", "o");
        } catch (ObjectStoreAdapterException expected) {
            assertNotNull(expected);
        }
        ReflectionTestUtils.setField(adapter, "connection", s3);
        org.mockito.Mockito.doThrow(new RuntimeException("x")).when(s3).getObjectMetadata(anyString(), anyString());
        try {
            adapter.incMetadata("Account", "box", "src", "proc", "o", "n");
        } catch (ObjectStoreAdapterException expected) {
            assertNotNull(expected);
        }
        ReflectionTestUtils.setField(adapter, "connection", s3);
        try {
            adapter.decMetadata("Account", "box", "src", "proc", "o", "n");
        } catch (ObjectStoreAdapterException expected) {
            assertNotNull(expected);
        }
        ReflectionTestUtils.setField(adapter, "connection", s3);
        org.mockito.Mockito.doThrow(new RuntimeException("x")).when(s3).getObject(anyString(), anyString());
        try {
            adapter.addObjectMetaData("Account", "box", "src", "proc", "o", Map.of("k", "v"));
        } catch (ObjectStoreAdapterException expected) {
            assertNotNull(expected);
        }
        ReflectionTestUtils.setField(adapter, "connection", s3);
        org.mockito.Mockito.doThrow(new RuntimeException("x")).when(s3).listObjects(anyString(), anyString());
        try {
            adapter.getTags("Account", "box");
        } catch (ObjectStoreAdapterException expected) {
            assertNotNull(expected);
        }
    }

    @Test
    public void connectionRetry_givesUpWhenClientCannotBeBuilt() {
        ReflectionTestUtils.setField(adapter, "connection", null);
        ReflectionTestUtils.setField(adapter, "maxRetry", 2);
        try {
            adapter.exists("a", "b", "s", "p", "o");
        } catch (ObjectStoreAdapterException expected) {
            assertNotNull(expected.getMessage());
        }
    }

    private static S3Object s3Object(String body) {
        S3Object object = new S3Object();
        object.setObjectContent(new S3ObjectInputStream(
                new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)),
                new HttpGet("http://127.0.0.1/object")));
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(body.length());
        object.setObjectMetadata(metadata);
        return object;
    }

    private static S3ObjectSummary summary(String key) {
        S3ObjectSummary summary = new S3ObjectSummary();
        summary.setKey(key);
        summary.setLastModified(new Date());
        return summary;
    }
}
