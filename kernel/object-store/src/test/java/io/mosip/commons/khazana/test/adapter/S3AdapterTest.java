package io.mosip.commons.khazana.test.adapter;

import io.mosip.commons.khazana.dto.ObjectStoreReference;
import io.mosip.commons.khazana.exception.ObjectStoreAdapterException;
import io.mosip.commons.khazana.impl.S3Adapter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.util.List;
import java.util.function.Consumer;

import static io.mosip.commons.khazana.constant.KhazanaErrorCodes.OBJECT_STORE_NOT_ACCESSIBLE;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class S3AdapterTest {

    private static final String ACCOUNT   = "testaccount";
    private static final String CONTAINER = "testbucket";
    private static final String SRC_KEY   = "_draft/abc123/Biometrics/bio.cbeff";
    private static final String DEST_KEY  = "Biometrics/bio.cbeff";

    @InjectMocks
    private S3Adapter s3Adapter;

    @Mock
    private S3Client s3Client;

    @Mock
    private ListObjectsV2Iterable paginator;

    @Before
    public void setup() {
        ReflectionTestUtils.setField(s3Adapter, "connection", s3Client);
        ReflectionTestUtils.setField(s3Adapter, "useAccountAsBucketname", false);
        ReflectionTestUtils.setField(s3Adapter, "bucketNamePrefix", "");
    }

    // ── moveObject ────────────────────────────────────────────────────────────

    @Test
    public void should_returnTrue_when_moveObjectSucceeds() {
        ArgumentCaptor<CopyObjectRequest> captor = ArgumentCaptor.forClass(CopyObjectRequest.class);
        when(s3Client.copyObject(any(CopyObjectRequest.class)))
                .thenReturn(CopyObjectResponse.builder().build());
        ObjectStoreReference src = new ObjectStoreReference(ACCOUNT, CONTAINER, null, null, SRC_KEY);
        ObjectStoreReference dst = new ObjectStoreReference(ACCOUNT, CONTAINER, null, null, DEST_KEY);

        boolean result = s3Adapter.moveObject(src, dst, false);

        assertTrue("moveObject should return true on success", result);
        verify(s3Client).copyObject(captor.capture());
        CopyObjectRequest req = captor.getValue();
        assertEquals("source bucket should be normalized container", CONTAINER, req.sourceBucket());
        assertEquals("source key should match", SRC_KEY, req.sourceKey());
        assertEquals("destination bucket should be normalized container", CONTAINER, req.destinationBucket());
        assertEquals("destination key should match", DEST_KEY, req.destinationKey());
    }

    @Test
    public void should_throwObjectStoreAdapterException_when_s3ExceptionOccurs() {
        S3Exception s3Ex = (S3Exception) S3Exception.builder()
                .statusCode(500)
                .message("Internal Server Error")
                .build();
        when(s3Client.copyObject(any(CopyObjectRequest.class))).thenThrow(s3Ex);
        ObjectStoreReference src = new ObjectStoreReference(ACCOUNT, CONTAINER, null, null, SRC_KEY);
        ObjectStoreReference dst = new ObjectStoreReference(ACCOUNT, CONTAINER, null, null, DEST_KEY);

        assertThrows(
                "S3Exception should be wrapped as ObjectStoreAdapterException",
                ObjectStoreAdapterException.class,
                () -> s3Adapter.moveObject(src, dst, false)
        );
    }

    @Test
    public void should_throwObjectStoreAdapterException_when_unexpectedExceptionOccurs() {
        when(s3Client.copyObject(any(CopyObjectRequest.class)))
                .thenThrow(new RuntimeException("connection reset"));
        ObjectStoreReference src = new ObjectStoreReference(ACCOUNT, CONTAINER, null, null, SRC_KEY);
        ObjectStoreReference dst = new ObjectStoreReference(ACCOUNT, CONTAINER, null, null, DEST_KEY);

        assertThrows(
                "Unexpected exception should be wrapped as ObjectStoreAdapterException",
                ObjectStoreAdapterException.class,
                () -> s3Adapter.moveObject(src, dst, false)
        );
    }

    @Test
    public void should_useAccountAsBucketNameAndReturnTrue_when_useAccountAsBucketnameIsTrue() {
        ReflectionTestUtils.setField(s3Adapter, "useAccountAsBucketname", true);
        ArgumentCaptor<CopyObjectRequest> captor = ArgumentCaptor.forClass(CopyObjectRequest.class);
        when(s3Client.copyObject(any(CopyObjectRequest.class)))
                .thenReturn(CopyObjectResponse.builder().build());
        ObjectStoreReference src = new ObjectStoreReference(ACCOUNT, CONTAINER, null, null, SRC_KEY);
        ObjectStoreReference dst = new ObjectStoreReference(ACCOUNT, CONTAINER, null, null, DEST_KEY);

        boolean result = s3Adapter.moveObject(src, dst, false);

        assertTrue("Should use account as bucket name and still return true", result);
        verify(s3Client).copyObject(captor.capture());
        CopyObjectRequest req = captor.getValue();
        assertEquals("source bucket should be account when useAccountAsBucketname is true", ACCOUNT, req.sourceBucket());
        assertEquals("source key should be container-prefixed when useAccountAsBucketname is true", CONTAINER + "/" + SRC_KEY, req.sourceKey());
        assertEquals("destination bucket should be account when useAccountAsBucketname is true", ACCOUNT, req.destinationBucket());
        assertEquals("destination key should be container-prefixed when useAccountAsBucketname is true", CONTAINER + "/" + DEST_KEY, req.destinationKey());
    }

    @Test
    public void should_deleteSourceObject_when_deleteSourceAfterCopyIsTrue() {
        ArgumentCaptor<DeleteObjectRequest> deleteCaptor = ArgumentCaptor.forClass(DeleteObjectRequest.class);
        when(s3Client.copyObject(any(CopyObjectRequest.class)))
                .thenReturn(CopyObjectResponse.builder().build());
        ObjectStoreReference src = new ObjectStoreReference(ACCOUNT, CONTAINER, null, null, SRC_KEY);
        ObjectStoreReference dst = new ObjectStoreReference(ACCOUNT, CONTAINER, null, null, DEST_KEY);

        boolean result = s3Adapter.moveObject(src, dst, true);

        assertTrue("moveObject with deleteSourceAfterCopy=true should return true", result);
        verify(s3Client).copyObject(any(CopyObjectRequest.class));
        verify(s3Client).deleteObject(deleteCaptor.capture());
        DeleteObjectRequest deleteReq = deleteCaptor.getValue();
        assertEquals("delete bucket should be normalized container", CONTAINER, deleteReq.bucket());
        assertEquals("delete key should be the source key", SRC_KEY, deleteReq.key());
    }

    // ── listObjectsByPrefix ───────────────────────────────────────────────────

    @Test
    public void should_returnKeys_when_listObjectsByPrefixSucceeds() {
        String prefix = "_draft/abc123/Biometrics/";
        List<S3Object> objects = List.of(
                S3Object.builder().key(prefix + "bio1.cbeff").build(),
                S3Object.builder().key(prefix + "bio2.cbeff").build()
        );
        ListObjectsV2Response page = ListObjectsV2Response.builder().contents(objects).build();

        when(s3Client.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenReturn(paginator);
        doAnswer(invocation -> {
            Consumer<ListObjectsV2Response> consumer = invocation.getArgument(0);
            consumer.accept(page);
            return null;
        }).when(paginator).forEach(any());

        List<String> result = s3Adapter.listObjectsByPrefix(ACCOUNT, CONTAINER, prefix);

        assertEquals(2, result.size());
        assertTrue(result.contains(prefix + "bio1.cbeff"));
        assertTrue(result.contains(prefix + "bio2.cbeff"));
        ArgumentCaptor<ListObjectsV2Request> captor = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        verify(s3Client).listObjectsV2Paginator(captor.capture());
        assertEquals("bucket should be container when useAccountAsBucketname=false", CONTAINER, captor.getValue().bucket());
        assertEquals("prefix should be passed through", prefix, captor.getValue().prefix());
    }

    @Test
    public void should_returnEmptyList_when_noObjectsMatchPrefix() {
        String prefix = "_draft/nonexistent/Biometrics/";
        ListObjectsV2Response emptyPage = ListObjectsV2Response.builder().contents(List.of()).build();

        when(s3Client.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenReturn(paginator);
        doAnswer(invocation -> {
            Consumer<ListObjectsV2Response> consumer = invocation.getArgument(0);
            consumer.accept(emptyPage);
            return null;
        }).when(paginator).forEach(any());

        List<String> result = s3Adapter.listObjectsByPrefix(ACCOUNT, CONTAINER, prefix);

        assertTrue("result should be empty when no objects match prefix", result.isEmpty());
    }

    public void should_useAccountAsBucketName_when_useAccountAsBucketnameIsTrue() {
        ReflectionTestUtils.setField(s3Adapter, "useAccountAsBucketname", true);
        String prefix = "_draft/abc123/Biometrics/";
        String expectedS3Prefix = CONTAINER + "/" + prefix;           // what S3 is queried with
        String s3BucketKey = expectedS3Prefix + "face.cbeff";         // what S3 stores/returns
        String expectedReturnedKey = prefix + "face.cbeff";           // what adapter should return (stripped)

        S3Object s3Object = S3Object.builder().key(s3BucketKey).build();
        ListObjectsV2Response page = ListObjectsV2Response.builder().contents(s3Object).build();

        when(s3Client.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenReturn(paginator);
        doAnswer(invocation -> {
            Consumer<ListObjectsV2Response> consumer = invocation.getArgument(0);
            consumer.accept(page);
            return null;
        }).when(paginator).forEach(any());

        List<String> result = s3Adapter.listObjectsByPrefix(ACCOUNT, CONTAINER, prefix);

        ArgumentCaptor<ListObjectsV2Request> captor = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        verify(s3Client).listObjectsV2Paginator(captor.capture());
        assertEquals(ACCOUNT, captor.getValue().bucket());
        assertEquals(expectedS3Prefix, captor.getValue().prefix());

        // Assert stripped, container-relative key (ready for moveObject)
        assertEquals(1, result.size());
        assertTrue(result.contains(expectedReturnedKey));
        assertFalse("container prefix should be stripped from returned keys",
                result.contains(s3BucketKey));
    }
    @Test
    public void should_feedListedKeysDirectlyIntoMoveObject_when_useAccountAsBucketnameIsTrue() {
        ReflectionTestUtils.setField(s3Adapter, "useAccountAsBucketname", true);
        String prefix = "_draft/abc123/Biometrics/";
        String listedKey = prefix + "bio.cbeff";  // stripped key from listObjectsByPrefix

        // ... mock list to return container/prefix/bio.cbeff from S3, assert list returns listedKey ...

        ObjectStoreReference src = new ObjectStoreReference(ACCOUNT, CONTAINER, null, null, listedKey);
        ObjectStoreReference dst = new ObjectStoreReference(ACCOUNT, CONTAINER, null, null, "Biometrics/bio.cbeff");
        when(s3Client.copyObject(any(CopyObjectRequest.class)))
                .thenReturn(CopyObjectResponse.builder().build());

        s3Adapter.moveObject(src, dst, false);

        ArgumentCaptor<CopyObjectRequest> captor = ArgumentCaptor.forClass(CopyObjectRequest.class);
        verify(s3Client).copyObject(captor.capture());
        assertEquals(CONTAINER + "/" + listedKey, captor.getValue().sourceKey());  // moveObject re-adds container
    }
    @Test
    public void should_throwObjectStoreAdapterException_when_listObjectsByPrefixFails() {
        S3Exception s3Exception = (S3Exception) S3Exception.builder()
                .statusCode(500)
                .message("Internal Server Error")
                .build();

        when(s3Client.listObjectsV2Paginator(any(ListObjectsV2Request.class)))
                .thenThrow(s3Exception);

        ObjectStoreAdapterException thrown = assertThrows(
                ObjectStoreAdapterException.class,
                () -> s3Adapter.listObjectsByPrefix(ACCOUNT, CONTAINER, "_draft/")
        );

        assertEquals(OBJECT_STORE_NOT_ACCESSIBLE.getErrorCode(), thrown.getErrorCode());
        assertSame(s3Exception, thrown.getCause());
    }

    @Test
    public void should_returnKeysAcrossMultiplePages_when_paginatorYieldsMultiplePages() {
        String prefix = "_draft/abc123/Biometrics/";
        ListObjectsV2Response page1 = ListObjectsV2Response.builder()
                .contents(List.of(S3Object.builder().key(prefix + "bio1.cbeff").build())).build();
        ListObjectsV2Response page2 = ListObjectsV2Response.builder()
                .contents(List.of(S3Object.builder().key(prefix + "bio2.cbeff").build())).build();

        when(s3Client.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenReturn(paginator);
        doAnswer(invocation -> {
            Consumer<ListObjectsV2Response> consumer = invocation.getArgument(0);
            consumer.accept(page1);
            consumer.accept(page2);
            return null;
        }).when(paginator).forEach(any());

        List<String> result = s3Adapter.listObjectsByPrefix(ACCOUNT, CONTAINER, prefix);

        assertEquals("all keys from all pages should be collected", 2, result.size());
        assertFalse("result should not be empty", result.isEmpty());
    }
}
