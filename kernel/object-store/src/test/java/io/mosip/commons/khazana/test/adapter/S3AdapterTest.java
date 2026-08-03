package io.mosip.commons.khazana.test.adapter;

import io.mosip.commons.khazana.exception.ObjectStoreAdapterException;
import io.mosip.commons.khazana.impl.S3Adapter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
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

    @Before
    public void setup() {
        // Inject the mock S3Client directly — getConnection() returns it immediately
        // when connection != null, bypassing the real builder logic.
        ReflectionTestUtils.setField(s3Adapter, "connection", s3Client);
        ReflectionTestUtils.setField(s3Adapter, "useAccountAsBucketname", false);
        ReflectionTestUtils.setField(s3Adapter, "bucketNamePrefix", "");
    }

    // ── copyAndReplaceObject ──────────────────────────────────────────────────

    @Test
    public void testCopyAndReplaceObject_Success() {
        when(s3Client.copyObject(any(CopyObjectRequest.class)))
                .thenReturn(CopyObjectResponse.builder().build());

        boolean result = s3Adapter.copyAndReplaceObject(ACCOUNT, CONTAINER, SRC_KEY, DEST_KEY);

        assertTrue("copyAndReplaceObject should return true on success", result);
    }

    @Test
    public void testCopyAndReplaceObject_S3Exception() {
        S3Exception s3Ex = (S3Exception) S3Exception.builder()
                .statusCode(500)
                .message("Internal Server Error")
                .build();
        when(s3Client.copyObject(any(CopyObjectRequest.class))).thenThrow(s3Ex);

        assertThrows(
                "S3Exception should be wrapped as ObjectStoreAdapterException",
                ObjectStoreAdapterException.class,
                () -> s3Adapter.copyAndReplaceObject(ACCOUNT, CONTAINER, SRC_KEY, DEST_KEY)
        );
    }

    @Test
    public void testCopyAndReplaceObject_UnexpectedException() {
        when(s3Client.copyObject(any(CopyObjectRequest.class)))
                .thenThrow(new RuntimeException("connection reset"));

        assertThrows(
                "Unexpected exception should be wrapped as ObjectStoreAdapterException",
                ObjectStoreAdapterException.class,
                () -> s3Adapter.copyAndReplaceObject(ACCOUNT, CONTAINER, SRC_KEY, DEST_KEY)
        );
    }

    @Test
    public void testCopyAndReplaceObject_UseAccountAsBucketname() {
        ReflectionTestUtils.setField(s3Adapter, "useAccountAsBucketname", true);
        when(s3Client.copyObject(any(CopyObjectRequest.class)))
                .thenReturn(CopyObjectResponse.builder().build());

        boolean result = s3Adapter.copyAndReplaceObject(ACCOUNT, CONTAINER, SRC_KEY, DEST_KEY);

        assertTrue("Should use account as bucket name and still return true", result);
    }
}
