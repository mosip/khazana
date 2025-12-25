package io.mosip.commons.khazana.impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;

import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.PutObjectResult;

import io.mosip.commons.khazana.exception.ObjectStoreAdapterException;

@ExtendWith(MockitoExtension.class)
class S3AdapterTest {

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
    }

    @Test
    void getObject_shouldReturnStream_whenS3ReturnsObject() throws Exception {
        String account = "acct";
        String container = "cont";
        String source = "src";
        String process = "proc";
        String objectName = "obj";

        byte[] payload = "s3-payload".getBytes(StandardCharsets.UTF_8);
        InputStream payloadStream = new ByteArrayInputStream(payload);

        when(amazonS3.getObject(eq(container), anyString())).thenReturn(s3Object);
        // create a real S3ObjectInputStream wrapping the ByteArrayInputStream
        when(s3Object.getObjectContent()).thenReturn(new S3ObjectInputStream(payloadStream, null));

        InputStream result = adapter.getObject(account, container, source, process, objectName);
        byte[] read = result.readAllBytes();

        assertArrayEquals(payload, read);
        verify(s3Object).close();
    }

    @Test
    void getObject_shouldThrow_whenUnderlyingThrows() throws Exception {
        String account = "acct";
        String container = "cont";
        String source = "src";
        String process = "proc";
        String objectName = "obj";

        when(amazonS3.getObject(eq(container), anyString())).thenThrow(new RuntimeException("boom"));

        assertThrows(ObjectStoreAdapterException.class, () -> adapter.getObject(account, container, source, process, objectName));
    }

    @Test
    void putObject_shouldCreateBucketAndPut_whenBucketMissing() throws Exception {
        String account = "acct";
        String container = "cont";
        String source = "src";
        String process = "proc";
        String objectName = "obj";

        byte[] payload = "put-payload".getBytes(StandardCharsets.UTF_8);
        InputStream data = new ByteArrayInputStream(payload);

        when(amazonS3.doesBucketExistV2(container)).thenReturn(false);
        // createBucket and putObject are non-void; stub them to return appropriate objects
        when(amazonS3.createBucket(container)).thenReturn(new Bucket(container));
        when(amazonS3.putObject(eq(container), anyString(), any(InputStream.class), any())).thenReturn(new PutObjectResult());

        boolean ok = adapter.putObject(account, container, source, process, objectName, data);
        assertTrue(ok);
        verify(amazonS3).createBucket(container);
        verify(amazonS3).putObject(eq(container), anyString(), any(InputStream.class), any());
    }

    @Test
    void exists_shouldDelegateToDoesObjectExist() throws Exception {
        // mock doesObjectExist used by adapter
        when(amazonS3.doesObjectExist(anyString(), anyString())).thenReturn(true);

        Field useAccount = S3Adapter.class.getDeclaredField("useAccountAsBucketname");
        useAccount.setAccessible(true);
        useAccount.set(adapter, false);

        boolean ok = adapter.exists("a", "b", "s", "p", "o");
        assertTrue(ok);
    }
}
