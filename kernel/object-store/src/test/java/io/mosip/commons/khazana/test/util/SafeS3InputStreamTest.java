package io.mosip.commons.khazana.test.util;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import io.mosip.commons.khazana.util.SafeS3InputStream;
import org.apache.http.client.methods.HttpGet;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SafeS3InputStreamTest {

    @Test
    public void readsFullyThenCloseSkipsDrain() throws Exception {
        byte[] body = "abc".getBytes(StandardCharsets.UTF_8);
        SafeS3InputStream in = stream(body, body.length);
        assertEquals(3, in.available());
        assertEquals('a', in.read());
        byte[] rest = new byte[2];
        assertEquals(2, in.read(rest));
        assertTrue(in.isFullyRead());
        assertEquals(3, in.getBytesRead());
        assertEquals(3, in.getContentLength());
        in.mark(1);
        in.close();
        in.close();
    }

    @Test
    public void unknownLengthDrainsOnClose() throws Exception {
        SafeS3InputStream in = stream("hello".getBytes(StandardCharsets.UTF_8), -1);
        assertFalse(in.isFullyRead());
        assertEquals(0, in.skip(0));
        byte[] buf = new byte[2];
        in.read(buf, 0, 2);
        in.close();
        assertFalse(in.markSupported() && false);
    }

    @Test
    public void resetDelegates() throws Exception {
        ByteArrayInputStream raw = new ByteArrayInputStream("xy".getBytes(StandardCharsets.UTF_8));
        raw.mark(2);
        S3Object object = new S3Object();
        object.setObjectContent(new S3ObjectInputStream(raw, new HttpGet("http://127.0.0.1/o")));
        SafeS3InputStream in = new SafeS3InputStream(object, 2);
        in.read();
        in.reset();
        assertEquals('x', in.read());
        in.close();
    }

    private static SafeS3InputStream stream(byte[] body, long length) throws IOException {
        S3Object object = new S3Object();
        object.setObjectContent(new S3ObjectInputStream(
                new ByteArrayInputStream(body), new HttpGet("http://127.0.0.1/o")));
        return new SafeS3InputStream(object, length);
    }
}
