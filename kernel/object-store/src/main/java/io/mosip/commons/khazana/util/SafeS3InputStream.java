package io.mosip.commons.khazana.util;

import com.amazonaws.services.s3.model.S3Object;
import io.mosip.commons.khazana.config.LoggerConfiguration;
import io.mosip.kernel.core.logger.spi.Logger;

import java.io.IOException;
import java.io.InputStream;

import static io.mosip.commons.khazana.config.LoggerConfiguration.REGISTRATIONID;
import static io.mosip.commons.khazana.config.LoggerConfiguration.SESSIONID;

/**
 * Wrapper class for S3ObjectInputStream to ensure proper resource cleanup
 * and prevent "Not all bytes were read from the S3ObjectInputStream" warnings.

 * This class:
 * - Tracks if the stream has been fully read
 * - Ensures the S3Object is properly closed
 * - Prevents connection leaks
 * - Provides content-length information
 */
public class SafeS3InputStream extends InputStream {

    private static final Logger LOGGER = LoggerConfiguration.logConfig(SafeS3InputStream.class);

    private final S3Object s3Object;
    private final InputStream delegateStream;
    private final long contentLength;
    private long bytesRead = 0;
    private boolean closed = false;

    public SafeS3InputStream(S3Object s3Object, long contentLength) {
        this.s3Object = s3Object;
        this.delegateStream = s3Object.getObjectContent();
        this.contentLength = contentLength;
        LOGGER.info(SESSIONID, REGISTRATIONID, "SafeS3InputStream created with contentLength: " + contentLength);
    }

    @Override
    public int read() throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        int byte_value = delegateStream.read();
        if (byte_value != -1) {
            bytesRead++;
        }
        return byte_value;
    }

    @Override
    public int read(byte[] b) throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        int bytesReadFromStream = delegateStream.read(b);
        if (bytesReadFromStream > 0) {
            bytesRead += bytesReadFromStream;
        }
        return bytesReadFromStream;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        int bytesReadFromStream = delegateStream.read(b, off, len);
        if (bytesReadFromStream > 0) {
            bytesRead += bytesReadFromStream;
        }
        return bytesReadFromStream;
    }

    @Override
    public long skip(long n) throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        long skipped = delegateStream.skip(n);
        bytesRead += skipped;
        return skipped;
    }

    @Override
    public int available() throws IOException {
        if (closed) {
            return 0;
        }
        return delegateStream.available();
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            try {
                // Ensure full consumption or explicit drain
                if (contentLength > 0 && bytesRead < contentLength) {
                    LOGGER.info(SESSIONID, REGISTRATIONID, "SafeS3InputStream - draining remaining bytes. Expected: " +
                            contentLength + ", Read: " + bytesRead);
                    byte[] buffer = new byte[8192];
                    int bytesReadFromStream;
                    while ((bytesReadFromStream = delegateStream.read(buffer)) != -1) {
                        bytesRead += bytesReadFromStream;
                    }
                }
                delegateStream.close();
                LOGGER.info(SESSIONID, REGISTRATIONID, "SafeS3InputStream - delegate stream closed. Total bytes read: " + bytesRead);
            } finally {
                try {
                    s3Object.close();
                    LOGGER.info(SESSIONID, REGISTRATIONID, "SafeS3InputStream - S3Object closed successfully");
                } catch (IOException e) {
                    LOGGER.error(SESSIONID, REGISTRATIONID, "Error closing S3Object", e);
                }
            }
        }
    }

    @Override
    public boolean markSupported() {
        return delegateStream.markSupported();
    }

    @Override
    public synchronized void mark(int readlimit) {
        delegateStream.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
        delegateStream.reset();
    }

    public long getBytesRead() {
        return bytesRead;
    }

    public long getContentLength() {
        return contentLength;
    }

    public boolean isFullyRead() {
        return contentLength == -1 || bytesRead >= contentLength;
    }
}