package io.mosip.commons.khazana.util;

import com.amazonaws.services.s3.model.S3Object;
import io.mosip.commons.khazana.config.LoggerConfiguration;
import io.mosip.kernel.core.logger.spi.Logger;

import java.io.IOException;
import java.io.InputStream;

/**
 * Wrapper class for S3ObjectInputStream to ensure proper resource cleanup
 * and prevent "Not all bytes were read from the S3ObjectInputStream" warnings.
 */
public class SafeS3InputStream extends InputStream {

    private static final Logger LOGGER = LoggerConfiguration.logConfig(SafeS3InputStream.class);

    private static final int DRAIN_BUFFER_SIZE = 8192;

    /** Safety cap to avoid hanging on unexpectedly large or infinite streams during drain. */
    private static final long MAX_DRAIN_BYTES = 10 * 1024 * 1024; // 10 MB

    private final S3Object s3Object;
    private final InputStream delegateStream;
    private final long contentLength;
    private long bytesRead = 0;
    private boolean fullyClosed = false;

    public SafeS3InputStream(S3Object s3Object, long contentLength) {
        this.s3Object = s3Object;
        this.delegateStream = s3Object.getObjectContent();
        this.contentLength = contentLength;
    }

    @Override
    public int read() throws IOException {
        int byteValue = delegateStream.read();
        if (byteValue != -1) {
            bytesRead++;
        }
        return byteValue;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int n = delegateStream.read(b, off, len);
        if (n > 0) {
            bytesRead += n;
        }
        return n;
    }

    @Override
    public long skip(long n) throws IOException {
        long skipped = delegateStream.skip(n);
        bytesRead += skipped;
        return skipped;
    }

    @Override
    public int available() throws IOException {
        try {
            return delegateStream.available();
        } catch (IOException e) {
            return 0;
        }
    }

    @Override
    public void close() throws IOException {
        if (fullyClosed) {
            return;
        }
        fullyClosed = true;

        try {
            // Drain any unread bytes to allow the underlying HTTP connection to be reused.
            // Skip drain only when we know the stream has been fully consumed.
            if (!isFullyRead()) {
                byte[] buffer = new byte[DRAIN_BUFFER_SIZE];
                long drained = 0;
                int n;
                while ((n = delegateStream.read(buffer)) != -1) {
                    drained += n;
                    bytesRead += n;
                    if (drained > MAX_DRAIN_BYTES) {
                        LOGGER.warn("SafeS3InputStream - drain exceeded safety limit of {} bytes, aborting drain",
                                MAX_DRAIN_BYTES);
                        break;
                    }
                }
                LOGGER.debug("SafeS3InputStream - drained {} bytes on close (contentLength={})", drained, contentLength);
            }

            delegateStream.close();
        } catch (IOException e) {
            LOGGER.warn("SafeS3InputStream - exception during drain/close of delegate stream", e);
        } finally {
            try {
                s3Object.close();
            } catch (IOException e) {
                LOGGER.error("SafeS3InputStream - failed to close S3Object", e);
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

    /**
     * Returns true only when contentLength is known and all bytes have been read.
     * When contentLength is -1 (unknown), conservatively returns false so drain is always attempted.
     */
    public boolean isFullyRead() {
        return contentLength > 0 && bytesRead >= contentLength;
    }

    public boolean isClosed() {
        return fullyClosed;
    }
}
