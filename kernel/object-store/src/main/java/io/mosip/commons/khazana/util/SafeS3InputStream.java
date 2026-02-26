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

    private static final int DRAIN_BUFFER_SIZE = 8192;

    private static final long MAX_DRAIN_BYTES = 10 * 1024 * 1024; // safety cap: 10 MB

    private final S3Object s3Object;
    private final InputStream delegateStream;
    private final long contentLength;
    private long bytesRead = 0;
    private boolean fullyClosed = false;

    private final Logger LOGGER = LoggerConfiguration.logConfig(SafeS3InputStream.class);

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
    public int read(byte[] b) throws IOException {
        int bytesReadFromStream = delegateStream.read(b);
        if (bytesReadFromStream > 0) {
            bytesRead += bytesReadFromStream;
        }
        return bytesReadFromStream;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int bytesReadFromStream = delegateStream.read(b, off, len);
        if (bytesReadFromStream > 0) {
            bytesRead += bytesReadFromStream;
        }
        return bytesReadFromStream;
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

    /**
     * Drain the remaining bytes from the stream
     */
    private void drainStream() {
        if (contentLength > 0 && bytesRead < contentLength) {
            byte[] buffer = new byte[DRAIN_BUFFER_SIZE];
            int bytesReadFromStream;
            long remainingBytes = contentLength - bytesRead;
            long drainedBytes = 0;

            try {
                while (drainedBytes < remainingBytes && (bytesReadFromStream = delegateStream.read(buffer)) != -1) {
                    drainedBytes += bytesReadFromStream;
                }
            } catch (IOException e) {
                // Silently ignore during drain
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (fullyClosed) {
            return;
        }
        fullyClosed = true;

        try {
            // Always attempt to drain — even if contentLength == -1 or bytesRead >= contentLength
            // This is the most reliable way to suppress the warning
            if (!isFullyRead() || contentLength <= 0) {  // also drain if length unknown
                LOGGER.debug("SafeS3InputStream - draining to prevent AWS warning. Known length: {}, bytesRead: {}",
                        contentLength, bytesRead);

                byte[] buffer = new byte[DRAIN_BUFFER_SIZE];
                long drained = 0;
                int readBytes;

                while ((readBytes = delegateStream.read(buffer)) != -1) {
                    drained += readBytes;
                    bytesRead += readBytes;

                    // Safety: prevent infinite loop or huge objects from hanging
                    if (drained > MAX_DRAIN_BYTES) {
                        LOGGER.warn( "Drain exceeded safety limit of {} bytes - aborting drain", MAX_DRAIN_BYTES);
                        break;
                    }
                }
                LOGGER.debug("Drained {} additional bytes. Total read now: {}", drained, bytesRead);
            } else {
                LOGGER.debug("Stream fully consumed ({}/{} bytes) - no drain needed", bytesRead, contentLength);
            }

            delegateStream.close();
        } catch (IOException e) {
            LOGGER.warn("Exception during drain/close of delegate stream", e);
        } finally {
            try {
                s3Object.close();
                LOGGER.debug( "S3Object closed");
            } catch (IOException e) {
                LOGGER.error("Failed to close S3Object", e);
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
        return contentLength > 0 && bytesRead >= contentLength;
    }

    public boolean isClosed() {
        return fullyClosed;
    }
}