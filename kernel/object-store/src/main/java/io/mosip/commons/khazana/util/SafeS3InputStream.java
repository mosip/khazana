package io.mosip.commons.khazana.util;

import com.amazonaws.services.s3.model.S3Object;
import io.mosip.commons.khazana.config.LoggerConfiguration;
import io.mosip.kernel.core.logger.spi.Logger;

import java.io.IOException;
import java.io.InputStream;

/**
 * Wrapper class for S3ObjectInputStream to ensure proper resource cleanup
 * and prevent "Not all bytes were read from the S3ObjectInputStream" warnings.
 *
 * Under high load, draining large objects ties up the HTTP connection and causes
 * latency spikes. When more than DRAIN_THRESHOLD_BYTES remain, the underlying
 * connection is aborted (removed from pool) rather than drained — trading one
 * pool slot for a significant reduction in network I/O and response time.
 */
public class SafeS3InputStream extends InputStream {

    private static final int DRAIN_BUFFER_SIZE = 8192;

    private static final long MAX_DRAIN_BYTES = 10 * 1024 * 1024; // safety cap: 10 MB

    /**
     * If more than this many bytes remain when close() is called, abort the HTTP
     * connection instead of draining. 256 KB keeps drain time bounded to ~1-2ms
     * even on a slow link; anything larger is cheaper to abort.
     */
    private static final long DRAIN_THRESHOLD_BYTES = 256 * 1024L; // 256 KB

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

    @Override
    public void close() throws IOException {
        if (fullyClosed) {
            return;
        }
        fullyClosed = true;

        try {
            if (isFullyRead()) {
                // Stream was fully consumed — no drain needed, return connection to pool cleanly
                LOGGER.debug("SafeS3InputStream - fully consumed ({}/{} bytes), closing normally", bytesRead, contentLength);
                delegateStream.close();
                return;
            }

            long remaining = contentLength > 0 ? contentLength - bytesRead : Long.MAX_VALUE;

            if (remaining > DRAIN_THRESHOLD_BYTES) {
                // Large remainder — abort the HTTP connection rather than draining over the network.
                // Under high load, draining N×256KB+ per thread spikes latency and holds connections
                // hostage. Aborting costs one pool slot but keeps response times stable.
                LOGGER.debug("SafeS3InputStream - aborting: {}B remaining exceeds {}B drain threshold",
                        remaining == Long.MAX_VALUE ? "unknown" : remaining, DRAIN_THRESHOLD_BYTES);
                try {
                    s3Object.getObjectContent().abort();
                } catch (Exception e) {
                    LOGGER.warn("Failed to abort S3ObjectInputStream", e);
                }
            } else {
                // Small remainder — drain to return the HTTP connection to the pool cleanly
                LOGGER.debug("SafeS3InputStream - draining small remainder (~{}B) to reuse connection", remaining);
                byte[] buffer = new byte[DRAIN_BUFFER_SIZE];
                long drained = 0;
                int readBytes;
                while ((readBytes = delegateStream.read(buffer)) != -1) {
                    drained += readBytes;
                    bytesRead += readBytes;
                    if (drained > MAX_DRAIN_BYTES) {
                        // Shouldn't happen since remaining <= DRAIN_THRESHOLD_BYTES, but guard anyway
                        LOGGER.warn("Drain exceeded safety limit — aborting");
                        s3Object.getObjectContent().abort();
                        return;
                    }
                }
                delegateStream.close();
            }
        } catch (IOException e) {
            LOGGER.warn("Exception during drain/close of delegate stream", e);
        } finally {
            try {
                s3Object.close();
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