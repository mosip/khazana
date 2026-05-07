package io.mosip.commons.khazana.util;

import io.mosip.commons.khazana.config.LoggerConfiguration;
import io.mosip.kernel.core.logger.spi.Logger;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.io.InputStream;

/**
 * Wraps SDK v2's ResponseInputStream&lt;GetObjectResponse&gt; to ensure proper resource
 * cleanup and prevent "connection not fully drained" issues in the Apache HTTP client pool.
 *
 * <p>Strategy on close():
 * <ul>
 *   <li>Fully consumed: close normally — HTTP connection returns to pool cleanly.</li>
 *   <li>Small remainder (&le; {@code DRAIN_THRESHOLD_BYTES}): drain to reuse the connection.</li>
 *   <li>Large remainder: abort() — drops the TCP connection rather than pulling megabytes
 *       of data over the wire just to discard them. Under 400 RPS this prevents latency spikes
 *       caused by N×large drains running concurrently.</li>
 * </ul>
 */
public class SafeS3InputStream extends InputStream {

    private static final Logger LOGGER = LoggerConfiguration.logConfig(SafeS3InputStream.class);

    /** Drain up to this many bytes to reuse the HTTP connection. Above this, abort instead. */
    private static final long DRAIN_THRESHOLD_BYTES = 256 * 1024; // 256 KB

    private static final int DRAIN_BUFFER_SIZE = 8 * 1024; // 8 KB

    /** Absolute safety cap so a bug can't cause an infinite drain loop. */
    private static final long MAX_DRAIN_BYTES = 10 * 1024 * 1024; // 10 MB

    /**
     * SDK v2: ResponseInputStream&lt;GetObjectResponse&gt; extends AbortableInputStream,
     * which is itself an InputStream. It exposes abort() to drop the HTTP connection
     * without draining, and response() for the S3 response metadata.
     */
    private final ResponseInputStream<GetObjectResponse> responseStream;
    private final long contentLength;
    private long bytesRead = 0;
    private boolean fullyClosed = false;

    public SafeS3InputStream(ResponseInputStream<GetObjectResponse> responseStream) {
        this.responseStream = responseStream;
        // contentLength() returns Long (nullable) — treat null as unknown (-1)
        Long length = responseStream.response().contentLength();
        this.contentLength = (length != null) ? length : -1L;
    }

    @Override
    public int read() throws IOException {
        int b = responseStream.read();
        if (b != -1) bytesRead++;
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int n = responseStream.read(b, off, len);
        if (n > 0) bytesRead += n;
        return n;
    }

    @Override
    public long skip(long n) throws IOException {
        long skipped = responseStream.skip(n);
        bytesRead += skipped;
        return skipped;
    }

    @Override
    public int available() throws IOException {
        try {
            return responseStream.available();
        } catch (IOException e) {
            return 0;
        }
    }

    @Override
    public boolean markSupported() {
        return responseStream.markSupported();
    }

    @Override
    public synchronized void mark(int readlimit) {
        responseStream.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
        responseStream.reset();
    }

    @Override
    public void close() throws IOException {
        if (fullyClosed) return;
        fullyClosed = true;

        try {
            if (isFullyRead()) {
                // Stream fully consumed — return connection to pool cleanly
                LOGGER.debug("SafeS3InputStream - fully consumed ({}/{} bytes), closing normally",
                        bytesRead, contentLength);
                responseStream.close();
                return;
            }

            long remaining = contentLength >= 0 ? contentLength - bytesRead : Long.MAX_VALUE;

            if (remaining > DRAIN_THRESHOLD_BYTES) {
                // Large remainder — abort the HTTP connection rather than draining over the network.
                // Under high load draining N×256KB+ per thread spikes latency and holds connections
                // hostage. Aborting costs one pool slot but keeps response times stable.
                LOGGER.debug("SafeS3InputStream - aborting: {}B remaining exceeds {}B drain threshold",
                        remaining == Long.MAX_VALUE ? "unknown" : remaining, DRAIN_THRESHOLD_BYTES);
                responseStream.abort();
            } else {
                // Small remainder — drain to return the HTTP connection to the pool cleanly
                LOGGER.debug("SafeS3InputStream - draining ~{}B remainder to reuse connection", remaining);
                byte[] buffer = new byte[DRAIN_BUFFER_SIZE];
                long drained = 0;
                int n;
                while ((n = responseStream.read(buffer)) != -1) {
                    drained += n;
                    bytesRead += n;
                    if (drained > MAX_DRAIN_BYTES) {
                        // Shouldn't happen since remaining <= DRAIN_THRESHOLD_BYTES, but guard anyway
                        LOGGER.warn("SafeS3InputStream - drain exceeded safety limit — aborting");
                        responseStream.abort();
                        return;
                    }
                }
                responseStream.close();
            }
        } catch (IOException e) {
            LOGGER.warn("SafeS3InputStream - exception during close, attempting abort", e);
            try {
                responseStream.abort();
            } catch (Exception ignored) {}
        }
    }

    public long getBytesRead() {
        return bytesRead;
    }

    public long getContentLength() {
        return contentLength;
    }

    /**
     * Returns true only when contentLength is known and all bytes have been read.
     * When contentLength is -1 (unknown), conservatively returns false so close()
     * always attempts a drain or abort.
     */
    public boolean isFullyRead() {
        return contentLength >= 0 && bytesRead >= contentLength;
    }

    public boolean isClosed() {
        return fullyClosed;
    }
}
