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
 * <p>
 * {@link io.mosip.commons.khazana.impl.S3Adapter#getObject} returns this stream.
 * Callers must close it. {@link #close()} drains unread bytes (up to
 * {@link #MAX_DRAIN_BYTES}), closes the delegate, and closes the {@link S3Object}
 * so the HTTP connection returns to the pool.
 * <p>
 * This class:
 * - Tracks if the stream has been fully read
 * - Ensures the S3Object is properly closed
 * - Prevents connection leaks
 * - Provides content-length information
 */
public class SafeS3InputStream extends InputStream {

    /**
     * Kernel logger for create, drain, and close messages.
     */
    private static final Logger LOGGER = LoggerConfiguration.logConfig(SafeS3InputStream.class);

    /**
     * S3 response that owns the HTTP connection. Closed by {@link #close()}.
     */
    private final S3Object s3Object;

    /**
     * Object-content stream delegated to for every read.
     */
    private final InputStream delegateStream;

    /**
     * Content length from object metadata, or {@code -1} when unknown.
     */
    private final long contentLength;

    /**
     * Number of bytes read or drained so far.
     */
    private long bytesRead = 0;

    /**
     * Whether {@link #close()} has already run. Later reads fail; a second close is a no-op.
     */
    private boolean closed = false;

    /**
     * Size of the buffer used to drain unread bytes on close.
     */
    private static final int DRAIN_BUFFER_SIZE = 8192;

    /**
     * Maximum number of extra bytes {@link #close()} will drain before it stops.
     */
    private static final long MAX_DRAIN_BYTES = 10 * 1024 * 1024; // safety cap: 10 MB

    /**
     * Wraps an S3 object and records its content length.
     *
     * @param s3Object      open S3 object whose content stream is delegated
     * @param contentLength metadata content length, or {@code -1} when unknown
     */
    public SafeS3InputStream(S3Object s3Object, long contentLength) {
        this.s3Object = s3Object;
        this.delegateStream = s3Object.getObjectContent();
        this.contentLength = contentLength;
        LOGGER.info(SESSIONID, REGISTRATIONID, "SafeS3InputStream created with contentLength: " + contentLength);
    }

    /**
     * Reads the next byte.
     *
     * @return the byte as an unsigned value {@code 0-255}, or {@code -1} at end of stream
     * @throws IOException when this stream is already closed or the delegate read fails
     */
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

    /**
     * Reads up to {@code b.length} bytes into {@code b}.
     *
     * @param b buffer to fill
     * @return number of bytes read, or {@code -1} at end of stream
     * @throws IOException when this stream is already closed or the delegate read fails
     */
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

    /**
     * Reads up to {@code len} bytes into {@code b} starting at {@code off}.
     *
     * @param b   buffer to fill
     * @param off start offset in {@code b}
     * @param len maximum number of bytes to read
     * @return number of bytes read, or {@code -1} at end of stream
     * @throws IOException when this stream is already closed or the delegate read fails
     */
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

    /**
     * Skips up to {@code n} bytes.
     *
     * @param n number of bytes to skip
     * @return number of bytes actually skipped
     * @throws IOException when this stream is already closed or the delegate skip fails
     */
    @Override
    public long skip(long n) throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        long skipped = delegateStream.skip(n);
        bytesRead += skipped;
        return skipped;
    }

    /**
     * Returns an estimate of bytes that can be read without blocking.
     *
     * @return bytes available from the delegate, or {@code 0} when this stream is closed
     * @throws IOException when the delegate cannot report availability
     */
    @Override
    public int available() throws IOException {
        if (closed) {
            return 0;
        }
        return delegateStream.available();
    }

    /**
     * Drains unread content when needed, then closes the delegate and the S3 object.
     * <p>
     * A second call returns immediately. Drain stops after {@link #MAX_DRAIN_BYTES}.
     * Failures while draining or closing are logged and do not propagate, except that
     * the method still declares {@link IOException} because it overrides {@link InputStream#close()}.
     *
     * @throws IOException declared by {@link InputStream#close()}; close failures are logged and swallowed
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;

        try {
            // Always attempt to drain — even if contentLength == -1 or bytesRead >= contentLength
            // This is the most reliable way to suppress the warning
            if (!isFullyRead() || contentLength <= 0) {  // also drain if length unknown
                LOGGER.debug(SESSIONID, REGISTRATIONID,
                        "SafeS3InputStream - draining to prevent AWS warning. Known length: {}, bytesRead: {}",
                        contentLength, bytesRead);

                byte[] buffer = new byte[DRAIN_BUFFER_SIZE];
                long drained = 0;
                int readBytes;


                while ((readBytes = delegateStream.read(buffer)) != -1) {
                    drained += readBytes;
                    bytesRead += readBytes;

                    // Safety: prevent infinite loop or huge objects from hanging
                    if (drained > MAX_DRAIN_BYTES) {
                        LOGGER.warn(SESSIONID, REGISTRATIONID,
                                "Drain exceeded safety limit of {} bytes - aborting drain", MAX_DRAIN_BYTES);
                        break;
                    }
                }
                LOGGER.debug(SESSIONID, REGISTRATIONID,
                        "Drained {} additional bytes. Total read now: {}", drained, bytesRead);
            } else {
                LOGGER.debug(SESSIONID, REGISTRATIONID,
                        "Stream fully consumed ({}/{} bytes) - no drain needed", bytesRead, contentLength);
            }

            delegateStream.close();
        } catch (IOException e) {
            LOGGER.warn(SESSIONID, REGISTRATIONID,
                    "Exception during drain/close of delegate stream", e);
        } finally {
            try {
                s3Object.close();
                LOGGER.debug(SESSIONID, REGISTRATIONID, "S3Object closed");
            } catch (IOException e) {
                LOGGER.error(SESSIONID, REGISTRATIONID, "Failed to close S3Object", e);
            }
        }
    }

    /**
     * Reports whether the delegate supports {@link #mark(int)} and {@link #reset()}.
     *
     * @return {@code true} when the delegate supports mark and reset
     */
    @Override
    public boolean markSupported() {
        return delegateStream.markSupported();
    }

    /**
     * Marks the current position on the delegate.
     *
     * @param readlimit maximum number of bytes that can be read before the mark is invalidated
     */
    @Override
    public synchronized void mark(int readlimit) {
        delegateStream.mark(readlimit);
    }

    /**
     * Repositions this stream to the last mark on the delegate.
     *
     * @throws IOException when the delegate cannot reset
     */
    @Override
    public synchronized void reset() throws IOException {
        delegateStream.reset();
    }

    /**
     * Returns how many bytes have been read or drained.
     *
     * @return bytes consumed so far
     */
    public long getBytesRead() {
        return bytesRead;
    }

    /**
     * Returns the content length captured at construction.
     *
     * @return content length, or {@code -1} when it was unknown
     */
    public long getContentLength() {
        return contentLength;
    }

    /**
     * Reports whether every byte of a known content length has been read.
     *
     * @return {@code true} when {@link #contentLength} is positive and {@link #bytesRead} has reached it
     */
    public boolean isFullyRead() {
        return contentLength > 0 && bytesRead >= contentLength;
    }
}
