package io.mosip.commons.khazana.exception;

import io.mosip.kernel.core.exception.BaseUncheckedException;

/**
 * {@code ObjectStoreAdapterException} is a runtime exception thrown when an unexpected
 * error occurs during object store adapter operations (e.g., Swift, MinIO, etc.).
 *
 * <p>This exception is used to wrap low-level storage errors such as connection failures,
 * read/write issues, or metadata operations, and translates them into meaningful application-level errors.</p>
 *
 * <p>Typical usage:</p>
 * <pre>
 * try {
 *     storedObject.uploadObject(data);
 * } catch (Exception e) {
 *     throw new ObjectStoreAdapterException("COM-KZN-003", "Failed to upload object to store.", e);
 * }
 * </pre>
 *
 * @author MOSIP Team
 * @since 1.0.0
 * @see io.mosip.kernel.core.exception.BaseUncheckedException
 */
public class ObjectStoreAdapterException extends BaseUncheckedException {

    /**
     * Constructs a new {@code ObjectStoreAdapterException} with the specified error code and message.
     *
     * @param errorCode the unique error code representing the failure scenario
     * @param message   the descriptive error message
     */
    public ObjectStoreAdapterException(String errorCode, String message) {
        super(errorCode, message);
    }

    /**
     * Constructs a new {@code ObjectStoreAdapterException} with the specified error code, message, and root cause.
     *
     * @param errorCode the unique error code
     * @param message   the descriptive error message
     * @param e         the underlying exception that caused this failure
     */
    public ObjectStoreAdapterException(String errorCode, String message, Throwable e) {
        super(errorCode, message, e);
    }
}
