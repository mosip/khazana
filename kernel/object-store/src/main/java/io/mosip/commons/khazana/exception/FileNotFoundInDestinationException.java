package io.mosip.commons.khazana.exception;

import io.mosip.kernel.core.exception.BaseUncheckedException;

/**
 * {@code FileNotFoundInDestinationException} is thrown when a requested object (file)
 * is not found in the expected destination container within the object store.
 *
 * <p>
 * This exception is part of the Khazana module and extends {@link BaseUncheckedException},
 * indicating it is a runtime (unchecked) exception typically used in service and utility layers.
 * </p>
 *
 * <p>Use Case Example:</p>
 * <pre>
 * if (!container.getObject(fileName).exists()) {
 *     throw new FileNotFoundInDestinationException("COM-KZN-001", "Object '" + fileName + "' not found.");
 * }
 * </pre>
 *
 * @author MOSIP Team
 * @since 1.0.0
 * @see io.mosip.kernel.core.exception.BaseUncheckedException
 */
public class FileNotFoundInDestinationException extends BaseUncheckedException {

    /**
     * Constructs a new {@code FileNotFoundInDestinationException} with the specified error code and message.
     *
     * @param errorCode the unique error code (e.g., {@code COM-KZN-001})
     * @param message   the detailed error message
     */
    public FileNotFoundInDestinationException(String errorCode, String message) {
        super(errorCode, message);
    }

    /**
     * Constructs a new {@code FileNotFoundInDestinationException} with the specified error code,
     * message, and root cause.
     *
     * @param errorCode    the unique error code
     * @param errorMessage the error message
     * @param t            the root cause (exception)
     */
    public FileNotFoundInDestinationException(String errorCode, String errorMessage, Throwable t) {
        super(errorCode, errorMessage, t);
    }
}