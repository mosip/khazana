package io.mosip.commons.khazana.exception;

import io.mosip.kernel.core.exception.BaseUncheckedException;

/**
 * Unchecked failure raised by {@link io.mosip.commons.khazana.impl.PosixAdapter}
 * when the container zip is not on disk.
 * <p>
 * {@code getObject} logs this and returns {@code null}. {@code getMetaData} logs it
 * and rethrows. {@code removeContainer} and {@code pack} catch it and return {@code false}.
 * The usual code is
 * {@link io.mosip.commons.khazana.constant.KhazanaErrorCodes#CONTAINER_NOT_PRESENT_IN_DESTINATION}.
 */
public class FileNotFoundInDestinationException extends BaseUncheckedException {

    /**
     * Creates an exception with an error code and message.
     *
     * @param errorCode stable Khazana error code
     * @param message   description of the missing container
     */
    public FileNotFoundInDestinationException(String errorCode, String message) {
        super(errorCode, message);
    }

    /**
     * Creates an exception with an error code, message, and cause.
     *
     * @param errorCode    stable Khazana error code
     * @param errorMessage description of the missing container
     * @param t            underlying cause
     */
    public FileNotFoundInDestinationException(String errorCode, String errorMessage, Throwable t) {
        super(errorCode, errorMessage, t);
    }
}
