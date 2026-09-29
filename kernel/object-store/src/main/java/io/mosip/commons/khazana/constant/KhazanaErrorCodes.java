package io.mosip.commons.khazana.constant;

/**
 * Error codes thrown by Khazana adapters and encryption helpers.
 * <p>
 * Each constant pairs a stable code ({@code COM-KZN-nnn}) with a message.
 * Adapters pass {@link #getErrorCode()} and {@link #getErrorMessage()} into
 * {@link io.mosip.commons.khazana.exception.ObjectStoreAdapterException} or
 * {@link io.mosip.commons.khazana.exception.FileNotFoundInDestinationException}.
 */
public enum KhazanaErrorCodes {

    /**
     * Container zip is missing under the POSIX account directory.
     */
    CONTAINER_NOT_PRESENT_IN_DESTINATION("COM-KZN-001", "Container not found."),

    /**
     * Packet encryption failed because the packet format is invalid.
     */
    ENCRYPTION_FAILURE("COM-KZN-002", "Packet Encryption Failed-Invalid Packet format"),

    /**
     * The object store could not be reached or the S3 call failed.
     */
    OBJECT_STORE_NOT_ACCESSIBLE("COM-KZN-003", "Object store not accessible");


    /**
     * Stable Khazana error code, for example {@code COM-KZN-003}.
     */
    private final String errorCode;

    /**
     * Human-readable message that accompanies {@link #errorCode}.
     */
    private final String errorMessage;

    /**
     * Binds a code and message for one enum constant.
     *
     * @param errorCode    stable Khazana error code
     * @param errorMessage message returned with that code
     */
    private KhazanaErrorCodes(final String errorCode, final String errorMessage) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    /**
     * Returns the stable error code.
     *
     * @return error code such as {@code COM-KZN-001}
     */
    public String getErrorCode() {
        return errorCode;
    }

    /**
     * Returns the message for this error.
     *
     * @return human-readable error message
     */
    public String getErrorMessage() {
        return errorMessage;
    }
}
