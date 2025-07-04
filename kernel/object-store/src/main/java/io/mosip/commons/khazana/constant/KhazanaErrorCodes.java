package io.mosip.commons.khazana.constant;

/**
 * {@code KhazanaErrorCodes} defines a set of standardized error codes and messages
 * used across the Khazana module within the MOSIP platform.
 * <p>
 * These error codes represent specific failure scenarios such as missing containers,
 * encryption issues, and unavailability of the object store.
 * </p>
 * 
 * <p>Each enum constant has:
 * <ul>
 *   <li>An error code (e.g., {@code COM-KZN-001})</li>
 *   <li>A human-readable error message</li>
 * </ul>
 * </p>
 *
 * <p>Example usage:</p>
 * <pre>
 *     throw new RuntimeException(KhazanaErrorCodes.OBJECT_STORE_NOT_ACCESSIBLE.getErrorMessage());
 * </pre>
 * 
 * @author MOSIP Team
 */
public enum KhazanaErrorCodes {

    /**
     * Error when the expected container is not found in the destination storage.
     * <p>Code: {@code COM-KZN-001}</p>
     */
    CONTAINER_NOT_PRESENT_IN_DESTINATION("COM-KZN-001", "Container not found."),

    /**
     * Error when encryption fails due to an invalid packet format or structure.
     * <p>Code: {@code COM-KZN-002}</p>
     */
    ENCRYPTION_FAILURE("COM-KZN-002", "Packet Encryption Failed - Invalid Packet format"),

    /**
     * Error when the object store is inaccessible due to service unavailability, misconfiguration, or network issues.
     * <p>Code: {@code COM-KZN-003}</p>
     */
    OBJECT_STORE_NOT_ACCESSIBLE("COM-KZN-003", "Object store not accessible");

    /**
     * The unique error code used for programmatic identification.
     */
    private final String errorCode;

    /**
     * The descriptive message associated with the error.
     */
    private final String errorMessage;

    /**
     * Constructor to initialize the error code and corresponding message.
     *
     * @param errorCode    the unique error identifier
     * @param errorMessage the human-readable error description
     */
    private KhazanaErrorCodes(final String errorCode, final String errorMessage) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    /**
     * Retrieves the error code associated with the enum constant.
     *
     * @return the error code (e.g., {@code COM-KZN-001})
     */
    public String getErrorCode() {
        return errorCode;
    }

    /**
     * Retrieves the descriptive error message.
     *
     * @return the error message string
     */
    public String getErrorMessage() {
        return errorMessage;
    }
}
