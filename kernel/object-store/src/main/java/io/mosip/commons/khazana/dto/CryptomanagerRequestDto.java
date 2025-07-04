package io.mosip.commons.khazana.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.LocalDateTime;

/**
 * {@code CryptomanagerRequestDto} represents the request structure used to
 * perform cryptographic operations (encryption/decryption) via the CryptoManager service.
 *
 * <p>This DTO carries metadata and cryptographic payload such as the input data,
 * salt, and AAD (Additional Authenticated Data) all encoded in Base64 format, along with
 * contextual information like application ID and timestamp.</p>
 *
 * <p>Validation constraints ensure the required fields are not null or blank,
 * supporting robust input validation in API layers.</p>
 *
 * <p>Example JSON:
 * <pre>
 * {
 *   "applicationId": "mosip-vid-service",
 *   "referenceId": "abc123",
 *   "timeStamp": "2025-07-04T13:45:30.000Z",
 *   "data": "BASE64_ENCODED_DATA",
 *   "salt": "BASE64_ENCODED_SALT",
 *   "aad": "BASE64_ENCODED_AAD",
 *   "prependThumbprint": true
 * }
 * </pre>
 * </p>
 *
 * @author MOSIP Team
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CryptomanagerRequestDto {

    /**
     * Identifier for the application requesting the cryptographic operation.
     * Used for access control and audit trail purposes.
     */
    @NotBlank(message = "should not be null or empty")
    private String applicationId;

    /**
     * Optional reference ID associated with the request, such as a transaction ID
     * or entity identifier for traceability.
     */
    private String referenceId;

    /**
     * The timestamp when the request is made. Must follow ISO 8601 format with milliseconds and 'Z' suffix (UTC).
     * <p>Example: {@code 2025-07-04T13:45:30.123Z}</p>
     */
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    @NotNull
    private LocalDateTime timeStamp;

    /**
     * The actual data to be encrypted or decrypted, encoded in Base64.
     */
    @NotBlank(message = "should not be null or empty")
    private String data;

    /**
     * The cryptographic salt used in encryption/decryption, encoded in Base64.
     */
    @NotBlank(message = "should not be null or empty")
    private String salt;

    /**
     * Additional Authenticated Data (AAD) for GCM encryption, encoded in Base64.
     * AAD is authenticated but not encrypted.
     */
    @NotBlank(message = "should not be null or empty")
    private String aad;

    /**
     * Flag indicating whether to prepend the thumbprint to the encrypted payload.
     * This is typically used for ensuring backward compatibility or auditability.
     */
    private Boolean prependThumbprint;
}
