package io.mosip.commons.khazana.dto;

import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * {@code ObjectDto} represents metadata about a stored object in the Khazana (object store) system.
 * <p>
 * It includes key details such as the source system, associated process, object name, and
 * the last modification timestamp. This DTO is commonly used for listing or retrieving
 * information about objects stored in backend containers.
 * </p>
 *
 * <p>Example usage:</p>
 * <pre>
 * ObjectDto obj = new ObjectDto("mosip-reg-client", "biometrics", "fingerprint-12345", new Date());
 * </pre>
 *
 * <p>Example JSON:</p>
 * <pre>
 * {
 *   "source": "mosip-reg-client",
 *   "process": "biometrics",
 *   "objectName": "fingerprint-12345",
 *   "lastModified": "2025-07-04T14:00:00Z"
 * }
 * </pre>
 * 
 * @author MOSIP Team
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ObjectDto {

    /**
     * Identifies the originating module or service that stored the object.
     * For example: {@code "mosip-reg-client"}, {@code "auth-service"}.
     */
    private String source;

    /**
     * Describes the process or workflow the object is associated with.
     * For example: {@code "biometrics"}, {@code "onboarding"}, {@code "encryption"}.
     */
    private String process;

    /**
     * The name or identifier of the stored object.
     * Typically used as the key when fetching or deleting the object.
     */
    private String objectName;

    /**
     * The timestamp of the last modification made to the object in the object store.
     */
    private Date lastModified;
}
