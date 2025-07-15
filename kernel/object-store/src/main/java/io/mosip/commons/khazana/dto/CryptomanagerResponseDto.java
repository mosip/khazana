package io.mosip.commons.khazana.dto;

import io.mosip.kernel.core.http.ResponseWrapper;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * {@code CryptomanagerResponseDto} represents the standardized HTTP response wrapper
 * for cryptographic operations such as decryption using the CryptoManager module.
 *
 * <p>
 * This class extends {@link ResponseWrapper} and wraps a {@link DecryptResponseDto}
 * as the actual payload, enabling consistent API responses that include metadata such as
 * response time, version, and status code.
 * </p>
 *
 * <p>Usage Example:</p>
 * <pre>
 * {
 *   "response": {
 *     "decryptedData": "Base64-decoded-string",
 *     "timestamp": "2025-07-04T14:00:00.000Z"
 *   },
 *   "id": "cryptomanager.decrypt",
 *   "version": "1.0",
 *   "responsetime": "2025-07-04T14:00:01.234Z",
 *   "status": "SUCCESS"
 * }
 * </pre>
 *
 * @see DecryptResponseDto
 * @see ResponseWrapper
 * @author MOSIP Team
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class CryptomanagerResponseDto extends ResponseWrapper<DecryptResponseDto> {
}
