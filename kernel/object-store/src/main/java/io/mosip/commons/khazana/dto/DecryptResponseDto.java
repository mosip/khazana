package io.mosip.commons.khazana.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * {@code DecryptResponseDto} represents the decrypted payload returned by the
 * CryptoManager service after a successful decryption operation.
 *
 * <p>This class is typically wrapped inside a {@link CryptomanagerResponseDto}
 * to standardize API responses.</p>
 *
 * <p>Example usage:</p>
 * <pre>
 * DecryptResponseDto response = new DecryptResponseDto();
 * String plainText = response.getData();
 * </pre>
 *
 * <p>Example JSON response:</p>
 * <pre>
 * {
 *   "data": "plain-text-result"
 * }
 * </pre>
 * 
 * @author MOSIP Team
 * @see CryptomanagerResponseDto
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DecryptResponseDto {

    /**
     * The decrypted data string (typically a UTF-8 encoded representation of the original input).
     */
    private String data;
}
