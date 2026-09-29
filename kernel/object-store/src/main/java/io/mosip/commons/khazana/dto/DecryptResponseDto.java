package io.mosip.commons.khazana.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Payload inside a cryptomanager HTTP response.
 * <p>
 * {@link CryptomanagerResponseDto} wraps this type.
 * {@link io.mosip.commons.khazana.util.OnlineCryptoUtil} reads {@link #data}
 * as Base64 ciphertext on encrypt and as Base64 plaintext on decrypt.
 * <p>
 * Lombok generates a no-args constructor, an all-args constructor, accessors, and
 * {@code equals}, {@code hashCode}, and {@code toString}.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DecryptResponseDto {

	/**
	 * Base64-encoded encrypt or decrypt result returned by cryptomanager.
	 */
	private String data;

}
