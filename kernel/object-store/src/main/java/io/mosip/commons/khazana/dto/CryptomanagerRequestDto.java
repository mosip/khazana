package io.mosip.commons.khazana.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.LocalDateTime;

/**
 * Request body sent to cryptomanager by {@link io.mosip.commons.khazana.util.OnlineCryptoUtil}.
 * <p>
 * {@code applicationId} is {@code REGISTRATION}. {@code data}, {@code salt}, and {@code aad}
 * are Base64. The timestamp uses UTC pattern {@code yyyy-MM-dd'T'HH:mm:ss.SSS'Z'}.
 * Offline encryption uses the kernel cryptomanager DTO instead of this type.
 * <p>
 * Lombok generates a no-args constructor, an all-args constructor, accessors, and
 * {@code equals}, {@code hashCode}, and {@code toString}.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor

public class CryptomanagerRequestDto {
	/**
	 * Application id of the module requesting encrypt or decrypt.
	 */
	
	@NotBlank(message = "should not be null or empty")
	private String applicationId;
	/**
	 * Reference id of the key used to encrypt or decrypt.
	 */
	
	private String referenceId;
	/**
	 * UTC timestamp of the request, serialized as {@code yyyy-MM-dd'T'HH:mm:ss.SSS'Z'}.
	 */

	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
	@NotNull
	private LocalDateTime timeStamp;
	/**
	 * Data in BASE64 encoding to encrypt/decrypt
	 */
	
	@NotBlank(message = "should not be null or empty")
	private String data;

	/**
	 * salt in BASE64 encoding for encrypt/decrypt
	 */

	@NotBlank(message = "should not be null or empty")
	private String salt;

	/**
	 * aad in BASE64 encoding for encrypt/decrypt
	 */

	@NotBlank(message = "should not be null or empty")
	private String aad;

	/**
	 * When {@code true}, cryptomanager prepends the certificate thumbprint to the result.
	 * {@link io.mosip.commons.khazana.util.OnlineCryptoUtil} sets this from
	 * {@code crypto.PrependThumbprint.enable} (default {@code true}).
	 */
	private Boolean prependThumbprint;
}
