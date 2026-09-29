package io.mosip.commons.khazana.dto;



import io.mosip.kernel.core.http.ResponseWrapper;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Cryptomanager HTTP response used by {@link io.mosip.commons.khazana.util.OnlineCryptoUtil}.
 * <p>
 * Extends the kernel {@code ResponseWrapper} so {@code getResponse()} yields a
 * {@link DecryptResponseDto} and {@code getErrors()} yields service errors.
 * A non-empty error list is thrown as
 * {@link io.mosip.commons.khazana.exception.ObjectStoreAdapterException}.
 * <p>
 * Lombok generates a no-args constructor, accessors, and {@code equals},
 * {@code hashCode}, and {@code toString}, including the wrapper fields.
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class CryptomanagerResponseDto extends ResponseWrapper<DecryptResponseDto> {

}
