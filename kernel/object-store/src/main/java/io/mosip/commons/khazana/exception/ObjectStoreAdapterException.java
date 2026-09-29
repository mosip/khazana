package io.mosip.commons.khazana.exception;

import io.mosip.kernel.core.exception.BaseUncheckedException;

/**
 * Unchecked failure raised when a Khazana adapter cannot complete a storage or crypto call.
 * <p>
 * {@link io.mosip.commons.khazana.impl.S3Adapter} throws this with
 * {@link io.mosip.commons.khazana.constant.KhazanaErrorCodes#OBJECT_STORE_NOT_ACCESSIBLE}
 * after an S3 error, and clears its shared client so the next call reconnects.
 * {@link io.mosip.commons.khazana.util.OnlineCryptoUtil} throws it when cryptomanager
 * returns an error or the encrypt or decrypt call fails.
 */
public class ObjectStoreAdapterException extends BaseUncheckedException {

    /**
     * Creates an exception with an error code and message.
     *
     * @param errorCode stable Khazana or caller-supplied error code; may be empty
     * @param message   description of the failure
     */
    public ObjectStoreAdapterException(String errorCode, String message) {
        super(errorCode, message);
    }

    /**
     * Creates an exception with an error code, message, and cause.
     *
     * @param errorCode stable Khazana or caller-supplied error code; may be empty
     * @param message   description of the failure
     * @param e         underlying storage, I/O, or cryptomanager failure
     */
    public ObjectStoreAdapterException(String errorCode, String message, Throwable e) {
        super(errorCode, message, e);
    }
}
