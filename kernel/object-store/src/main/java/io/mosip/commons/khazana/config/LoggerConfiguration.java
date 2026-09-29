package io.mosip.commons.khazana.config;

import io.mosip.kernel.core.logger.spi.Logger;
import io.mosip.kernel.logger.logback.appender.ConsoleAppender;
import io.mosip.kernel.logger.logback.factory.Logfactory;

/**
 * Console logger factory for Khazana.
 * <p>
 * {@link io.mosip.commons.khazana.impl.S3Adapter} and
 * {@link io.mosip.commons.khazana.util.SafeS3InputStream} obtain a kernel
 * {@link Logger} through {@link #logConfig(Class)} and pass {@link #SESSIONID}
 * and {@link #REGISTRATIONID} as the first two log arguments.
 */
public class LoggerConfiguration {

	/**
	 * Log context key used as the session id on Khazana logger calls.
	 */
	public static final String SESSIONID = "SESSION_ID";

	/**
	 * Log context key used as the registration id on Khazana logger calls.
	 */
	public static final String REGISTRATIONID = "REGISTRATION_ID";

	/**
	 * Private Constructor to prevent instantiation.
	 */
	private LoggerConfiguration() {
	}

	/**
	 * This method sets the logger target, and returns appender.
	 * 
	 * @param clazz the class whose name is bound to the logger
	 * @return the kernel SLF4J logger for {@code clazz}
	 */
	public static Logger logConfig(Class<?> clazz) {
		return Logfactory.getSlf4jLogger(clazz);
	}
}
