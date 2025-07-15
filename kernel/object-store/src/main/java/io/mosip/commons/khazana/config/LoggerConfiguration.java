package io.mosip.commons.khazana.config;

import io.mosip.kernel.core.logger.spi.Logger;
import io.mosip.kernel.logger.logback.factory.Logfactory;

/**
 * {@code LoggerConfiguration} is a utility class that provides centralized access to
 * application-wide logging configuration using the MOSIP SLF4J-compatible logging infrastructure.
 * 
 * <p>
 * It offers methods to obtain logger instances and defines key constants related to logging context, 
 * such as session and registration IDs used in audit trails and traceability.
 * </p>
 * 
 * <p>Example usage:</p>
 * <pre>
 *     private static final Logger LOGGER = LoggerConfiguration.logConfig(MyService.class);
 *     LOGGER.info("Service started...");
 * </pre>
 * 
 * @author MOSIP Team
 */
public class LoggerConfiguration {

    /**
     * MDC (Mapped Diagnostic Context) key for session ID used in logs to correlate requests.
     */
    public static final String SESSIONID = "SESSION_ID";

    /**
     * MDC key for registration ID, useful in identifying logs tied to registration workflows.
     */
    public static final String REGISTRATIONID = "REGISTRATION_ID";

    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private LoggerConfiguration() {
        // Prevent instantiation
    }

    /**
     * Returns a logger instance for the specified class using MOSIP's Logfactory.
     *
     * @param clazz the target class for which logger is to be created.
     * @return a {@link Logger} instance specific to the given class.
     */
    public static Logger logConfig(Class<?> clazz) {
        return Logfactory.getSlf4jLogger(clazz);
    }
}
