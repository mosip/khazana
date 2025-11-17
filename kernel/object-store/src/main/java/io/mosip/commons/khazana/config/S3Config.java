package io.mosip.commons.khazana.config;
import com.amazonaws.auth.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class S3Config {

    @Value("${objectstore.access-key:}")
    private String accessKey;

    @Value("${objectstore.secret-key:}")
    private String secretKey;

    @Bean
    public AWSCredentialsProvider awsCredentialsProvider() {
        if (accessKey != null && !accessKey.isEmpty() && secretKey != null && !secretKey.isEmpty()) {
            // Load from application properties (not hardcoded)
            return new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));
        }

        // Fallback to the default AWS credentials provider chain
        return DefaultAWSCredentialsProviderChain.getInstance();
    }
}