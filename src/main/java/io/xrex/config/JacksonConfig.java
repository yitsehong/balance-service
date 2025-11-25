package io.xrex.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class JacksonConfig {

    /**
     * Creates a primary, custom ObjectMapper bean that overrides the Spring Boot default.
     * This configuration sets the property naming strategy to SNAKE_CASE, allowing
     * for automatic conversion between snake_case JSON fields and camelCase Java properties.
     * It also configures the handling of `java.time.LocalDateTime` objects to be
     * serialized as ISO 8601 strings.
     *
     * @return A customized ObjectMapper instance.
     */
    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        // Configure Java 8 Date/Time API (LocalDateTime, etc.)
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS); // Serialize dates as ISO-8601 strings
        objectMapper.enable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE); // Adjust parsed dates to local time zone
        return objectMapper;
    }
}
