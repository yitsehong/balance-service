package io.xrex.config;

import io.xrex.model.dto.event.TransactionEventDto;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${spring.kafka.properties.security.protocol}")
    private String securityProtocol;
    @Value("${spring.kafka.properties.sasl.mechanism}")
    private String saslMechanism;
    @Value("${spring.kafka.properties.sasl.jaas.config}")
    private String saslJaasConfig;

    @Bean
    public ProducerFactory<String, TransactionEventDto> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        // For high throughput, you can tune these settings
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, "20"); // Wait up to 20ms to batch sends
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // 32KB batch size
        configProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4"); // Use snappy compression
        configProps.put(ProducerConfig.ACKS_CONFIG, "all"); // Leader ack is a good balance of safety and performance

        if (StringUtils.isNotBlank(saslJaasConfig)) {
            configProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
            configProps.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
            configProps.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
        }

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, TransactionEventDto> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
