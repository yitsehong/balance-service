package io.xrex.config;

import io.xrex.dto.event.CancelOrderEventDto;
import io.xrex.dto.event.TradeEventDto;
import io.xrex.dto.event.TransactionEventDto;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${app.kafka.transfer-persistence-event.max-poll-records}")
    private Integer transferPersistenceEventMaxPollRecords;
    @Value("${app.kafka.trade-event.max-poll-records}")
    private Integer tradeEventMaxPollRecords;
    @Value("${app.kafka.cancel-order-event.max-poll-records}")
    private Integer cancelOrderEventMaxPollRecords;
    @Value("${spring.kafka.properties.security.protocol}")
    private String securityProtocol;
    @Value("${spring.kafka.properties.sasl.mechanism}")
    private String saslMechanism;
    @Value("${spring.kafka.properties.sasl.jaas.config}")
    private String saslJaasConfig;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, TransactionEventDto> transferPersistenceEventFactory() {
        Map<String, Object> props = buildCommonProperties();
        props.put(JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, TransactionEventDto.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, transferPersistenceEventMaxPollRecords);

        ConcurrentKafkaListenerContainerFactory<String, TransactionEventDto> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props));
        factory.setBatchListener(true); // Enable batch listening
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(1000L, 3L)));
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, TradeEventDto> tradeEventFactory() {
        Map<String, Object> props = buildCommonProperties();
        props.put(JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, TradeEventDto.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, tradeEventMaxPollRecords);

        ConcurrentKafkaListenerContainerFactory<String, TradeEventDto> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props));
        factory.setBatchListener(true); // Enable batch listening
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(1000L, 3L)));
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, CancelOrderEventDto> cancelOrderEventFactory() {
        Map<String, Object> props = buildCommonProperties();
        props.put(JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, CancelOrderEventDto.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, cancelOrderEventMaxPollRecords);

        ConcurrentKafkaListenerContainerFactory<String, CancelOrderEventDto> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props));
        factory.setBatchListener(true); // Enable batch listening
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(1000L, 3L)));
        return factory;
    }

    private Map<String, Object> buildCommonProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);
        props.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, "io.xrex.dto.event");
        props.put(JacksonJsonDeserializer.USE_TYPE_INFO_HEADERS, true);
        props.put(JacksonJsonDeserializer.REMOVE_TYPE_INFO_HEADERS, false);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        if (StringUtils.isNotBlank(saslJaasConfig)) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
            props.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
            props.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
        }
        return props;
    }
}
