package io.xrex.event.handler;

import io.xrex.model.dto.event.TransactionEventDto;
import io.xrex.service.ConfigService;
import io.xrex.service.RocksDBService;
import org.springframework.kafka.core.KafkaTemplate;

public class BalanceUpdateEventHandlerFactory {

    private final KafkaTemplate<String, TransactionEventDto> kafkaTemplate;
    private final ConfigService configService;
    private final RocksDBService rocksDBService;

    public BalanceUpdateEventHandlerFactory(
            KafkaTemplate<String, TransactionEventDto> kafkaTemplate,
            ConfigService configService,
            RocksDBService rocksDBService) {
        this.kafkaTemplate = kafkaTemplate;
        this.configService = configService;
        this.rocksDBService = rocksDBService;
    }

    public BalanceUpdateEventHandler create(String topic) {
        return new BalanceUpdateEventHandler(topic, kafkaTemplate, configService, rocksDBService);
    }
}
