package io.xrex.service.kafka;

import io.xrex.model.dto.event.TransactionEventDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);

    private final KafkaTemplate<String, TransactionEventDto> kafkaTemplate;

    @Value("${app.kafka.balance-transfer.topic}")
    private String topic;

    public KafkaProducerService(KafkaTemplate<String, TransactionEventDto> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendTransferEvent(TransactionEventDto event) {
        try {
            // Using transactionId as the key to ensure related events go to the same partition if needed.
            kafkaTemplate.send(topic, event.getEventKey(), event);
        } catch (Exception e) {
            // In a real-world scenario, a failure here requires a robust compensation mechanism.
            // For example, adding the event to a persistent dead-letter queue for later retry.
            logger.error("Failed to send transfer event [{}] to Kafka. CRITICAL: Compensation required.", event.getEventKey(), e);
            // Depending on the system's requirements, you might re-throw a specific exception
            // to let the caller know the async part failed.
            throw new RuntimeException("Failed to publish Kafka event", e);
        }
    }
}
