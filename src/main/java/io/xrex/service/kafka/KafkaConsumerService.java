package io.xrex.service.kafka;

import io.xrex.model.dto.event.TransactionEventDto;
import io.xrex.service.TransferService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaConsumerService {

    private static final int DB_BATCH_SIZE = 200;
    private final TransferService transferService;

    @KafkaListener(topics = "${app.kafka.balance-transfer.topic}", groupId = "${app.kafka.balance-transfer.group}", containerFactory = "consumerFactory")
    public void consume(List<ConsumerRecord<String, TransactionEventDto>> records, Acknowledgment acknowledgment) {
        if (records.isEmpty()) {
            acknowledgment.acknowledge();
            return;
        }

        try {
            List<TransactionEventDto> allEvents = records.stream().map(ConsumerRecord::value).toList();
            int totalSize = allEvents.size();

            for (int i = 0; i < totalSize; i += DB_BATCH_SIZE) {
                int end = Math.min(i + DB_BATCH_SIZE, totalSize);
                List<TransactionEventDto> miniBatch = allEvents.subList(i, end);

                try {
                    // Each mini-batch is processed in its own transaction via batchTransfer
                    transferService.batchTransfer(miniBatch);
                    log.debug("Successfully persisted mini-batch of size: {}", miniBatch.size());
                } catch (Exception e) {
                    // Log the error for the specific mini-batch and continue with the next
                    // This enhances resilience, preventing one bad batch from stopping the entire poll.
                    log.error("Failed to process a mini-batch of size {}. Error: {}", miniBatch.size(), e.getMessage(), e);
                    // TODO: Consider sending the failed mini-batch to a dead-letter queue for manual inspection.
                }
            }

        } finally {
            // Acknowledge the entire polled batch, even if some mini-batches failed.
            // The failed ones are logged for later handling.
            acknowledgment.acknowledge();
        }
    }

}
