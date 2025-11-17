package io.xrex.service;

import io.xrex.dto.AccountIdDto;
import io.xrex.dto.event.TransactionEventDto;
import io.xrex.persistence.entity.TransactionEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This service is responsible for consuming transaction events from Kafka,
 * processing them, and persisting the resulting state changes to the database.
 * It listens to Kafka topics, processes batches of transactions, updates account balances,
 * and records ledger entries and transactions.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AccountPersistenceService {

    private final TransferService transferService;
    private final LedgerBookService ledgerBookService;

    /**
     * Consumes a list of transaction events from a Kafka topic.
     * This method processes a batch of records, aggregates balance changes,
     * creates ledger and transaction records, and then persists these changes
     * within a database transaction.
     *
     * @param records A list of ConsumerRecord objects containing TransactionEventDto payloads.
     * @param acknowledgment The Acknowledgment object to confirm that the batch has been processed.
     */
    // TODO
    // @KafkaListener(topicPattern = "${app.kafka.balance-transfer.topic}.*", groupId = "${app.kafka.balance-transfer.group}", containerFactory = "consumerFactory")
    @KafkaListener(topicPattern = "${app.kafka.balance-transfer.topic}", groupId = "${app.kafka.balance-transfer.group}", containerFactory = "persistenceFactory")
    public void consume(List<ConsumerRecord<String, TransactionEventDto>> records, Acknowledgment acknowledgment) {
        if (records.isEmpty()) {
            acknowledgment.acknowledge();
            return;
        }

        try {
            long start = System.currentTimeMillis();
//            String topic = records.get(0).topic();
//            // Assuming topic format is "base-topic-coin" e.g., "balance-transfer-btc"
//            String coin = "unknown";
//            int lastDashIndex = topic.lastIndexOf('-');
//            if (lastDashIndex != -1 && lastDashIndex < topic.length() - 1) {
//                coin = topic.substring(lastDashIndex + 1).toLowerCase();
//            }

            List<TransactionEventDto> events = records.stream().map(ConsumerRecord::value).toList();
            ledgerBookService.produceLedgerBook(events);

            // Step 1: Aggregate balance changes for each account
            Map<AccountIdDto, BigDecimal> balanceAdjustments = events.parallelStream()
                    .flatMap(event -> Stream.of(
                            Map.entry(new AccountIdDto(event.getFrom().getChainupId(), event.getFrom().getAssetType(), event.getFrom().getCoinSymbol(), event.getFrom().getAccountTag()), event.getFrom().getAmount()),
                            Map.entry(new AccountIdDto(event.getTo().getChainupId(), event.getTo().getAssetType(), event.getTo().getCoinSymbol(), event.getTo().getAccountTag()), event.getTo().getAmount())
                    )).collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.reducing(BigDecimal.ZERO, Map.Entry::getValue, BigDecimal::add)));
            // Step 2: Collect all individual ledger and transaction records for batch insertion
            List<TransactionEntity> transactions = events.stream().map(transferService::createTransactionEntityFromEvent).toList();

            // Each mini-batch is processed in its own transaction via batchTransfer
            transferService.batchTransfer(balanceAdjustments, transactions);
//            log.info("Successfully persisted of event size={}, coin={}, balanceAdjustments size={}, time={}ms", events.size(), coin, balanceAdjustments.size(), System.currentTimeMillis() - start);
            log.info("Successfully persisted of event size={}, balanceAdjustments size={}, time={}ms", events.size(), balanceAdjustments.size(), System.currentTimeMillis() - start);
        } catch (Exception e) {
            // Log the error for the specific mini-batch and continue with the next
            // This enhances resilience, preventing one bad batch from stopping the entire poll.
            log.error("Failed to process persisted. Error: {}", e.getMessage(), e);
            // TODO: Consider sending the failed mini-batch to a dead-letter queue for manual inspection.
        } finally {
            // Acknowledge the entire polled batch, even if some mini-batches failed.
            // The failed ones are logged for later handling.
            acknowledgment.acknowledge();
        }
    }

}
