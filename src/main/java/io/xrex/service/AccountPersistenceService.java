package io.xrex.service;

import io.xrex.model.dto.AccountIdDto;
import io.xrex.model.dto.event.TransactionEventDto;
import io.xrex.model.entity.TransactionEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Service
@RequiredArgsConstructor
public class AccountPersistenceService {

    private final TransferService transferService;
    private final LedgerBookService ledgerBookService;
    @Value("${app.kafka.balance-transfer.topic}-")
    private String topicPrefix;

    @KafkaListener(topicPattern = "${app.kafka.balance-transfer.topic}.*", groupId = "${app.kafka.balance-transfer.group}", containerFactory = "consumerFactory")
    public void consume(List<ConsumerRecord<String, TransactionEventDto>> records, Acknowledgment acknowledgment) {
        if (records.isEmpty()) {
            acknowledgment.acknowledge();
            return;
        }

        String topic = records.get(0).topic();
        // Assuming topic format is "base-topic-coin" e.g., "balance-transfer-btc"
        String coin = "unknown";
        int lastDashIndex = topic.lastIndexOf('-');
        if (lastDashIndex != -1 && lastDashIndex < topic.length() - 1) {
            coin = topic.substring(lastDashIndex + 1).toUpperCase();
        }
        MDC.put("coin", coin);

        try {
            long start = System.currentTimeMillis();
            List<TransactionEventDto> events = records.stream().map(ConsumerRecord::value).toList();
            MDC.put("eventKeys", events.get(0).getEventKey());
            ledgerBookService.produceLedgerBook(events);

            // Step 1: Aggregate balance changes for each account
            Map<AccountIdDto, BigDecimal> balanceAdjustments = events.stream()
                    .flatMap(event -> Stream.of(
                            Map.entry(new AccountIdDto(event.getFrom().getChainupId(), event.getFrom().getAssetType(), event.getFrom().getCoinSymbol(), event.getFrom().getAccountTag()), event.getFrom().getAmount()),
                            Map.entry(new AccountIdDto(event.getTo().getChainupId(), event.getTo().getAssetType(), event.getTo().getCoinSymbol(), event.getTo().getAccountTag()), event.getTo().getAmount())
                    )).collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.reducing(BigDecimal.ZERO, Map.Entry::getValue, BigDecimal::add)));
            // Step 2: Collect all individual ledger and transaction records for batch insertion
            List<TransactionEntity> transactions = events.stream().map(transferService::createTransactionEntityFromEvent).toList();

            // Each mini-batch is processed in its own transaction via batchTransfer
            transferService.batchTransfer(balanceAdjustments, transactions);
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
            MDC.remove("eventKeys");
            MDC.remove("coin"); // Clear the coin from MDC
        }
    }

}
