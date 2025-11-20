package io.xrex.event.handler;

import io.xrex.dto.CancelOrderIdDto;
import io.xrex.dto.event.CancelOrderEventDto;
import io.xrex.dto.event.ExTradeDto;
import io.xrex.dto.event.TradeEventDto;
import io.xrex.service.trade.TradeTransferService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

@Slf4j
@Component
@RequiredArgsConstructor
public class TradeEventHandler {

    private final TradeTransferService tradeTransferService;
    private final ExecutorService virtualThreadExecutor;

    @KafkaListener(topicPattern = "${app.kafka.trade-event.topic}", groupId = "${app.kafka.trade-event.group}", containerFactory = "tradeEventFactory")
    public void handleTradeEventTransfer(List<ConsumerRecord<String, TradeEventDto>> records, Acknowledgment acknowledgment) {
        if (records.isEmpty()) {
            acknowledgment.acknowledge();
            return;
        }

        try {
            long start = System.currentTimeMillis();
            Map<String, List<ExTradeDto>> eventsByPair = new HashMap<>();
            for (ConsumerRecord<String, TradeEventDto> record : records) {
                eventsByPair.computeIfAbsent(record.value().getPair(), _ -> new ArrayList<>()).add(record.value().getTrade());
            }

            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (Map.Entry<String, List<ExTradeDto>> entry : eventsByPair.entrySet()) {
                if (entry.getValue().isEmpty()) {
                    continue;
                }
                futures.add(CompletableFuture.runAsync(() -> tradeTransferService.handleTradeTransfer(entry.getKey(), entry.getValue()), virtualThreadExecutor));
            }

            // Wait for all aggregated events to complete processing.
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            long end = System.currentTimeMillis();
            log.info("Trade event transfer complete in {} ms with {} records", (end - start), records.size());
        } finally {
            acknowledgment.acknowledge();
        }
    }

    @KafkaListener(topicPattern = "${app.kafka.cancel-order-event.topic}", groupId = "${app.kafka.cancel-order-event.group}", containerFactory = "cancelOrderEventFactory")
    public void handleCancelOrderEventTransfer(List<ConsumerRecord<String, CancelOrderEventDto>> records, Acknowledgment acknowledgment) {
        if (records.isEmpty()) {
            acknowledgment.acknowledge();
            return;
        }

        try {
            Map<CancelOrderIdDto, List<Long>> userPairCancelOrderIds = new HashMap<>();
            for (ConsumerRecord<String, CancelOrderEventDto> record : records) {
                CancelOrderEventDto event = record.value();
                CancelOrderIdDto key = new CancelOrderIdDto(event.getChainupId(), event.getPair(), event.getOrderType());
                userPairCancelOrderIds.computeIfAbsent(key, _ -> new ArrayList<>()).add(event.getOrderId());
            }

            for (Map.Entry<CancelOrderIdDto, List<Long>> entry : userPairCancelOrderIds.entrySet()) {
                tradeTransferService.handleCancelOrderTransfer(entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            // Log the error for the specific mini-batch and continue with the next
            // This enhances resilience, preventing one bad batch from stopping the entire poll.
            log.error("Failed to process persisted. Error: {}", e.getMessage(), e);
            // TODO: Consider sending the failed mini-batch to a dead-letter queue for manual inspection.
        } finally {
            acknowledgment.acknowledge();
        }
    }

}
