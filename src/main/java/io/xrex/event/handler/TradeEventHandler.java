package io.xrex.event.handler;

import io.grpc.stub.StreamObserver;
import io.xrex.dto.CancelOrderIdDto;
import io.xrex.dto.event.CancelOrderEventDto;
import io.xrex.dto.event.ExTradeDto;
import io.xrex.dto.event.TradeEventDto;
import io.xrex.grpc.TransferListRequest;
import io.xrex.grpc.TransferResponse;
import io.xrex.service.OrderTransferService;
import io.xrex.service.TradeTransferService;
import io.xrex.service.grpc.TransferGrpcService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

@Slf4j
@Component
@RequiredArgsConstructor
public class TradeEventHandler {

    private final TradeTransferService tradeTransferService;
    private final TransferGrpcService transferGrpcService;
    private final OrderTransferService orderTransferService;
    private final ExecutorService virtualThreadExecutor;

    @KafkaListener(topicPattern = "${app.kafka.trade-event.topic}", groupId = "${app.kafka.trade-event.group}", containerFactory = "tradeEventFactory")
    public void handleTradeEventTransfer(List<ConsumerRecord<String, TradeEventDto>> records, Acknowledgment acknowledgment) {
        if (records.isEmpty()) {
            acknowledgment.acknowledge();
            return;
        }

        try {
            // Step 1: Group all events by orderId.
            Map<String, List<TradeEventDto>> eventsByOrderId = new HashMap<>();
            for (ConsumerRecord<String, TradeEventDto> record : records) {
                eventsByOrderId.computeIfAbsent(record.key(), k -> new ArrayList<>()).add(record.value());
            }

            // Step 2: Aggregate trades for each orderId into a single event.
            List<TradeEventDto> aggregatedEvents = new ArrayList<>();
            for (Map.Entry<String, List<TradeEventDto>> entry : eventsByOrderId.entrySet()) {
                List<TradeEventDto> group = entry.getValue();
                if (group.isEmpty()) {
                    continue;
                }
                // Use the first event as a template.
                TradeEventDto masterEvent = group.getFirst();
                List<ExTradeDto> allTrades = new ArrayList<>();
                for (TradeEventDto event : group) {
                    if (event.getTrades() != null) {
                        allTrades.addAll(event.getTrades());
                    }
                }

                // Create a new aggregated event DTO.
                TradeEventDto aggregatedEvent = TradeEventDto.builder()
                        .orderId(masterEvent.getOrderId())
                        .pair(masterEvent.getPair())
                        .chainupId(masterEvent.getChainupId())
                        .trades(allTrades)
                        .build();
                aggregatedEvents.add(aggregatedEvent);
            }

            // Step 3: Concurrently process each aggregated event in a virtual thread.
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (TradeEventDto aggEvent : aggregatedEvents) {
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        long start = System.currentTimeMillis();
                        // Each aggregated event for a unique orderId is processed here.
                        tradeTransferService.handleTradeTransfer(aggEvent, buildResponseObserver());
                        log.info("Submitted aggregated transfer for order {} with {} trades in {} ms",
                                aggEvent.getOrderId(), aggEvent.getTrades().size(), System.currentTimeMillis() - start);
                    } catch (Exception e) {
                        log.error("Failed to process aggregated event for orderId: {}. Error: {}",
                                aggEvent.getOrderId(), e.getMessage(), e);
                    }
                }, virtualThreadExecutor);
                futures.add(future);
            }

            // Wait for all aggregated events to complete processing.
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            log.info("All {} records processed, aggregated into {} unique orders.", records.size(), aggregatedEvents.size());

        } catch (Exception e) {
            log.error("Failed to process trade event batch. Error: {}", e.getMessage(), e);
            // TODO: Consider sending all failed records to a dead-letter queue for manual inspection.
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
            long start = System.currentTimeMillis();
            StreamObserver<TransferResponse> responseObserver = buildResponseObserver();
            Map<CancelOrderIdDto, List<Long>> userPairCancelOrderIds = new HashMap<>();
            for (ConsumerRecord<String, CancelOrderEventDto> record : records) {
                CancelOrderEventDto event = record.value();
                CancelOrderIdDto key = new CancelOrderIdDto(event.getChainupId(), event.getPair(), event.getOrderType());
                userPairCancelOrderIds.computeIfAbsent(key, _ -> new ArrayList<>()).add(event.getOrderId());
            }

            for (Map.Entry<CancelOrderIdDto, List<Long>> entry : userPairCancelOrderIds.entrySet()) {
                TransferListRequest transferListRequest = orderTransferService.handleCancelOrderTransfer(entry.getKey(), entry.getValue());
                if (transferListRequest.getRequestsCount() > 0) {
                    transferGrpcService.transfer(transferListRequest, responseObserver);
                }
            }
            log.info("cancel order time={}ms", System.currentTimeMillis() - start);
        } catch (Exception e) {
            // Log the error for the specific mini-batch and continue with the next
            // This enhances resilience, preventing one bad batch from stopping the entire poll.
            log.error("Failed to process persisted. Error: {}", e.getMessage(), e);
            // TODO: Consider sending the failed mini-batch to a dead-letter queue for manual inspection.
        } finally {
            acknowledgment.acknowledge();
        }
    }

    private StreamObserver<TransferResponse> buildResponseObserver() {
        return new StreamObserver<>() {
            @Override
            public void onNext(TransferResponse value) {
                log.debug("[TradeEventHandler.handleTradeEventTransfer] transfer submitted to Raft: code={}, response={}", value.getCode(), value.getData());
            }

            @Override
            public void onError(Throwable t) {
                log.error("TradeEventHandler.handleTradeEventTransfer] transfer submitted error", t);
            }

            @Override
            public void onCompleted() {
                //log.info("Test transfer stream completed.");
            }
        };
    }
}
