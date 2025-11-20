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
            Map<String, List<ExTradeDto>> eventsByPair = new HashMap<>();
            for (ConsumerRecord<String, TradeEventDto> record : records) {
                eventsByPair.computeIfAbsent(record.value().getPair(), _ -> new ArrayList<>()).add(record.value().getTrade());
            }

            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (Map.Entry<String, List<ExTradeDto>> entry : eventsByPair.entrySet()) {
                if (entry.getValue().isEmpty()) {
                    continue;
                }

                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        tradeTransferService.handleTradeTransfer(entry.getKey(), entry.getValue(), buildResponseObserver());
                    } catch (Exception e) {
                        log.error("Failed to process trade event batch. Error: {}", e.getMessage(), e);
                        // TODO: Consider sending all failed records to a dead-letter queue for manual inspection.
                    }
                }, virtualThreadExecutor);
                futures.add(future);
            }

            // Wait for all aggregated events to complete processing.
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            log.info("All {} records processed.", records.size());
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
