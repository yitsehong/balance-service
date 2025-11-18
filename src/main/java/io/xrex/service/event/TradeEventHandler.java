package io.xrex.service.event;

import io.grpc.stub.StreamObserver;
import io.xrex.dto.CancelOrderIdDto;
import io.xrex.dto.event.CancelOrderEventDto;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class TradeEventHandler {

    private final TradeTransferService tradeTransferService;
    private final TransferGrpcService transferGrpcService;
    private final OrderTransferService orderTransferService;

    @KafkaListener(topicPattern = "${app.kafka.trade-event.topic}", groupId = "${app.kafka.trade-event.group}", containerFactory = "tradeEventFactory")
    public void handleTradeEventTransfer(List<ConsumerRecord<String, TradeEventDto>> records, Acknowledgment acknowledgment) {
        if (records.isEmpty()) {
            acknowledgment.acknowledge();
            return;
        }

        try {
            StreamObserver<TransferResponse> responseObserver = buildResponseObserver();
            for (ConsumerRecord<String, TradeEventDto> record : records) {
                long start = System.currentTimeMillis();
                TradeEventDto tradeEvent = record.value();
                log.info("[handleTradeEventTransfer] tradeEvent={}", tradeEvent);
                TransferListRequest transferListRequest = tradeTransferService.handleTradeTransfer(tradeEvent);
                transferGrpcService.transfer(transferListRequest, responseObserver);
                log.info("Transfer to {} completed in {} ms", tradeEvent, System.currentTimeMillis() - start);
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
                //log.info("Test transfer submitted to Raft: {}", value.getMessage());
            }

            @Override
            public void onError(Throwable t) {
                log.error("Test transfer error", t);
            }

            @Override
            public void onCompleted() {
                //log.info("Test transfer stream completed.");
            }
        };
    }
}
