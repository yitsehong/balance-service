package io.xrex.service.trade;

import io.grpc.stub.StreamObserver;
import io.xrex.dto.CancelOrderIdDto;
import io.xrex.dto.PairConfigDto;
import io.xrex.dto.event.ExTradeDto;
import io.xrex.grpc.TransferListRequest;
import io.xrex.grpc.TransferRequest;
import io.xrex.grpc.TransferResponse;
import io.xrex.persistence.entity.ExOrderEntity;
import io.xrex.persistence.entity.ExTradeEntity;
import io.xrex.service.ConfigService;
import io.xrex.service.grpc.TransferGrpcService;
import io.xrex.util.TransferRequestIntegrator;
import io.xrex.util.UUIDv7Generator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class TradeTransferService {

    @Value("${balance-service.fee-chainup-id}")
    private Integer feeChainupId;

    private final ConfigService configService;
    private final TransferGrpcService transferGrpcService;
    private final ExOrderTradeService exOrderTradeService;

    public void handleTradeTransfer(String pair, List<ExTradeDto> exTrades) {
        try {
            log.debug("handleTradeTransfer pair={}, aggregateTradeEvent={}", pair, exTrades);
            long t1 = System.currentTimeMillis();
            PairConfigDto pairConfig = configService.findPairConfigByPair(pair);
            StreamObserver<TransferResponse> responseObserver = buildResponseObserver();

            Map<Long, ExOrderEntity> exOrderEntityMap = exOrderTradeService.queryExOrderByExTrade(exTrades, pairConfig);
            long t2 = System.currentTimeMillis();
            List<ExTradeEntity> exTradeEntities = exOrderTradeService.updateExOrderByExTrade(exTrades, exOrderEntityMap, pairConfig);
            long t3 = System.currentTimeMillis();
            for (ExTradeEntity exTrade : exTradeEntities) {
                log.debug("[handleTradeTransfer] exTrade={}", exTrade);

                ExOrderEntity bid = exOrderEntityMap.get(exTrade.getBidId());
                ExOrderEntity ask = exOrderEntityMap.get(exTrade.getAskId());
                TransferRequestIntegrator transferRequestIntegrator = new TransferRequestIntegrator(exTrade, bid, ask, pairConfig, feeChainupId);
                transferGrpcService.transfer(transferRequestIntegrator.getTradeTransferListRequest(), responseObserver);

                exOrderTradeService.handleRemainOrder(bid, ask, pairConfig, responseObserver);
            }
            log.info("[handleTradeTransfer] pair={}, record size={}, query order={}ms, update order={}ms, transfer={}ms", pair, exTrades.size(), (t2 - t1), (t3 - t2), (System.currentTimeMillis() - t3));
        } catch (Exception e) {
            log.error("Failed to process trade event batch. Error: {}", e.getMessage(), e);
            // TODO: Consider sending all failed records to a dead-letter queue for manual inspection.
        }
    }

    public void handleCancelOrderTransfer(CancelOrderIdDto cancelOrderId, List<Long> cancelOrderIds) {
        PairConfigDto pairConfig = configService.findPairConfigByPair(cancelOrderId.pair());
        List<TransferRequest> requests = exOrderTradeService.handleCancelOrder(cancelOrderId, cancelOrderIds, pairConfig);
        if (!requests.isEmpty()) {
            TransferListRequest transferListRequest = TransferListRequest.newBuilder().addAllRequests(requests).setRequestId(UUIDv7Generator.generate()).build();
            StreamObserver<TransferResponse> responseObserver = buildResponseObserver();
            transferGrpcService.transfer(transferListRequest, responseObserver);
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
