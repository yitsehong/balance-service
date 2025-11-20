package io.xrex.service;

import io.grpc.stub.StreamObserver;
import io.xrex.dto.PairConfigDto;
import io.xrex.dto.event.ExTradeDto;
import io.xrex.grpc.TransferResponse;
import io.xrex.persistence.entity.ExOrderEntity;
import io.xrex.persistence.entity.ExTradeEntity;
import io.xrex.persistence.repository.ExOrderDao;
import io.xrex.service.grpc.TransferGrpcService;
import io.xrex.service.trade.ExOrderTradeService;
import io.xrex.util.TransferRequestIntegrator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class TradeTransferService {

    @Value("${balance-service.fee-chainup-id}")
    private Integer feeChainupId;

    private final ConfigService configService;
    private final TransferGrpcService transferGrpcService;
    private final ExOrderTradeService exOrderTradeService;
    private final RemainReturnTransferService  remainReturnTransferService;

    private final ExOrderDao exOrderDao;

    @Transactional
    public void handleTradeTransfer(String pair, List<ExTradeDto> exTradeDtos, StreamObserver<TransferResponse> responseObserver) {
        log.debug("handleTradeTransfer pair={}, aggregateTradeEvent={}", pair, exTradeDtos);
        PairConfigDto pairConfig = configService.findPairConfigByPair(pair);
        String orderTable = pairConfig.getOrderTable();

        List<Long> orderIds = new ArrayList<>();
        for (ExTradeDto exTradeDto : exTradeDtos) {
            orderIds.add(exTradeDto.getBidId());
            orderIds.add(exTradeDto.getAskId());
        }

        Map<Long, ExOrderEntity> exOrderEntityMap = exOrderDao.findByIdIn(orderIds, orderTable).stream().collect(Collectors.toMap(ExOrderEntity::getId, e -> e));

        List<ExTradeEntity> exTradeEntities = exOrderTradeService.updateExOrderByExTrade(exTradeDtos, exOrderEntityMap, pairConfig);
        for (ExTradeEntity exTrade : exTradeEntities) {
            log.debug("[handleTradeTransfer] exTrade={}", exTrade);

            ExOrderEntity bid = exOrderEntityMap.get(exTrade.getBidId());
            ExOrderEntity ask = exOrderEntityMap.get(exTrade.getAskId());

            TransferRequestIntegrator transferRequestIntegrator = new TransferRequestIntegrator(exTrade, bid, ask, pairConfig, feeChainupId);
            transferGrpcService.transfer(transferRequestIntegrator.getTradeTransferListRequest(), responseObserver);

            remainReturnTransferService.handleRemainMoney(bid, pairConfig, responseObserver);
            remainReturnTransferService.handleRemainMoney(ask, pairConfig, responseObserver);
        }
    }



}
