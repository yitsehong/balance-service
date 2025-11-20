package io.xrex.service;

import io.grpc.stub.StreamObserver;
import io.xrex.dto.PairConfigDto;
import io.xrex.dto.event.ExTradeDto;
import io.xrex.enums.OrderLeverType;
import io.xrex.enums.OrderSide;
import io.xrex.enums.OrderStatus;
import io.xrex.enums.TransactionScene;
import io.xrex.grpc.TransferListRequest;
import io.xrex.grpc.TransferRequest;
import io.xrex.grpc.TransferResponse;
import io.xrex.persistence.entity.ExOrderEntity;
import io.xrex.persistence.entity.ExTradeEntity;
import io.xrex.persistence.repository.ExOrderDao;
import io.xrex.persistence.repository.ExTradeDao;
import io.xrex.service.grpc.TransferGrpcService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.xrex.util.XrexConstant.SYSTEM_CHAINUP_ID;

@Slf4j
@Service
@RequiredArgsConstructor
public class TradeTransferService {

    @Value("${balance-service.fee-chainup-id}")
    private Integer feeChainupId;

    private final ConfigService configService;
    private final TransferGrpcService transferGrpcService;
    private final RemainReturnTransferService  remainReturnTransferService;

    private final ExOrderDao exOrderDao;
    private final ExTradeDao exTradeDao;

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
        List<ExTradeEntity> insertExTradeEntityList = new ArrayList<>();
        List<ExOrderEntity> updateExOrderEntityList = new ArrayList<>();
        for (ExTradeDto exTradeDto : exTradeDtos) {
            ExOrderEntity bid = exOrderEntityMap.get(exTradeDto.getBidId());
            ExOrderEntity ask = exOrderEntityMap.get(exTradeDto.getAskId());

            updateOrder(exTradeDto, bid, pairConfig, updateExOrderEntityList);
            updateOrder(exTradeDto, ask, pairConfig, updateExOrderEntityList);
            log.debug("[handleTradeTransfer] exTradeDto={}, bid={}, ask={}", exTradeDto, bid, ask);
            insertExTradeEntityList.add(exTradeDto.toEntity());
        }

        exTradeDao.batchInsert(insertExTradeEntityList, pairConfig.getTradeTable());
        exOrderDao.batchUpsert(updateExOrderEntityList, orderTable);

        for (ExTradeEntity exTrade : insertExTradeEntityList) {
            log.info("[handleTradeTransfer] exTrade={}", exTrade);

            ExOrderEntity bid = exOrderEntityMap.get(exTrade.getBidId());
            ExOrderEntity ask = exOrderEntityMap.get(exTrade.getAskId());
            List<TransferRequest> requests = new ArrayList<>();
            requests.add(quoteAmountTransfer(exTrade, bid, ask, pairConfig));
            requests.add(baseAmountTransfer(exTrade, bid, ask, pairConfig));
            requests.add(sellFeeTransfer(exTrade, bid, ask, pairConfig));
            requests.add(buyFeeTransfer(exTrade, bid, ask, pairConfig));

            TransferListRequest tradeTransferListRequest = TransferListRequest.newBuilder()
                    .addAllRequests(requests).setRequestId(exTrade.getTradeNonce()).build();
            transferGrpcService.transfer(tradeTransferListRequest, responseObserver);

            remainReturnTransferService.handleRemainMoney(bid, pairConfig, responseObserver);
            remainReturnTransferService.handleRemainMoney(ask, pairConfig, responseObserver);
        }
    }

    private void updateOrder(ExTradeDto exTrade, ExOrderEntity exOrder, PairConfigDto pairConfig, List<ExOrderEntity> updateExOrderEntityList) {
        if (exOrder == null) {
            return;
        }

        if (OrderLeverType.MARKET_MAKING_ORDER == exOrder.getOrderType()) {
            if (OrderSide.BUY == exOrder.getSide()) {
                exTrade.setBuyFee(BigDecimal.ZERO);
                exTrade.setBuyFeeCoin(pairConfig.getQuote());
            } else {
                exTrade.setSellFee(BigDecimal.ZERO);
                exTrade.setSellFeeCoin(pairConfig.getQuote());
            }
            return;
        }

        BigDecimal tradeQuoteAmount = exTrade.getVolume().multiply(exTrade.getPrice());
        if (OrderSide.BUY == exOrder.getSide()) {
            BigDecimal feeRate = OrderSide.BUY.value.equalsIgnoreCase(exTrade.getTrendSide()) ?
                    BigDecimal.valueOf(exOrder.getFeeRateTaker()) : BigDecimal.valueOf(exOrder.getFeeRateMaker());

            if (exOrder.isInnerFeeDeduct()) {
                exTrade.setBuyFee(exTrade.getVolume().multiply(feeRate));
                exTrade.setBuyFeeCoin(pairConfig.getBase());
            } else {
                exTrade.setBuyFee(tradeQuoteAmount.multiply(feeRate));
                exTrade.setBuyFeeCoin(pairConfig.getQuote());
            }
        } else {
            BigDecimal feeRate = OrderSide.BUY.value.equalsIgnoreCase(exTrade.getTrendSide()) ?
                    BigDecimal.valueOf(exOrder.getFeeRateMaker()) : BigDecimal.valueOf(exOrder.getFeeRateTaker());
            exTrade.setSellFee(tradeQuoteAmount.multiply(feeRate));
            exTrade.setSellFeeCoin(pairConfig.getQuote());
        }

        BigDecimal fee = OrderSide.BUY == exOrder.getSide() ? exTrade.getBuyFee() : exTrade.getSellFee();
        exOrder.setFee(Optional.ofNullable(exOrder.getFee()).orElse(BigDecimal.ZERO).multiply(fee));

        BigDecimal dealMoney = exTrade.getVolume().multiply(exTrade.getPrice());
        exOrder.setDealVolume(Optional.ofNullable(exOrder.getDealVolume()).orElse(BigDecimal.ZERO).add(exTrade.getVolume()));
        if (exOrder.isOuterFeeDeduct()) {
            BigDecimal tradeQuoteFeeAmount = dealMoney.multiply(new BigDecimal(exOrder.getFeeRateTaker().toString()));
            exOrder.setFee(Optional.ofNullable(exOrder.getFee()).orElse(BigDecimal.ZERO).add(tradeQuoteFeeAmount));
        }
        exOrder.setDealMoney(Optional.ofNullable(exOrder.getDealMoney()).orElse(BigDecimal.ZERO).add(dealMoney));

        // 平均成交价
        BigDecimal orderAvgPrice = exOrder.getDealMoney().divide(exOrder.getDealVolume(), 12, RoundingMode.HALF_EVEN);
        exOrder.setAvgPrice(orderAvgPrice);
        if (!exOrder.isMarketOrder()) {
            exOrder.setStatus(exOrder.isFilled(pairConfig) ? OrderStatus.FILLED : OrderStatus.PART_FILLED);
        }
        LocalDateTime updateTime = LocalDateTime.now();
        exOrder.setMtime(updateTime);

        OrderStatus status = exOrder.isFilled(pairConfig) ? OrderStatus.FILLED : OrderStatus.PART_FILLED;
        exOrder.setStatus(status);
        updateExOrderEntityList.add(exOrder);
    }

    private TransferRequest quoteAmountTransfer(ExTradeEntity exTradeEntity, ExOrderEntity bid, ExOrderEntity ask, PairConfigDto pairConfig) {
        // buyer.quoteLock -> seller.quoteNormal  quoteAmount
        Integer fromType;
        if (OrderLeverType.NORMAL_ORDER.value == exTradeEntity.getBuyType()) {
            fromType = pairConfig.getQuoteAccountLock();
        } else if (OrderLeverType.MARKET_MAKING_ORDER.value == exTradeEntity.getBuyType()) {
            fromType = pairConfig.getQuoteMmAccountLock();
        } else {
            fromType = bid.getQuoteAccountType();
        }

        Integer toType;
        if (OrderLeverType.NORMAL_ORDER.value == exTradeEntity.getSellType()) {
            toType = pairConfig.getQuoteAccountNormal();
        } else if (OrderLeverType.MARKET_MAKING_ORDER.value == exTradeEntity.getSellType()) {
            toType = pairConfig.getQuoteMmAccountNormal();
        } else {
            toType = ask.getQuoteAccountType();
        }

        BigDecimal quoteAmount = exTradeEntity.getPrice().multiply(exTradeEntity.getVolume());
        TransferRequest.Builder transA = TransferRequest.newBuilder()
                .setFromUid(exTradeEntity.getBidUserId()).setFromType(fromType)
                .setToUid(exTradeEntity.getAskUserId()).setToType(toType)
                .setAmount(quoteAmount.toPlainString())
                .setRefType(pairConfig.getTradeTable())
                .setRefId(exTradeEntity.getId())
                .setMeta("trade.transfer.quoteAmount")
                .setScene(TransactionScene.TRADE.value)
                .setOpUid(SYSTEM_CHAINUP_ID)
                .setOpIp(StringUtils.EMPTY);
        if (bid.isRelatedSubAccount()) {
            transA.setFromSubType(bid.getQuoteSubaccountType());
        }
        if (ask.isRelatedSubAccount()) {
            transA.setToSubType(ask.getQuoteSubaccountType());
        }
        return transA.build();
    }

    private TransferRequest baseAmountTransfer(ExTradeEntity exTradeEntity, ExOrderEntity bid, ExOrderEntity ask, PairConfigDto pairConfig) {
        // seller.baseLock -> buyer.baseNormal  baseVolume
        Integer fromType;
        if (OrderLeverType.NORMAL_ORDER.value == exTradeEntity.getSellType()) {
            fromType = pairConfig.getBaseAccountLock();
        } else if (OrderLeverType.MARKET_MAKING_ORDER.value == exTradeEntity.getSellType()) {
            fromType = pairConfig.getBaseMmAccountLock();
        } else {
            fromType = ask.getBaseAccountType();
        }

        Integer toType;
        if (OrderLeverType.NORMAL_ORDER.value == exTradeEntity.getBuyType()) {
            toType = pairConfig.getBaseAccountNormal();
        } else if (OrderLeverType.MARKET_MAKING_ORDER.value == exTradeEntity.getBuyType()) {
            toType = pairConfig.getBaseMmAccountNormal();
        } else {
            toType = bid.getBaseAccountType();
        }

        TransferRequest.Builder transB = TransferRequest.newBuilder()
                .setFromUid(exTradeEntity.getAskUserId()).setFromType(fromType)
                .setToUid(exTradeEntity.getBidUserId()).setToType(toType)
                .setAmount(exTradeEntity.getVolume().toPlainString())
                .setRefType(pairConfig.getTradeTable())
                .setRefId(exTradeEntity.getId())
                .setMeta("trade.transfer.baseVolume")
                .setScene(TransactionScene.TRADE.value)
                .setOpUid(SYSTEM_CHAINUP_ID)
                .setOpIp(StringUtils.EMPTY);
        if (bid.isRelatedSubAccount()) {
            transB.setToSubType(bid.getBaseSubaccountType());
        }
        if (ask.isRelatedSubAccount()) {
            transB.setFromSubType(ask.getBaseSubaccountType());
        }
        return transB.build();
    }

    private TransferRequest sellFeeTransfer(ExTradeEntity exTradeEntity, ExOrderEntity bid, ExOrderEntity ask, PairConfigDto pairConfig) {
        // 收seller手续费：
        // inner: seller.quoteNormal -> exchange.quoteFee  quoteFeeAmount
        // outer: seller.quoteLock -> exchange.quoteFee  quoteFeeAmount
        Integer fromType;
        Integer toType;
        String scene = getFeeTransactionScene(ask);
        String meta = getOrderFeeTransactionMeta(exTradeEntity, OrderSide.SELL, pairConfig);
        if (OrderLeverType.NORMAL_ORDER.value == exTradeEntity.getSellType()
                || OrderLeverType.MARKET_MAKING_ORDER.value == exTradeEntity.getSellType()) {
            fromType = pairConfig.getQuoteAccountNormal();
            toType = pairConfig.getSysQuoteAccount();
        } else {
            fromType = ask.getQuoteAccountType();
            toType = ask.getFeeAccountType();
        }

        TransferRequest.Builder transC = TransferRequest.newBuilder()
                .setFromUid(exTradeEntity.getAskUserId()).setFromType(fromType)
                .setToUid(feeChainupId).setToType(toType)
                .setAmount(exTradeEntity.getSellFee().toPlainString())
                .setRefType(pairConfig.getTradeTable())
                .setRefId(exTradeEntity.getId())
                .setMeta(meta)
                .setScene(scene)
                .setOpUid(SYSTEM_CHAINUP_ID)
                .setOpIp(StringUtils.EMPTY);
        if (ask.isRelatedSubAccount()) {
            transC.setFromSubType(ask.getQuoteSubaccountType());
        }

        if (bid.isRelatedSubAccount()) {
            transC.setToSubType(bid.getQuoteSubaccountType());
        }
        return transC.build();
    }

    private TransferRequest buyFeeTransfer(ExTradeEntity exTradeEntity, ExOrderEntity bid, ExOrderEntity ask, PairConfigDto pairConfig) {
        // 收buyer手续费：
        // inner: buyer.baseNormal -> exchange.baseFee  baseFeeVolume
        // outer: buyer.baseLock -> exchange.baseFee  baseFeeVolume
        Integer fromType;
        Integer toType;
        String scene = getFeeTransactionScene(bid);
        String meta = getOrderFeeTransactionMeta(exTradeEntity, OrderSide.BUY, pairConfig);
        if (bid.isInnerFeeDeduct()) {
            if (exTradeEntity.getBuyType() == OrderLeverType.NORMAL_ORDER.value
                    || OrderLeverType.MARKET_MAKING_ORDER.value == exTradeEntity.getBuyType()) {
                fromType = pairConfig.getBaseAccountNormal();
                toType = pairConfig.getSysBaseAccount();
            } else {
                fromType = bid.getBaseAccountType();
                toType = bid.getFeeAccountType();
            }
        } else {
            // 使用计价货币收手续费
            fromType = pairConfig.getQuoteAccountLock();
            toType = pairConfig.getSysQuoteAccount();
            if (bid.isRelatedSubAccount()) {
                fromType = bid.getQuoteAccountType();
                toType = bid.getFeeAccountType();
            }
        }

        TransferRequest.Builder transD = TransferRequest.newBuilder()
                .setFromUid(exTradeEntity.getBidUserId()).setFromType(fromType)
                .setToUid(feeChainupId).setToType(toType)
                .setAmount(exTradeEntity.getBuyFee().toPlainString())
                .setRefType(pairConfig.getTradeTable())
                .setRefId(exTradeEntity.getId())
                .setMeta(meta)
                .setScene(scene)
                .setOpUid(SYSTEM_CHAINUP_ID)
                .setOpIp(StringUtils.EMPTY);
        if (ask.isRelatedSubAccount()) {
            transD.setToSubType(ask.getBaseSubaccountType());
        }
        if (bid.isRelatedSubAccount()) {
            transD.setFromSubType(bid.getBaseSubaccountType());
        }
        return transD.build();
    }

    private String getFeeTransactionScene(ExOrderEntity exOrder) {
        if (exOrder.getOrderType() == OrderLeverType.MARGIN_ORDER) {
            return TransactionScene.MARGIN_TRADING_FEE.getValue();
        }
        if (exOrder.getOrderType() == OrderLeverType.GRID_ORDER) {
            return TransactionScene.GRID_TRADING_FEE.getValue();
        }
        if (exOrder.getOrderType() == OrderLeverType.GRID_MARGIN_ORDER) {
            return TransactionScene.GRID_MARGIN_TRADING_FEE.getValue();
        }
        return TransactionScene.TRADE.getValue();
    }

    private String getOrderFeeTransactionMeta(ExTradeEntity exTrade, OrderSide side, PairConfigDto pairConfig) {
        String feeCoin = OrderSide.BUY == side ?
                exTrade.getBuyFeeCoin() : exTrade.getSellFeeCoin();
        return feeCoin.equalsIgnoreCase(pairConfig.getQuote()) ?
                "trade.transfer.quoteFeeAmount." + feeCoin : "trade.transfer.baseFeeAmount." + feeCoin;
    }

}
