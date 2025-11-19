package io.xrex.service;

import io.grpc.stub.StreamObserver;
import io.xrex.dto.PairConfigDto;
import io.xrex.dto.event.TradeEventDto;
import io.xrex.enums.*;
import io.xrex.grpc.TransferListRequest;
import io.xrex.grpc.TransferRequest;
import io.xrex.grpc.TransferResponse;
import io.xrex.persistence.entity.ConfigAccountTypeEntity;
import io.xrex.persistence.entity.ExOrderEntity;
import io.xrex.persistence.entity.ExTradeEntity;
import io.xrex.persistence.repository.ConfigAccountTypeRepository;
import io.xrex.persistence.repository.ExOrderDao;
import io.xrex.persistence.repository.ExTradeDao;
import io.xrex.service.grpc.TransferGrpcService;
import io.xrex.util.UUIDv7Generator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.xrex.util.XrexConstant.SYSTEM_CHAINUP_ID;

@Slf4j
@Service
@RequiredArgsConstructor
public class TradeTransferService {

    @Value("${balance-service.fee-chainup-id}")
    private Integer feeChainupId;
    @Value("${balance-service.mm-chainup-id}")
    private Integer mmChainupId;

    private final ConfigService configService;
    private final TransferGrpcService transferGrpcService;

    private final ExOrderDao exOrderDao;
    private final ExTradeDao exTradeDao;
    private final ConfigAccountTypeRepository configAccountTypeRepository;

    @Transactional
    public void handleTradeTransfer(TradeEventDto tradeEvent, StreamObserver<TransferResponse> responseObserver) {
        PairConfigDto pairConfig = configService.findPairConfigByPair(tradeEvent.getPair());
        ExTradeEntity exTrade = tradeEvent.getTrade().toEntity();

        LocalDateTime handleTime = LocalDateTime.now();
        Duration duration = Duration.between(tradeEvent.getEventTime(),  handleTime);
        log.info("[handleTradeTransfer] timeDiff={}ms", duration.toMillis());
        if (OrderLeverType.MARKET_MAKING_ORDER.value == exTrade.getBuyType()
                || OrderLeverType.MARKET_MAKING_ORDER.value == exTrade.getSellType()) {
            Long mmOrderId = exOrderDao.insert(tradeEvent.toMMExOrderEntity(mmChainupId, handleTime), pairConfig.getOrderTable());
            if (OrderLeverType.MARKET_MAKING_ORDER.value == exTrade.getBuyType()) {
                exTrade.setBidId(mmOrderId);
            } else {
                exTrade.setAskId(mmOrderId);
            }
        }
        List<ExOrderEntity> exOrderEntityList = exOrderDao.findByIdIn(List.of(exTrade.getBidId(), exTrade.getAskId()), pairConfig.getOrderTable());
        ExOrderEntity bid = exOrderEntityList.stream().filter(o -> exTrade.getBidId().equals(o.getId())).findFirst().orElse(null);
        ExOrderEntity ask = exOrderEntityList.stream().filter(o -> exTrade.getAskId().equals(o.getId())).findFirst().orElse(null);
        log.info("[handleTradeTransfer] exOrderEntityList={}", exOrderEntityList);

        updateOrder(exTrade, bid, pairConfig, handleTime);
        updateOrder(exTrade, ask, pairConfig, handleTime);
        log.info("[handleTradeTransfer] bid={}, ask={}", bid, ask);

        Long tradeId = exTradeDao.insert(exTrade, pairConfig.getTradeTable());
        exTrade.setId(tradeId);
        log.info("[handleTradeTransfer] tradeId={}, exTrade={}", tradeId, exTrade);

        List<TransferRequest> requests = new ArrayList<>();
        requests.add(quoteAmountTransfer(exTrade, bid, ask, pairConfig));
        requests.add(baseAmountTransfer(exTrade, bid, ask, pairConfig));
        requests.add(sellFeeTransfer(exTrade, bid, ask, pairConfig));
        requests.add(buyFeeTransfer(exTrade, bid, ask, pairConfig));

        TransferListRequest tradeTransferListRequest = TransferListRequest.newBuilder().addAllRequests(requests).setRequestId(UUIDv7Generator.generate()).build();
        transferGrpcService.transfer(tradeTransferListRequest, responseObserver);
        requests.clear();

        handleRemainMoney(bid, pairConfig, requests);
        handleRemainMoney(ask, pairConfig, requests);
        TransferListRequest remainReturnTransferListRequest = TransferListRequest.newBuilder().addAllRequests(requests).setRequestId(UUIDv7Generator.generate()).build();
        transferGrpcService.transfer(remainReturnTransferListRequest, responseObserver);
    }

    private void updateOrder(ExTradeEntity exTrade, ExOrderEntity exOrder, PairConfigDto pairConfig, LocalDateTime handleTime) {
        if (exOrder == null || OrderLeverType.MARKET_MAKING_ORDER == exOrder.getOrderType()) {
            return;
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
        exOrder.setMtime(handleTime);

        OrderStatus status = exOrder.isFilled(pairConfig) ? OrderStatus.FILLED : OrderStatus.PART_FILLED;
        exOrderDao.updateOrder(exOrder.getId(), status, fee, exTrade.getVolume(), dealMoney, orderAvgPrice, handleTime, pairConfig.getOrderTable());
    }

    private void handleRemainMoney(ExOrderEntity exOrder, PairConfigDto pairConfig, List<TransferRequest> requests) {
        if (exOrder == null || OrderLeverType.MARKET_MAKING_ORDER == exOrder.getOrderType()) {
            return;
        }

        if (exOrder.isMarginOrder()) {
            if (exOrder.isFilled(pairConfig)) {
                exOrderDao.updateStatus(exOrder.getId(), OrderStatus.FILLED, pairConfig.getOrderTable());
            }
            return;
        }

        if (!exOrder.isFilled(pairConfig)) {
            return;
        }

        BigDecimal remainAmount = exOrder.getRemainAmount();
        log.info("[handleRemainMoney] remainAmount={}", remainAmount);
        if (remainAmount.compareTo(BigDecimal.ZERO) > 0) {
            Pair<Integer, Integer> remainAccountTypes = getRemainAccountTypes(exOrder, pairConfig);
            TransferRequest.Builder trans = TransferRequest.newBuilder()
                    .setFromUid(exOrder.getUserId()).setFromType(remainAccountTypes.getLeft())
                    .setToUid(exOrder.getUserId()).setToType(remainAccountTypes.getRight())
                    .setAmount(remainAmount.toPlainString())
                    .setRefType(pairConfig.getOrderTable())
                    .setRefId(exOrder.getId())
                    .setMeta(getRemainMeta(exOrder))
                    .setScene(TransactionScene.CANCEL_TRADE.value)
                    .setOpUid(SYSTEM_CHAINUP_ID)
                    .setOpIp(StringUtils.EMPTY);
            if (exOrder.isRelatedSubAccount()) {
                String subAccountType = OrderSide.BUY == exOrder.getSide() ? exOrder.getQuoteSubaccountType() : exOrder.getBaseSubaccountType();
                trans.setFromSubType(subAccountType);
                trans.setToSubType(subAccountType);
            }
            requests.add(trans.build());
            log.info("[handleRemainMoney] trans={}", trans.build());
        }
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

    private String getRemainMeta(ExOrderEntity exOrder) {
        String meta;
        if (exOrder.getSide() == OrderSide.BUY) {
            if (exOrder.isMarketOrder()) {
                meta = "order.unlock.returnMarketOrderQuoteAmount";
            } else {
                meta = "order.unlock.remainQuoteAmount";
            }
        } else {
            if (exOrder.isMarketOrder()) {
                meta = "order.unlock.returnMarketOrderBaseAmount";
            } else {
                meta = "order.unlock.remainBaseAmount";
            }
        }
        return meta;
    }

    private Pair<Integer, Integer> getRemainAccountTypes(ExOrderEntity exOrder, PairConfigDto pairConfig) {
        Integer fromType;
        Integer toType;
        if (OrderSide.BUY == exOrder.getSide()) {
            if (exOrder.isGridOrder() || exOrder.isGridMarginOrder()) {
                // Grid or Grid Margin
                fromType = exOrder.getQuoteAccountType();
                AssetType_A_BC assetType = exOrder.isGridOrder() ? AssetType_A_BC.U_GRID_NORMAL : AssetType_A_BC.U_GRID_MARGIN_NORMAL;

                String quoteCoin = "'" + pairConfig.getQuote().toUpperCase() + "'";
                ConfigAccountTypeEntity configAccountType = configAccountTypeRepository.findByAssetAAndAssetBcAndCoinSymbol(assetType.account_A,
                        assetType.account_BC, quoteCoin);
                toType = configAccountType.getAssetType();
            } else if (exOrder.isMarginOrder()) {
                fromType = exOrder.getQuoteAccountType();
                toType = pairConfig.getQuoteAccountNormal();
            } else if (exOrder.isConvertOrder() && exOrder.getQuoteAccountType() != null) {
                fromType = exOrder.getQuoteAccountType();
                toType = pairConfig.getQuoteAccountNormal();
            } else {
                fromType = pairConfig.getQuoteAccountLock();
                toType = pairConfig.getQuoteAccountNormal();
            }
        } else {
            if (exOrder.isGridOrder() || exOrder.isGridMarginOrder()) {
                // Grid
                fromType = exOrder.getBaseAccountType();
                AssetType_A_BC assetType = exOrder.isGridOrder() ? AssetType_A_BC.U_GRID_NORMAL : AssetType_A_BC.U_GRID_MARGIN_NORMAL;

                String baseCoin = "'" + pairConfig.getBase().toUpperCase() + "'";
                ConfigAccountTypeEntity configAccountType = configAccountTypeRepository.findByAssetAAndAssetBcAndCoinSymbol(assetType.account_A,
                        assetType.account_BC, baseCoin);
                toType = configAccountType.getAssetType();
            } else if (exOrder.isMarginOrder()) {
                fromType = exOrder.getBaseAccountType();
                toType = pairConfig.getBaseAccountNormal();
            } else if (exOrder.isConvertOrder() && exOrder.getBaseAccountType() != null) {
                fromType = exOrder.getBaseAccountType();
                toType = pairConfig.getBaseAccountNormal();
            } else {
                fromType = pairConfig.getBaseAccountLock();
                toType = pairConfig.getBaseAccountNormal();
            }
        }
        return Pair.of(fromType, toType);
    }
}
