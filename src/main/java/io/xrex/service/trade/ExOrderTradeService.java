package io.xrex.service.trade;

import io.grpc.stub.StreamObserver;
import io.xrex.dto.CancelOrderIdDto;
import io.xrex.dto.PairConfigDto;
import io.xrex.dto.event.ExTradeDto;
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
import org.springframework.dao.TransientDataAccessException;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static io.xrex.util.XrexConstant.SYSTEM_CHAINUP_ID;

@Slf4j
@Service
@RequiredArgsConstructor
public class ExOrderTradeService {

    private final TransferGrpcService transferGrpcService;

    private final ExOrderDao exOrderDao;
    private final ExTradeDao exTradeDao;
    private final ConfigAccountTypeRepository configAccountTypeRepository;

    @Retryable(retryFor = {TransientDataAccessException.class}, backoff = @Backoff(delay = 500, random = true, multiplier = 2, maxDelay = 3000), listeners = "retryLoggingListener")
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public List<ExTradeEntity> updateExOrderByExTrade(List<ExTradeDto> exTrades, Map<Long, ExOrderEntity> exOrderEntityMap, PairConfigDto pairConfig) {
        List<ExTradeEntity> insertExTradeEntityList = new ArrayList<>();
        List<ExOrderEntity> updateExOrderEntityList = new ArrayList<>();
        for (ExTradeDto exTrade : exTrades) {
            ExOrderEntity bid = exOrderEntityMap.get(exTrade.getBidId());
            ExOrderEntity ask = exOrderEntityMap.get(exTrade.getAskId());

            updateOrder(exTrade, bid, pairConfig, updateExOrderEntityList);
            updateOrder(exTrade, ask, pairConfig, updateExOrderEntityList);
            log.debug("[handleTradeTransfer] exTrade={}, bid={}, ask={}", exTrade, bid, ask);
            insertExTradeEntityList.add(exTrade.toEntity());
        }
        long t1 = System.currentTimeMillis();
        exTradeDao.batchInsert(insertExTradeEntityList, pairConfig.getTradeTable());
        long t2 = System.currentTimeMillis();
        exOrderDao.batchUpsert(updateExOrderEntityList, pairConfig.getOrderTable());
        log.debug("updateExOrderByExTrade insert={}ms, upsert={}ms", (t2 - t1), (System.currentTimeMillis() - t2));
        return insertExTradeEntityList;
    }

    @Async
    @Retryable(retryFor = {TransientDataAccessException.class}, backoff = @Backoff(delay = 500, random = true, multiplier = 2, maxDelay = 3000), listeners = "retryLoggingListener")
    @Transactional
    public void handleRemainOrder(ExOrderEntity bid, ExOrderEntity ask, PairConfigDto pairConfig, StreamObserver<TransferResponse> responseObserver) {
        handleRemainMoney(bid, pairConfig, responseObserver);
        handleRemainMoney(ask, pairConfig, responseObserver);
    }

    @Retryable(retryFor = {TransientDataAccessException.class}, backoff = @Backoff(delay = 500, random = true, multiplier = 2, maxDelay = 3000), listeners = "retryLoggingListener")
    @Transactional
    public List<TransferRequest> handleCancelOrder(CancelOrderIdDto cancelOrderId, List<Long> cancelOrderIds, PairConfigDto pairConfig) {
        List<ExOrderEntity> exOrders = exOrderDao.findPendingCancelByIdIn(cancelOrderIds, pairConfig.getOrderTable());
        if (exOrders.isEmpty()) {
            return new ArrayList<>();
        }

        Map<OrderSide, List<ExOrderEntity>> cancelOrders = new HashMap<>();
        for (ExOrderEntity exOrder : exOrders) {
            List<ExOrderEntity> sideCancelOrders = cancelOrders.getOrDefault(exOrder.getSide(), new ArrayList<>());
            sideCancelOrders.add(exOrder);
            cancelOrders.put(exOrder.getSide(), sideCancelOrders);
        }

        Map<String, Pair<Integer, Integer>> cancelOrderAccountTypes = getCancelOrderAccountTypes(cancelOrderId.orderType(), pairConfig,
                exOrders.getFirst().getQuoteAccountType(), exOrders.getFirst().getBaseAccountType());
        List<TransferRequest> requests = new ArrayList<>();
        for (Map.Entry<OrderSide, List<ExOrderEntity>> entry : cancelOrders.entrySet()) {
            String coinSymbol = OrderSide.BUY == entry.getKey() ? pairConfig.getQuote() : pairConfig.getBase();
            Pair<Integer, Integer> cancelAccountTypes = cancelOrderAccountTypes.get(coinSymbol);
            for (ExOrderEntity cancelOrder : entry.getValue()) {
                TransferRequest.Builder transD = TransferRequest.newBuilder()
                        .setFromUid(cancelOrder.getUserId()).setFromType(cancelAccountTypes.getLeft())
                        .setToUid(cancelOrder.getUserId()).setToType(cancelAccountTypes.getRight())
                        .setAmount(cancelOrder.getRemainAmount().toPlainString())
                        .setRefType(pairConfig.getOrderTable())
                        .setRefId(cancelOrder.getId())
                        .setMeta("fund.transaction.scene.cancel.order")
                        .setScene(TransactionScene.CANCEL_ORDER.value)
                        .setOpUid(SYSTEM_CHAINUP_ID)
                        .setOpIp(StringUtils.EMPTY);

                if (cancelOrder.isRelatedSubAccount()) {
                    if (OrderSide.BUY == cancelOrder.getSide()) {
                        transD.setFromSubType(cancelOrder.getQuoteSubaccountType());
                        transD.setToSubType(cancelOrder.getQuoteSubaccountType());
                    } else {
                        transD.setFromSubType(cancelOrder.getBaseSubaccountType());
                        transD.setToSubType(cancelOrder.getBaseSubaccountType());
                    }
                }
                requests.add(transD.build());
            }
        }
        exOrderDao.batchUpdateCancelStatus(cancelOrderIds, OrderStatus.CANCELED, pairConfig.getOrderTable());
        return requests;
    }

    public Map<Long, ExOrderEntity> queryExOrderByExTrade(List<ExTradeDto> exTrades, PairConfigDto pairConfig) {
        List<Long> orderIds = new ArrayList<>();
        for (ExTradeDto exTrade : exTrades) {
            orderIds.add(exTrade.getBidId());
            orderIds.add(exTrade.getAskId());
        }
        orderIds = orderIds.stream().sorted(Long::compareTo).toList();
        return exOrderDao.findByIdIn(orderIds, pairConfig.getOrderTable()).stream().collect(Collectors.toMap(ExOrderEntity::getId, e -> e));
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

        BigDecimal fee = calculateTradeFee(exTrade, exOrder, pairConfig);
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

        if (exOrder.getSource() == null) {
            exOrder.setSource(OrderSourceType.WEB);
        }
        updateExOrderEntityList.add(exOrder);
    }

    private BigDecimal calculateTradeFee(ExTradeDto exTrade, ExOrderEntity exOrder, PairConfigDto pairConfig) {
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

        return OrderSide.BUY == exOrder.getSide() ? exTrade.getBuyFee() : exTrade.getSellFee();
    }

    private void handleRemainMoney(ExOrderEntity exOrder, PairConfigDto pairConfig, StreamObserver<TransferResponse> responseObserver) {
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
            TransferListRequest remainReturnTransferListRequest = TransferListRequest.newBuilder().addAllRequests(List.of(trans.build())).setRequestId(UUIDv7Generator.generate()).build();
            transferGrpcService.transfer(remainReturnTransferListRequest, responseObserver);
        }
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

    private Map<String, Pair<Integer, Integer>> getCancelOrderAccountTypes(OrderLeverType orderType, PairConfigDto pairConfig,
                                                                           Integer orderQuoteAccountType, Integer orderBaseAccountType) {
        List<String> coinSymbols = List.of(pairConfig.getBase(), pairConfig.getQuote());
        Map<String, Pair<Integer, Integer>> result = new HashMap<>();
        for (String coinSymbol : coinSymbols) {
            Integer fromType;
            Integer toType;
            if (OrderLeverType.GRID_ORDER == orderType || OrderLeverType.GRID_MARGIN_ORDER == orderType) {
                // Grid or Grid Margin
                fromType = pairConfig.getQuote().equalsIgnoreCase(coinSymbol) ? orderQuoteAccountType : orderBaseAccountType;
                AssetType_A_BC assetType = OrderLeverType.GRID_ORDER == orderType ? AssetType_A_BC.U_GRID_NORMAL : AssetType_A_BC.U_GRID_MARGIN_NORMAL;
                ConfigAccountTypeEntity configAccountType = configAccountTypeRepository.findByAssetAAndAssetBcAndCoinSymbol(assetType.account_A,
                        assetType.account_BC, coinSymbol.toUpperCase());
                toType = configAccountType.getAssetType();
            } else if (OrderLeverType.MARGIN_ORDER == orderType) {
                fromType = pairConfig.getQuote().equalsIgnoreCase(coinSymbol) ? orderQuoteAccountType : orderBaseAccountType;
                toType = pairConfig.getQuote().equalsIgnoreCase(coinSymbol) ? pairConfig.getQuoteAccountNormal() : pairConfig.getBaseAccountNormal();
            } else {
                fromType = pairConfig.getQuote().equalsIgnoreCase(coinSymbol) ? pairConfig.getQuoteAccountLock() : pairConfig.getBaseAccountLock();
                toType = pairConfig.getQuote().equalsIgnoreCase(coinSymbol) ? pairConfig.getQuoteAccountNormal() : pairConfig.getBaseAccountNormal();
            }
            result.put(coinSymbol, Pair.of(fromType, toType));
        }
        return result;
    }
}
