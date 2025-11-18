package io.xrex.service;

import io.xrex.dto.PairConfigDto;
import io.xrex.dto.event.TradeEventDto;
import io.xrex.enums.OrderLeverType;
import io.xrex.enums.OrderSide;
import io.xrex.enums.TransactionScene;
import io.xrex.grpc.TransferListRequest;
import io.xrex.grpc.TransferRequest;
import io.xrex.persistence.entity.ExOrderEntity;
import io.xrex.persistence.entity.ExTradeEntity;
import io.xrex.persistence.repository.ExOrderDao;
import io.xrex.persistence.repository.ExTradeDao;
import io.xrex.util.UUIDv7Generator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
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
    private final ExOrderDao exOrderDao;
    private final ExTradeDao exTradeDao;

    @Transactional
    public TransferListRequest handleTradeTransfer(TradeEventDto tradeEvent) {
        PairConfigDto pairConfig = configService.findPairConfigByPair(tradeEvent.getPair());

        List<ExOrderEntity> orders = new ArrayList<>();
        ExTradeEntity exTrade = tradeEvent.getTrade().toEntity();

        ExOrderEntity exOrder;
        ExOrderEntity counterExOrder = null;
        if (mmChainupId.equals(tradeEvent.getCounterPartyChainupId())) {
            orders.add(tradeEvent.toMMExOrderEntity(mmChainupId));
            exOrder = exOrderDao.findById(tradeEvent.getOrderId(), pairConfig.getOrderTable());
        } else {
            List<ExOrderEntity> exOrderEntityList = exOrderDao.findByIdIn(List.of(exTrade.getBidId(), exTrade.getAskId()), pairConfig.getOrderTable());
            exOrder = exOrderEntityList.stream().filter(o -> tradeEvent.getOrderId().equals(o.getId())).findFirst().orElse(null);
            counterExOrder = exOrderEntityList.stream().filter(o -> !tradeEvent.getOrderId().equals(o.getId())).findFirst().orElse(null);
        }

        updateOrder(exTrade, exOrder);
        updateOrder(exTrade, counterExOrder);
        orders.add(exOrder);
        orders.add(counterExOrder);

        Long tradeId = exTradeDao.insert(exTrade, pairConfig.getTradeTable());
        exTrade.setId(tradeId);
        exOrderDao.batchUpsert(orders, pairConfig.getOrderTable());

        List<TransferRequest> requests = new ArrayList<>();
        requests.add(quoteAmountTransfer(exTrade, exOrder, pairConfig));
        requests.add(baseAmountTransfer(exTrade, exOrder, pairConfig));
        requests.add(sellFeeTransfer(exTrade, exOrder, pairConfig));
        requests.add(buyFeeTransfer(exTrade, exOrder, pairConfig));
        return TransferListRequest.newBuilder().addAllRequests(requests).setRequestId(UUIDv7Generator.generate()).build();
    }

    private void updateOrder(ExTradeEntity exTrade, ExOrderEntity exOrder) {
        if (exOrder == null) {
            return;
        }

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
    }

    private TransferRequest quoteAmountTransfer(ExTradeEntity exTradeEntity, ExOrderEntity exOrder, PairConfigDto pairConfig) {
        // buyer.quoteLock -> seller.quoteNormal  quoteAmount
        Integer quoteAccountType = exOrder.getQuoteAccountType();
        Integer fromType = OrderLeverType.NORMAL_ORDER.value == exTradeEntity.getBuyType() ?
                pairConfig.getQuoteAccountLock() : quoteAccountType;

        Integer toType = OrderLeverType.NORMAL_ORDER.value == exTradeEntity.getSellType() ?
                pairConfig.getQuoteAccountNormal() : quoteAccountType;

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
        if (exOrder.isRelatedSubAccount()) {
            if (OrderSide.BUY == exOrder.getSide()) {
                transA.setFromSubType(exOrder.getQuoteSubaccountType());
            } else {
                transA.setToSubType(exOrder.getQuoteSubaccountType());
            }
        }
        return transA.build();
    }

    private TransferRequest baseAmountTransfer(ExTradeEntity exTradeEntity, ExOrderEntity exOrder, PairConfigDto pairConfig) {
        // seller.baseLock -> buyer.baseNormal  baseVolume
        Integer fromType = OrderLeverType.NORMAL_ORDER.value == exTradeEntity.getSellType() ?
                pairConfig.getBaseAccountLock() : exOrder.getBaseAccountType();
        Integer toType = OrderLeverType.NORMAL_ORDER.value == exTradeEntity.getBuyType() ?
                pairConfig.getBaseAccountNormal() : exOrder.getBaseAccountType();

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
        if (exOrder.isRelatedSubAccount()) {
            if (OrderSide.BUY == exOrder.getSide()) {
                transB.setToSubType(exOrder.getBaseSubaccountType());
            } else {
                transB.setFromSubType(exOrder.getBaseSubaccountType());
            }
        }
        return transB.build();
    }

    private TransferRequest sellFeeTransfer(ExTradeEntity exTradeEntity, ExOrderEntity exOrder, PairConfigDto pairConfig) {
        // 收seller手续费：
        // inner: seller.quoteNormal -> exchange.quoteFee  quoteFeeAmount
        // outer: seller.quoteLock -> exchange.quoteFee  quoteFeeAmount
        Integer fromType;
        Integer toType;
        String scene = getFeeTransactionScene(exOrder);
        String meta = getOrderFeeTransactionMeta(exTradeEntity, OrderSide.SELL, pairConfig);
        if (OrderLeverType.NORMAL_ORDER.value == exTradeEntity.getSellType()) {
            fromType = pairConfig.getQuoteAccountNormal();
            toType = pairConfig.getSysQuoteAccount();
        } else {
            fromType = exOrder.getQuoteAccountType();
            toType = exOrder.getFeeAccountType();
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

        if (exOrder.isRelatedSubAccount()) {
            if (OrderSide.BUY == exOrder.getSide()) {
                transC.setToSubType(exOrder.getQuoteSubaccountType());
            } else {
                transC.setFromSubType(exOrder.getQuoteSubaccountType());
            }
        }
        return transC.build();
    }

    private TransferRequest buyFeeTransfer(ExTradeEntity exTradeEntity, ExOrderEntity exOrder, PairConfigDto pairConfig) {
        // 收buyer手续费：
        // inner: buyer.baseNormal -> exchange.baseFee  baseFeeVolume
        // outer: buyer.baseLock -> exchange.baseFee  baseFeeVolume
        Integer fromType;
        Integer toType;
        String scene = getFeeTransactionScene(exOrder);
        String meta = getOrderFeeTransactionMeta(exTradeEntity, OrderSide.BUY, pairConfig);
        if (exOrder.isInnerFeeDeduct()) {
            if (exTradeEntity.getBuyType() == OrderLeverType.NORMAL_ORDER.value) {
                fromType = pairConfig.getBaseAccountNormal();
                toType = pairConfig.getSysBaseAccount();
            } else {
                fromType = exOrder.getBaseAccountType();
                toType = exOrder.getFeeAccountType();
            }
        } else {
            // 使用计价货币收手续费
            fromType = pairConfig.getQuoteAccountLock();
            toType = pairConfig.getSysQuoteAccount();
            if (exOrder.isRelatedSubAccount()
                    || OrderLeverType.MARKET_MAKING_ORDER.value == exTradeEntity.getBuyType()) {
                fromType = exOrder.getQuoteAccountType();
                toType = exOrder.getFeeAccountType();
            }
        }

        TransferRequest.Builder transD = TransferRequest.newBuilder()
                .setFromUid(exTradeEntity.getBidUserId()).setFromType(fromType)
                .setToUid(feeChainupId).setToType(toType)
                .setAmount(exTradeEntity.getSellFee().toPlainString())
                .setRefType(pairConfig.getTradeTable())
                .setRefId(exTradeEntity.getId())
                .setMeta(meta)
                .setScene(scene)
                .setOpUid(SYSTEM_CHAINUP_ID)
                .setOpIp(StringUtils.EMPTY);

        if (exOrder.isRelatedSubAccount()) {
            if (OrderSide.BUY == exOrder.getSide()) {
                transD.setFromSubType(exOrder.getBaseSubaccountType());
            } else {
                transD.setToSubType(exOrder.getBaseSubaccountType());
            }
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
