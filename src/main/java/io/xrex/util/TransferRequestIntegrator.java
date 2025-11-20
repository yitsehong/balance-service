package io.xrex.util;

import io.xrex.dto.PairConfigDto;
import io.xrex.enums.OrderLeverType;
import io.xrex.enums.OrderSide;
import io.xrex.enums.TransactionScene;
import io.xrex.grpc.TransferListRequest;
import io.xrex.grpc.TransferRequest;
import io.xrex.persistence.entity.ExOrderEntity;
import io.xrex.persistence.entity.ExTradeEntity;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.List;

import static io.xrex.util.XrexConstant.SYSTEM_CHAINUP_ID;

public class TransferRequestIntegrator {
    @Getter
    private final TransferListRequest tradeTransferListRequest;

    public TransferRequestIntegrator(ExTradeEntity exTrade, ExOrderEntity bid, ExOrderEntity ask, PairConfigDto pairConfig, Integer feeChainupId) {
        List<TransferRequest> requests = new LinkedList<>();
        requests.add(this.quoteAmountTransfer(exTrade, bid, ask, pairConfig));
        requests.add(this.baseAmountTransfer(exTrade, bid, ask, pairConfig));
        requests.add(this.sellFeeTransfer(exTrade, bid, ask, pairConfig, feeChainupId));
        requests.add(this.buyFeeTransfer(exTrade, bid, ask, pairConfig, feeChainupId));
        tradeTransferListRequest = TransferListRequest.newBuilder().addAllRequests(requests).setRequestId(exTrade.getTradeNonce()).build();
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

    private TransferRequest sellFeeTransfer(ExTradeEntity exTradeEntity, ExOrderEntity bid, ExOrderEntity ask, PairConfigDto pairConfig, Integer feeChainupId) {
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

    private TransferRequest buyFeeTransfer(ExTradeEntity exTradeEntity, ExOrderEntity bid, ExOrderEntity ask, PairConfigDto pairConfig, Integer feeChainupId) {
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
