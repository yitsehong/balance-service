package io.xrex.service.trade;

import io.xrex.dto.PairConfigDto;
import io.xrex.dto.event.ExTradeDto;
import io.xrex.enums.OrderLeverType;
import io.xrex.enums.OrderSide;
import io.xrex.enums.OrderStatus;
import io.xrex.persistence.entity.ExOrderEntity;
import io.xrex.persistence.entity.ExTradeEntity;
import io.xrex.persistence.repository.ExOrderDao;
import io.xrex.persistence.repository.ExTradeDao;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class ExOrderTradeService {

    private final ExOrderDao exOrderDao;
    private final ExTradeDao exTradeDao;

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
        exTradeDao.batchInsert(insertExTradeEntityList, pairConfig.getTradeTable());
        exOrderDao.batchUpsert(updateExOrderEntityList, pairConfig.getOrderTable());
        return insertExTradeEntityList;
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
}
