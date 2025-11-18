package io.xrex.persistence.repository;

import io.xrex.enums.*;
import io.xrex.persistence.entity.ExOrderEntity;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ExOrderEntityRowMapper implements RowMapper<ExOrderEntity> {

    @Override
    public ExOrderEntity mapRow(ResultSet rs, int rowNum) throws SQLException {
        return ExOrderEntity.builder()
                .id(rs.getLong("id"))
                .userId(rs.getInt("user_id"))
                .side(OrderSide.fromValue(rs.getString("side")))
                .price(rs.getBigDecimal("price"))
                .volume(rs.getBigDecimal("volume"))
                .feeAccountType(rs.getInt("fee_account_type"))
                .feeDeductType(FeeDeductType.fromValue(rs.getInt("fee_deduct_type")))
                .feeRateMaker(rs.getDouble("fee_rate_maker"))
                .feeRateTaker(rs.getDouble("fee_rate_taker"))
                .fee(rs.getBigDecimal("fee"))
                .feeCoinRate(rs.getDouble("fee_coin_rate"))
                .dealVolume(rs.getBigDecimal("deal_volume"))
                .dealMoney(rs.getBigDecimal("deal_money"))
                .avgPrice(rs.getBigDecimal("avg_price"))
                .lockedAmount(rs.getBigDecimal("locked_amount"))
                .status(OrderStatus.fromValue(rs.getInt("status")))
                .type(OrderType.fromValue(rs.getByte("type")))
                .ctime(rs.getTimestamp("ctime").toLocalDateTime())
                .mtime(rs.getTimestamp("mtime").toLocalDateTime())
                .source(OrderSourceType.fromValue(rs.getByte("source")))
                .orderType(OrderLeverType.fromValue(rs.getByte("order_type")))
                .stopPrice(rs.getBigDecimal("stop_price"))
                .stopPriceDirection(StopPriceDirection.fromValue(rs.getByte("stop_price_direction")))
                .quoteAccountType(rs.getInt("quote_account_type"))
                .quoteSubaccountType(rs.getString("quote_subaccount_type"))
                .baseAccountType(rs.getInt("base_account_type"))
                .baseSubaccountType(rs.getString("base_subaccount_type"))
                .marginTradeId(rs.getLong("margin_trade_id"))
                .marginDirection(rs.getString("margin_direction"))
                .botId(rs.getLong("bot_id"))
                .build();
    }
}
