package io.xrex.persistence.repository;

import io.xrex.enums.OrderStatus;
import io.xrex.persistence.entity.ExOrderEntity;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class ExOrderDao {

    private final JdbcTemplate jdbcTemplate;

    @Transactional
    public List<Long> batchInsert(List<ExOrderEntity> orders, String tableName) {
        isValidTableName(tableName);
        String sql = "INSERT INTO " + tableName +
                """
                         (user_id, side, price, volume, fee_account_type, fee_deduct_type,
                        fee_rate_maker, fee_rate_taker, fee, fee_coin_rate, deal_volume, deal_money, avg_price, locked_amount,
                        status, type, ctime, mtime, source, order_type, stop_price, stop_price_direction, quote_account_type,
                        quote_subaccount_type, base_account_type, base_subaccount_type, margin_trade_id, margin_direction, bot_id) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """;

        List<Long> generatedIds = new ArrayList<>();
        for (ExOrderEntity order : orders) {
            KeyHolder keyHolder = new GeneratedKeyHolder();
            jdbcTemplate.update(connection -> {
                PreparedStatement ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                ps.setInt(1, order.getUserId());
                ps.setString(2, order.getSide().value);
                ps.setBigDecimal(3, order.getPrice());
                ps.setBigDecimal(4, order.getVolume());
                ps.setInt(5, order.getFeeAccountType());
                ps.setInt(6, order.getFeeDeductType().value);
                ps.setDouble(7, order.getFeeRateMaker());
                ps.setDouble(8, order.getFeeRateTaker());
                ps.setBigDecimal(9, order.getFee());
                ps.setDouble(10, order.getFeeCoinRate());
                ps.setBigDecimal(11, order.getDealVolume());
                ps.setBigDecimal(12, order.getDealMoney());
                ps.setBigDecimal(13, order.getAvgPrice());
                ps.setBigDecimal(14, order.getLockedAmount());
                ps.setByte(15, order.getStatus().value);
                ps.setByte(16, order.getType().value);
                ps.setTimestamp(17, Timestamp.valueOf(order.getCtime()));
                ps.setTimestamp(18, Timestamp.valueOf(order.getMtime()));
                ps.setByte(19, order.getSource().value);
                ps.setByte(20, order.getOrderType().value);
                ps.setBigDecimal(21, order.getStopPrice());
                ps.setByte(22, order.getStopPriceDirection().value);
                ps.setInt(23, order.getQuoteAccountType());
                ps.setString(24, order.getQuoteSubaccountType());
                ps.setInt(25, order.getBaseAccountType());
                ps.setString(26, order.getBaseSubaccountType());
                ps.setLong(27, order.getMarginTradeId());
                ps.setString(28, order.getMarginDirection());
                ps.setLong(29, order.getBotId());
                return ps;
            }, keyHolder);
            generatedIds.add(keyHolder.getKey().longValue());
        }
        return generatedIds;
    }

    public ExOrderEntity findById(Long id, String tableName) {
        isValidTableName(tableName);
        String sql = "SELECT * FROM " + tableName + " WHERE id = ?";
        return jdbcTemplate.queryForObject(sql, new Object[]{id}, new ExOrderEntityRowMapper());
    }

    public ExOrderEntity findLatestOrder(String tableName) {
        isValidTableName(tableName);
        String sql = "SELECT * FROM " + tableName + " ORDER BY id DESC LIMIT 1";
        return jdbcTemplate.queryForObject(sql, new ExOrderEntityRowMapper());
    }

    public List<ExOrderEntity> findByIdIn(List<Long> ids, String tableName) {
        isValidTableName(tableName);
        if (ids == null || ids.isEmpty()) {
            return new ArrayList<>();
        }
        String inClause = String.join(",", java.util.Collections.nCopies(ids.size(), "?"));
        String sql = "SELECT * FROM " + tableName + " WHERE id IN (" + inClause + ")";
        return jdbcTemplate.query(sql, ids.toArray(), new ExOrderEntityRowMapper());
    }

    @Transactional
    public List<Long> batchUpsert(List<ExOrderEntity> orders, String tableName) {
        isValidTableName(tableName);
        if (orders == null || orders.isEmpty()) {
            return new ArrayList<>();
        }
        String sql = "INSERT INTO " + tableName + " (id, user_id, side, price, volume, fee_account_type, fee_deduct_type, " +
                "fee_rate_maker, fee_rate_taker, fee, fee_coin_rate, deal_volume, deal_money, avg_price, locked_amount, " +
                "status, type, ctime, mtime, source, order_type, stop_price, stop_price_direction, quote_account_type, " +
                "quote_subaccount_type, base_account_type, base_subaccount_type, margin_trade_id, margin_direction, bot_id) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE " +
                "deal_volume = VALUES(deal_volume), " +
                "deal_money = VALUES(deal_money), " +
                "avg_price = VALUES(avg_price), " +
                "mtime = VALUES(mtime)";

        List<Long> resultIds = new ArrayList<>();
        for (ExOrderEntity order : orders) {
            KeyHolder keyHolder = new GeneratedKeyHolder();
            jdbcTemplate.update(connection -> {
                PreparedStatement ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                ps.setLong(1, order.getId());
                ps.setInt(2, order.getUserId());
                ps.setString(3, order.getSide().name());
                ps.setBigDecimal(4, order.getPrice());
                ps.setBigDecimal(5, order.getVolume());
                ps.setInt(6, order.getFeeAccountType());
                ps.setInt(7, order.getFeeDeductType().value);
                ps.setDouble(8, order.getFeeRateMaker());
                ps.setDouble(9, order.getFeeRateTaker());
                ps.setBigDecimal(10, order.getFee());
                ps.setDouble(11, order.getFeeCoinRate());
                ps.setBigDecimal(12, order.getDealVolume());
                ps.setBigDecimal(13, order.getDealMoney());
                ps.setBigDecimal(14, order.getAvgPrice());
                ps.setBigDecimal(15, order.getLockedAmount());
                ps.setByte(16, (byte) order.getStatus().value);
                ps.setByte(17, (byte) order.getType().value);
                ps.setTimestamp(18, Timestamp.valueOf(order.getCtime()));
                ps.setTimestamp(19, Timestamp.valueOf(order.getMtime()));
                ps.setByte(20, (byte) order.getSource().value);
                ps.setByte(21, (byte) order.getOrderType().value);
                ps.setBigDecimal(22, order.getStopPrice());
                ps.setByte(23, (byte) order.getStopPriceDirection().value);
                ps.setInt(24, order.getQuoteAccountType());
                ps.setString(25, order.getQuoteSubaccountType());
                ps.setInt(26, order.getBaseAccountType());
                ps.setString(27, order.getBaseSubaccountType());
                ps.setLong(28, order.getMarginTradeId());
                ps.setString(29, order.getMarginDirection());
                ps.setLong(30, order.getBotId());
                return ps;
            }, keyHolder);
            resultIds.add(keyHolder.getKey().longValue());
        }
        return resultIds;
    }

    public int updateStatus(Long id, OrderStatus newStatus, String tableName) {
        isValidTableName(tableName);
        String sql = "UPDATE " + tableName + " SET status = ?, mtime = ? WHERE id = ?  AND status IN (0,1,3)";
        return jdbcTemplate.update(sql, newStatus.value, Timestamp.valueOf(java.time.LocalDateTime.now()), id);
    }

    public int batchUpdateCancelStatus(List<Long> ids, OrderStatus newStatus, String tableName) {
        isValidTableName(tableName);
        String inClause = String.join(",", java.util.Collections.nCopies(ids.size(), "?"));
        String sql = "UPDATE " + tableName + " SET status = ?, mtime = ? WHERE id IN (" + inClause + ") AND status = 5";
        return jdbcTemplate.update(sql, newStatus.value, Timestamp.valueOf(java.time.LocalDateTime.now()), ids.toArray());
    }

    private void isValidTableName(String tableName) {
        if (tableName == null || !tableName.matches("^ex_order_[a-zA-Z0-9_]+$")) {
            throw new IllegalArgumentException("Invalid table name");
        }
    }

}