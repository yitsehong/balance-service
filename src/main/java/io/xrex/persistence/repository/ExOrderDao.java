package io.xrex.persistence.repository;

import io.xrex.enums.OrderStatus;
import io.xrex.persistence.entity.ExOrderEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@Repository
@RequiredArgsConstructor
public class ExOrderDao {

    private final JdbcTemplate jdbcTemplate;

    @Transactional
    public List<Long> batchInsert(List<ExOrderEntity> orders, String tableName) {
        isValidTableName(tableName);
        if (orders == null || orders.isEmpty()) {
            return new ArrayList<>();
        }
        String sql = "INSERT INTO " + tableName +
                """
                         (user_id, side, price, volume, fee_account_type, fee_deduct_type,
                        fee_rate_maker, fee_rate_taker, fee, fee_coin_rate, deal_volume, deal_money, avg_price, locked_amount,
                        status, type, ctime, mtime, source, order_type, stop_price, stop_price_direction, quote_account_type,
                        quote_subaccount_type, base_account_type, base_subaccount_type, margin_trade_id, margin_direction, bot_id) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """;

        List<Long> generatedIds = new ArrayList<>();
        jdbcTemplate.execute((java.sql.Connection con) -> {
            try (PreparedStatement ps = con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
                for (ExOrderEntity order : orders) {
                    if (order.getUserId() != null) ps.setInt(1, order.getUserId());
                    else ps.setNull(1, Types.INTEGER);
                    if (order.getSide() != null) ps.setString(2, order.getSide().name());
                    else ps.setNull(2, Types.VARCHAR);
                    if (order.getPrice() != null) ps.setBigDecimal(3, order.getPrice());
                    else ps.setNull(3, Types.DECIMAL);
                    if (order.getVolume() != null) ps.setBigDecimal(4, order.getVolume());
                    else ps.setNull(4, Types.DECIMAL);
                    if (order.getFeeAccountType() != null) ps.setInt(5, order.getFeeAccountType());
                    else ps.setNull(5, Types.INTEGER);
                    if (order.getFeeDeductType() != null) ps.setInt(6, order.getFeeDeductType().value);
                    else ps.setNull(6, Types.INTEGER);
                    if (order.getFeeRateMaker() != null) ps.setDouble(7, order.getFeeRateMaker());
                    else ps.setNull(7, Types.DOUBLE);
                    if (order.getFeeRateTaker() != null) ps.setDouble(8, order.getFeeRateTaker());
                    else ps.setNull(8, Types.DOUBLE);
                    if (order.getFee() != null) ps.setBigDecimal(9, order.getFee());
                    else ps.setNull(9, Types.DECIMAL);
                    if (order.getFeeCoinRate() != null) ps.setDouble(10, order.getFeeCoinRate());
                    else ps.setNull(10, Types.DOUBLE);
                    if (order.getDealVolume() != null) ps.setBigDecimal(11, order.getDealVolume());
                    else ps.setNull(11, Types.DECIMAL);
                    if (order.getDealMoney() != null) ps.setBigDecimal(12, order.getDealMoney());
                    else ps.setNull(12, Types.DECIMAL);
                    if (order.getAvgPrice() != null) ps.setBigDecimal(13, order.getAvgPrice());
                    else ps.setNull(13, Types.DECIMAL);
                    if (order.getLockedAmount() != null) ps.setBigDecimal(14, order.getLockedAmount());
                    else ps.setNull(14, Types.DECIMAL);
                    if (order.getStatus() != null) ps.setByte(15, order.getStatus().value);
                    else ps.setNull(15, Types.TINYINT);
                    if (order.getType() != null) ps.setByte(16, order.getType().value);
                    else ps.setNull(16, Types.TINYINT);
                    if (order.getCtime() != null) ps.setTimestamp(17, Timestamp.valueOf(order.getCtime()));
                    else ps.setNull(17, Types.TIMESTAMP);
                    if (order.getMtime() != null) ps.setTimestamp(18, Timestamp.valueOf(order.getMtime()));
                    else ps.setNull(18, Types.TIMESTAMP);
                    if (order.getSource() != null) ps.setByte(19, order.getSource().value);
                    else ps.setNull(19, Types.TINYINT);
                    if (order.getOrderType() != null) ps.setByte(20, order.getOrderType().value);
                    else ps.setNull(20, Types.TINYINT);
                    if (order.getStopPrice() != null) ps.setBigDecimal(21, order.getStopPrice());
                    else ps.setNull(21, Types.DECIMAL);
                    if (order.getStopPriceDirection() != null) ps.setByte(22, order.getStopPriceDirection().value);
                    else ps.setNull(22, Types.TINYINT);
                    if (order.getQuoteAccountType() != null) ps.setInt(23, order.getQuoteAccountType());
                    else ps.setNull(23, Types.INTEGER);
                    if (order.getQuoteSubaccountType() != null) ps.setString(24, order.getQuoteSubaccountType());
                    else ps.setNull(24, Types.VARCHAR);
                    if (order.getBaseAccountType() != null) ps.setInt(25, order.getBaseAccountType());
                    else ps.setNull(25, Types.INTEGER);
                    if (order.getBaseSubaccountType() != null) ps.setString(26, order.getBaseSubaccountType());
                    else ps.setNull(26, Types.VARCHAR);
                    if (order.getMarginTradeId() != null) ps.setLong(27, order.getMarginTradeId());
                    else ps.setNull(27, Types.BIGINT);
                    if (order.getMarginDirection() != null) ps.setString(28, order.getMarginDirection());
                    else ps.setNull(28, Types.VARCHAR);
                    if (order.getBotId() != null) ps.setLong(29, order.getBotId());
                    else ps.setNull(29, Types.BIGINT);
                    ps.addBatch();
                }
                ps.executeBatch();
                try (java.sql.ResultSet rs = ps.getGeneratedKeys()) {
                    while (rs.next()) {
                        generatedIds.add(rs.getLong(1));
                    }
                }
            }
            return null;
        });
        return generatedIds;
    }

    public void batchUpsert(List<ExOrderEntity> updateOrders, String tableName) {
        isValidTableName(tableName);
        String valuesSql = IntStream.range(0, updateOrders.size())
                .mapToObj(i -> "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                .collect(Collectors.joining(", "));

        String sql = "INSERT INTO " + tableName +
                """
                         (id, user_id, side, price, volume, fee_account_type, fee_deduct_type,
                        fee_rate_maker, fee_rate_taker, fee, fee_coin_rate, deal_volume, deal_money, avg_price, locked_amount,
                        status, type, ctime, mtime, source, order_type, stop_price, stop_price_direction, quote_account_type,
                        quote_subaccount_type, base_account_type, base_subaccount_type, margin_trade_id, margin_direction, bot_id) 
                        VALUES 
                        """ + valuesSql + """
                    ON DUPLICATE KEY UPDATE
                    status = IF(VALUES(status) IS NOT NULL, VALUES(status), status),
                    fee = IF(VALUES(fee) IS NOT NULL, VALUES(fee), fee),
                    deal_volume = IF(VALUES(deal_volume) IS NOT NULL, VALUES(deal_volume), deal_volume),
                    deal_money = IF(VALUES(deal_money) IS NOT NULL, VALUES(deal_money), deal_money),
                    avg_price = IF(VALUES(avg_price) IS NOT NULL, VALUES(avg_price), avg_price),
                    mtime = IF(VALUES(mtime) IS NOT NULL, VALUES(mtime), mtime)
                """;
        jdbcTemplate.update(con -> {
            PreparedStatement ps = con.prepareStatement(sql);
            int idx = 1;
            for (ExOrderEntity order : updateOrders) {
                ps.setLong(idx++, order.getId());
                ps.setInt(idx++, order.getUserId());
                ps.setString(idx++, order.getSide().name());
                ps.setBigDecimal(idx++, order.getPrice());
                ps.setBigDecimal(idx++, order.getVolume());
                ps.setInt(idx++, order.getFeeAccountType());
                ps.setInt(idx++, order.getFeeDeductType().value);
                ps.setDouble(idx++, order.getFeeRateMaker());
                ps.setDouble(idx++, order.getFeeRateTaker());
                ps.setBigDecimal(idx++, order.getFee());
                ps.setDouble(idx++, order.getFeeCoinRate());
                ps.setBigDecimal(idx++, order.getDealVolume());
                ps.setBigDecimal(idx++, order.getDealMoney());
                ps.setBigDecimal(idx++, order.getAvgPrice());
                ps.setBigDecimal(idx++, order.getLockedAmount());
                ps.setByte(idx++, order.getStatus().value);
                ps.setByte(idx++, order.getType().value);
                ps.setTimestamp(idx++, Timestamp.valueOf(order.getCtime()));
                ps.setTimestamp(idx++, Timestamp.valueOf(order.getMtime()));
                ps.setByte(idx++, order.getSource().value);
                ps.setByte(idx++, order.getOrderType().value);
                ps.setBigDecimal(idx++, order.getStopPrice());
                if (order.getStopPriceDirection() != null) ps.setByte(idx++, order.getStopPriceDirection().value);
                else ps.setNull(idx++, Types.TINYINT);
                ps.setInt(idx++, order.getQuoteAccountType());
                ps.setString(idx++, order.getQuoteSubaccountType());
                ps.setInt(idx++, order.getBaseAccountType());
                ps.setString(idx++, order.getBaseSubaccountType());
                ps.setLong(idx++, order.getMarginTradeId());
                ps.setString(idx++, order.getMarginDirection());
                ps.setLong(idx++, order.getBotId());
            }

            return ps;
        });
    }

    public ExOrderEntity findById(Long id, String tableName) {
        isValidTableName(tableName);
        String sql = "SELECT * FROM " + tableName + " WHERE id = ?";
        return jdbcTemplate.queryForObject(sql, new Object[]{id}, new ExOrderEntityRowMapper());
    }

    public ExOrderEntity findLatestOrder(String tableName) {
        isValidTableName(tableName);
        String sql = "SELECT * FROM " + tableName + " ORDER BY id DESC LIMIT 1";
        try {
            return jdbcTemplate.queryForObject(sql, new ExOrderEntityRowMapper());
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return null;
        }
    }

    public List<ExOrderEntity> findByIdIn(List<Long> ids, String tableName) {
        isValidTableName(tableName);
        if (ids == null || ids.isEmpty()) {
            return new ArrayList<>();
        }

        NamedParameterJdbcTemplate namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
        String sql = "SELECT * FROM " + tableName + " WHERE id >= " + ids.getFirst() + " AND id IN (:ids)";
        ExOrderEntityRowMapper rowMapper = new ExOrderEntityRowMapper(); // Re-use the mapper
        List<ExOrderEntity> result = new ArrayList<>();

        // Partition the list manually to avoid issues with very large IN clauses
        int batchSize = 1000;
        for (int i = 0; i < ids.size(); i += batchSize) {
            int end = Math.min(i + batchSize, ids.size());
            List<Long> batch = ids.subList(i, end);
            Map<String, List<Long>> params = Collections.singletonMap("ids", batch);
            result.addAll(namedParameterJdbcTemplate.query(sql, params, rowMapper));
        }

        return result;
    }

    public List<ExOrderEntity> findByUserIdAndStatus(Integer chainupId, OrderStatus status, String tableName) {
        isValidTableName(tableName);
        String sql = String.format("SELECT * FROM %s WHERE user_id = ? AND status = ?", tableName);
        return jdbcTemplate.query(sql, new Object[]{chainupId, status.value}, new ExOrderEntityRowMapper());
    }

    public List<ExOrderEntity> findPendingCancelByIdIn(List<Long> ids, String tableName) {
        isValidTableName(tableName);
        if (ids == null || ids.isEmpty()) {
            return new ArrayList<>();
        }

        NamedParameterJdbcTemplate namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
        String sql = "SELECT * FROM " + tableName + " WHERE id IN (:ids) AND status = 5";
        ExOrderEntityRowMapper rowMapper = new ExOrderEntityRowMapper();
        List<ExOrderEntity> result = new ArrayList<>();

        int batchSize = 1000;
        for (int i = 0; i < ids.size(); i += batchSize) {
            int end = Math.min(i + batchSize, ids.size());
            List<Long> batch = ids.subList(i, end);
            Map<String, List<Long>> params = Collections.singletonMap("ids", batch);
            result.addAll(namedParameterJdbcTemplate.query(sql, params, rowMapper));
        }

        return result;
    }

    public int updateStatus(Long id, OrderStatus newStatus, String tableName) {
        isValidTableName(tableName);
        String sql = "UPDATE " + tableName + " SET status = ?, mtime = ? WHERE id = ?  AND status IN (0,1,3)";
        return jdbcTemplate.update(sql, newStatus.value, Timestamp.valueOf(java.time.LocalDateTime.now()), id);
    }

    public int batchUpdateCancelStatus(List<Long> ids, OrderStatus newStatus, String tableName) {
        isValidTableName(tableName);
        if (ids == null || ids.isEmpty()) {
            return 0;
        }
        String inClause = String.join(",", java.util.Collections.nCopies(ids.size(), "?"));
        String sql = "UPDATE " + tableName + " SET status = ?, mtime = ? WHERE id IN (" + inClause + ") AND status = 5";

        List<Object> params = new ArrayList<>();
        params.add(newStatus.value);
        params.add(Timestamp.valueOf(java.time.LocalDateTime.now()));
        params.addAll(ids);

        return jdbcTemplate.update(sql, params.toArray());
    }

    private void isValidTableName(String tableName) {
        if (tableName == null || !tableName.matches("^ex_order_[a-zA-Z0-9_]+$")) {
            throw new IllegalArgumentException("Invalid table name");
        }
    }

}