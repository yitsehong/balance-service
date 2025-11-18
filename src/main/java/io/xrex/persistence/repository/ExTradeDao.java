package io.xrex.persistence.repository;

import io.xrex.persistence.entity.ExTradeEntity;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class ExTradeDao {

    private final JdbcTemplate jdbcTemplate;

    @Transactional
    public List<Long> batchInsert(List<ExTradeEntity> trades, String tableName) {
        isValidTableName(tableName);
        String sql = "INSERT INTO " + tableName + " (price, volume, bid_id, ask_id, trend_side, bid_user_id, ask_user_id, " +
                "buy_fee, sell_fee, buy_fee_coin, sell_fee_coin, ctime, mtime, buy_type, sell_type) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        List<Long> generatedIds = new ArrayList<>();
        for (ExTradeEntity trade : trades) {
            KeyHolder keyHolder = new GeneratedKeyHolder();
            jdbcTemplate.update(connection -> {
                PreparedStatement ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                if (trade.getPrice() != null) ps.setBigDecimal(1, trade.getPrice()); else ps.setNull(1, Types.DECIMAL);
                if (trade.getVolume() != null) ps.setBigDecimal(2, trade.getVolume()); else ps.setNull(2, Types.DECIMAL);
                if (trade.getBidId() != null) ps.setLong(3, trade.getBidId()); else ps.setNull(3, Types.BIGINT);
                if (trade.getAskId() != null) ps.setLong(4, trade.getAskId()); else ps.setNull(4, Types.BIGINT);
                if (trade.getTrendSide() != null) ps.setString(5, trade.getTrendSide()); else ps.setNull(5, Types.VARCHAR);
                if (trade.getBidUserId() != null) ps.setInt(6, trade.getBidUserId()); else ps.setNull(6, Types.INTEGER);
                if (trade.getAskUserId() != null) ps.setInt(7, trade.getAskUserId()); else ps.setNull(7, Types.INTEGER);
                if (trade.getBuyFee() != null) ps.setBigDecimal(8, trade.getBuyFee()); else ps.setNull(8, Types.DECIMAL);
                if (trade.getSellFee() != null) ps.setBigDecimal(9, trade.getSellFee()); else ps.setNull(9, Types.DECIMAL);
                if (trade.getBuyFeeCoin() != null) ps.setString(10, trade.getBuyFeeCoin()); else ps.setNull(10, Types.VARCHAR);
                if (trade.getSellFeeCoin() != null) ps.setString(11, trade.getSellFeeCoin()); else ps.setNull(11, Types.VARCHAR);
                if (trade.getCtime() != null) ps.setTimestamp(12, Timestamp.valueOf(trade.getCtime())); else ps.setNull(12, Types.TIMESTAMP);
                if (trade.getMtime() != null) ps.setTimestamp(13, Timestamp.valueOf(trade.getMtime())); else ps.setNull(13, Types.TIMESTAMP);
                if (trade.getBuyType() != null) ps.setByte(14, trade.getBuyType()); else ps.setNull(14, Types.TINYINT);
                if (trade.getSellType() != null) ps.setByte(15, trade.getSellType()); else ps.setNull(15, Types.TINYINT);
                return ps;
            }, keyHolder);
            generatedIds.add(keyHolder.getKey().longValue());
        }
        return generatedIds;
    }

    public Long insert(ExTradeEntity trade, String tableName) {
        isValidTableName(tableName);
        String sql = "INSERT INTO " + tableName + " (price, volume, bid_id, ask_id, trend_side, bid_user_id, ask_user_id, " +
                "buy_fee, sell_fee, buy_fee_coin, sell_fee_coin, ctime, mtime, buy_type, sell_type) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(connection -> {
            PreparedStatement ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            if (trade.getPrice() != null) ps.setBigDecimal(1, trade.getPrice()); else ps.setNull(1, Types.DECIMAL);
            if (trade.getVolume() != null) ps.setBigDecimal(2, trade.getVolume()); else ps.setNull(2, Types.DECIMAL);
            if (trade.getBidId() != null) ps.setLong(3, trade.getBidId()); else ps.setNull(3, Types.BIGINT);
            if (trade.getAskId() != null) ps.setLong(4, trade.getAskId()); else ps.setNull(4, Types.BIGINT);
            if (trade.getTrendSide() != null) ps.setString(5, trade.getTrendSide()); else ps.setNull(5, Types.VARCHAR);
            if (trade.getBidUserId() != null) ps.setInt(6, trade.getBidUserId()); else ps.setNull(6, Types.INTEGER);
            if (trade.getAskUserId() != null) ps.setInt(7, trade.getAskUserId()); else ps.setNull(7, Types.INTEGER);
            if (trade.getBuyFee() != null) ps.setBigDecimal(8, trade.getBuyFee()); else ps.setNull(8, Types.DECIMAL);
            if (trade.getSellFee() != null) ps.setBigDecimal(9, trade.getSellFee()); else ps.setNull(9, Types.DECIMAL);
            if (trade.getBuyFeeCoin() != null) ps.setString(10, trade.getBuyFeeCoin()); else ps.setNull(10, Types.VARCHAR);
            if (trade.getSellFeeCoin() != null) ps.setString(11, trade.getSellFeeCoin()); else ps.setNull(11, Types.VARCHAR);
            if (trade.getCtime() != null) ps.setTimestamp(12, Timestamp.valueOf(trade.getCtime())); else ps.setNull(12, Types.TIMESTAMP);
            if (trade.getMtime() != null) ps.setTimestamp(13, Timestamp.valueOf(trade.getMtime())); else ps.setNull(13, Types.TIMESTAMP);
            if (trade.getBuyType() != null) ps.setByte(14, trade.getBuyType()); else ps.setNull(14, Types.TINYINT);
            if (trade.getSellType() != null) ps.setByte(15, trade.getSellType()); else ps.setNull(15, Types.TINYINT);
            return ps;
        }, keyHolder);

        return keyHolder.getKey().longValue();
    }

    public ExTradeEntity findLatestTrade(String tableName) {
        isValidTableName(tableName);
        String sql = "SELECT * FROM " + tableName + " ORDER BY id DESC LIMIT 1";
        try {
            return jdbcTemplate.queryForObject(sql, new BeanPropertyRowMapper<>(ExTradeEntity.class));
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return null;
        }
    }

    private void isValidTableName(String tableName) {
        if (tableName == null || !tableName.matches("^ex_trade_[a-zA-Z0-9_]+$")) {
            throw new IllegalArgumentException("Invalid table name");
        }
    }
}
