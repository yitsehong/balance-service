package io.xrex.persistence.repository;

import io.xrex.persistence.entity.ExTradeEntity;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class ExTradeDao {

    private final JdbcTemplate jdbcTemplate;

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
                ps.setBigDecimal(1, trade.getPrice());
                ps.setBigDecimal(2, trade.getVolume());
                ps.setLong(3, trade.getBidId());
                ps.setLong(4, trade.getAskId());
                ps.setString(5, trade.getTrendSide());
                ps.setInt(6, trade.getBidUserId());
                ps.setInt(7, trade.getAskUserId());
                ps.setBigDecimal(8, trade.getBuyFee());
                ps.setBigDecimal(9, trade.getSellFee());
                ps.setString(10, trade.getBuyFeeCoin());
                ps.setString(11, trade.getSellFeeCoin());
                ps.setTimestamp(12, Timestamp.valueOf(trade.getCtime()));
                ps.setTimestamp(13, Timestamp.valueOf(trade.getMtime()));
                ps.setByte(14, trade.getBuyType());
                ps.setByte(15, trade.getSellType());
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
            ps.setBigDecimal(1, trade.getPrice());
            ps.setBigDecimal(2, trade.getVolume());
            ps.setLong(3, trade.getBidId());
            ps.setLong(4, trade.getAskId());
            ps.setString(5, trade.getTrendSide());
            ps.setInt(6, trade.getBidUserId());
            ps.setInt(7, trade.getAskUserId());
            ps.setBigDecimal(8, trade.getBuyFee());
            ps.setBigDecimal(9, trade.getSellFee());
            ps.setString(10, trade.getBuyFeeCoin());
            ps.setString(11, trade.getSellFeeCoin());
            ps.setTimestamp(12, Timestamp.valueOf(trade.getCtime()));
            ps.setTimestamp(13, Timestamp.valueOf(trade.getMtime()));
            ps.setByte(14, trade.getBuyType());
            ps.setByte(15, trade.getSellType());
            return ps;
        }, keyHolder);

        return keyHolder.getKey().longValue();
    }

    private void isValidTableName(String tableName) {
        if (tableName == null || !tableName.matches("^ex_trade_[a-zA-Z0-9_]+$")) {
            throw new IllegalArgumentException("Invalid table name");
        }
    }
}
