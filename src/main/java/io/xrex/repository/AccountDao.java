package io.xrex.repository;

import io.xrex.model.dto.AccountIdDto;
import io.xrex.model.entity.AccountEntity;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Repository
@RequiredArgsConstructor
public class AccountDao {

    private static final String BATCH_UPDATE_SQL = "UPDATE account SET balance = balance + ?, mtime = ? WHERE uid = ? AND type = ?";
    private static final String BATCH_INSERT_SQL = "INSERT INTO account (uid, type, balance, tag, ctime, mtime) VALUES (?, ?, ?, ?, ?, ?)";
    private final JdbcTemplate jdbcTemplate;

    public int[] batchUpdateBalances(List<Map.Entry<AccountIdDto, BigDecimal>> adjustmentsList) {
        if (adjustmentsList == null || adjustmentsList.isEmpty()) {
            return new int[0];
        }

        return jdbcTemplate.batchUpdate(BATCH_UPDATE_SQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Map.Entry<AccountIdDto, BigDecimal> entry = adjustmentsList.get(i);
                AccountIdDto accountId = entry.getKey();
                BigDecimal adjustment = entry.getValue();

                ps.setBigDecimal(1, adjustment);
                ps.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
                ps.setInt(3, accountId.getChainupId());
                ps.setInt(4, accountId.getAssetType());
            }

            @Override
            public int getBatchSize() {
                return adjustmentsList.size();
            }
        });
    }

    public void batchInsert(List<AccountEntity> accounts) {
        if (accounts == null || accounts.isEmpty()) {
            return;
        }

        jdbcTemplate.batchUpdate(BATCH_INSERT_SQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                AccountEntity account = accounts.get(i);
                ps.setInt(1, account.getUid());
                ps.setInt(2, account.getType());
                ps.setBigDecimal(3, account.getBalance());
                ps.setString(4, account.getTag());
                ps.setTimestamp(5, Timestamp.valueOf(account.getCtime()));
                ps.setTimestamp(6, Timestamp.valueOf(account.getMtime()));
            }

            @Override
            public int getBatchSize() {
                return accounts.size();
            }
        });
    }
}
