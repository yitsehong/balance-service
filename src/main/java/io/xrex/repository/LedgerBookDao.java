package io.xrex.repository;

import io.xrex.model.entity.LedgerBookEntity;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class LedgerBookDao {

    private final JdbcTemplate jdbcTemplate;

    private static final String BATCH_INSERT_SQL = """
            INSERT INTO ledger_book(idempotency_key, chainup_id, asset_type, coin_symbol, account_tag, amount, before_balance, after_balance, 
            scene, ref_type, ref_id, created_time, updated_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    public void batchInsert(List<LedgerBookEntity> ledgerBooks) {
        jdbcTemplate.batchUpdate(BATCH_INSERT_SQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                LedgerBookEntity ledger = ledgerBooks.get(i);
                ps.setString(1, ledger.getIdempotencyKey());
                ps.setInt(2, ledger.getChainupId());
                ps.setInt(3, ledger.getAssetType());
                ps.setString(4, ledger.getCoinSymbol());
                ps.setString(5, ledger.getAccountTag());
                ps.setBigDecimal(6, ledger.getAmount());
                ps.setBigDecimal(7, ledger.getBeforeBalance());
                ps.setBigDecimal(8, ledger.getAfterBalance());
                ps.setString(9, ledger.getScene());
                ps.setString(10, ledger.getRefType());
                ps.setLong(11, ledger.getRefId());
                ps.setTimestamp(12, Timestamp.valueOf(ledger.getCreatedTime()));
                ps.setTimestamp(13, Timestamp.valueOf(ledger.getUpdatedTime()));
            }

            @Override
            public int getBatchSize() {
                return ledgerBooks.size();
            }
        });
    }
}
