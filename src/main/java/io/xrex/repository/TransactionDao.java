package io.xrex.repository;

import io.xrex.model.entity.TransactionEntity;
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
public class TransactionDao {

    private final JdbcTemplate jdbcTemplate;

    private static final String BATCH_INSERT_SQL = """
            INSERT INTO transaction(id, from_uid, from_type, from_balance, to_uid, to_type, to_balance, amount,
            meta, scene, ref_type, ref_id, op_uid, op_ip, ctime, mtime, fingerprint) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    public void batchInsert(List<TransactionEntity> transactions) {
        jdbcTemplate.batchUpdate(BATCH_INSERT_SQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                TransactionEntity transaction = transactions.get(i);

                ps.setLong(1, transaction.getId());
                ps.setInt(2, transaction.getFromUid());
                ps.setInt(3, transaction.getFromType());
                ps.setBigDecimal(4, transaction.getFromBalance());
                ps.setInt(5, transaction.getToUid());
                ps.setInt(6, transaction.getToType());
                ps.setBigDecimal(7, transaction.getToBalance());
                ps.setBigDecimal(8, transaction.getAmount());
                ps.setString(9, transaction.getMeta());
                ps.setString(10, transaction.getScene());
                ps.setString(11, transaction.getRefType());
                ps.setLong(12, transaction.getRefId());
                ps.setInt(13, transaction.getOpUid());
                ps.setString(14, transaction.getOpIp());
                ps.setTimestamp(15, Timestamp.valueOf(transaction.getCtime()));
                ps.setTimestamp(16, Timestamp.valueOf(transaction.getMtime()));
                ps.setString(17, transaction.getFingerprint());
            }

            @Override
            public int getBatchSize() {
                return transactions.size();
            }
        });
    }
}
