package io.xrex.persistence.repository;

import io.xrex.persistence.entity.AccountEntity;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface AccountRepository extends JpaRepository<AccountEntity, Long> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT a FROM AccountEntity a WHERE a.id = :id")
    AccountEntity findByIdForUpdate(Long id);

    List<AccountEntity> findByUid(Integer chainupId);
    AccountEntity findByUidAndType(Integer chainupId, Integer type);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT a FROM AccountEntity a WHERE a.uid = :chainupId AND a.type = :type")
    AccountEntity findByUidAndTypeForUpdate(Integer chainupId, Integer type);

    List<AccountEntity> findAllByUidAndTypeIn(Integer chainupId, List<Integer> types);

    @Modifying
    @Query(value = "UPDATE AccountEntity SET balance = balance + :changeBalance, mtime = :mtime WHERE uid = :chainupId AND type = :type")
    int updateBalance(Integer chainupId, Integer type, BigDecimal changeBalance, LocalDateTime mtime);
}
