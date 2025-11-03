
package io.xrex.repository;

import com.alibaba.fastjson2.JSON;
import io.xrex.model.dto.AccountIdDto;
import io.xrex.model.dto.BalanceDto;
import io.xrex.service.RocksDBService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
@RequiredArgsConstructor
public class RocksDBBalanceRepository {

    private final RocksDBService rocksDBService;

    public Optional<BalanceDto> findById(AccountIdDto accountId) {
        String key = accountId.toString();
        byte[] balanceBytes = rocksDBService.get(key.getBytes());
        if (balanceBytes == null) {
            return Optional.empty();
        }
        return Optional.of(JSON.parseObject(balanceBytes, BalanceDto.class));
    }

    public void save(BalanceDto balance) {
        String key = balance.getAccountId().toString();
        rocksDBService.save(key.getBytes(), JSON.toJSONBytes(balance));
    }

    // Overloaded save method for convenience
    public void save(AccountIdDto accountId, BalanceDto balance) {
        rocksDBService.save(accountId.toString().getBytes(), JSON.toJSONBytes(balance));
    }
}
