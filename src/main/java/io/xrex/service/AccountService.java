package io.xrex.service;

import io.xrex.model.dto.AccountIdDto;
import io.xrex.model.entity.AccountEntity;
import io.xrex.model.entity.ConfigAccountTypeEntity;
import io.xrex.repository.AccountDao;
import io.xrex.repository.AccountRepository;
import io.xrex.repository.ConfigAccountTypeRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Service
public class AccountService {

    private final AccountRepository accountRepository;
    private final ConfigAccountTypeRepository configAccountTypeRepository;
    private final AccountDao accountDao;

    public AccountService(AccountRepository accountRepository,
                          ConfigAccountTypeRepository configAccountTypeRepository,
                          AccountDao accountDao) {
        this.accountRepository = accountRepository;
        this.configAccountTypeRepository = configAccountTypeRepository;
        this.accountDao = accountDao;
    }

    public void batchUpdateBalances(Map<AccountIdDto, BigDecimal> balanceAdjustments) {
        if (balanceAdjustments == null || balanceAdjustments.isEmpty()) {
            return;
        }

        List<Map.Entry<AccountIdDto, BigDecimal>> adjustmentsList = new ArrayList<>(balanceAdjustments.entrySet());

        // Step 1: Attempt to batch update existing accounts
        int[] updateCounts = accountDao.batchUpdateBalances(adjustmentsList);

        // Step 2: Identify accounts that were not found and need to be inserted
        List<AccountEntity> accountsToInsert = new ArrayList<>();
        for (int i = 0; i < updateCounts.length; i++) {
            if (updateCounts[i] == 0) {
                // This update failed, meaning the account does not exist. We need to create it.
                Map.Entry<AccountIdDto, BigDecimal> failedEntry = adjustmentsList.get(i);
                AccountIdDto accountId = failedEntry.getKey();
                BigDecimal initialBalance = failedEntry.getValue();
                accountsToInsert.add(buildNewAccount(accountId, initialBalance));
            }
        }

        // Step 3: Batch insert all the new accounts
        if (!accountsToInsert.isEmpty()) {
            log.info("Found {} new accounts to insert.", accountsToInsert.size());
            accountDao.batchInsert(accountsToInsert);
        }
    }

    private AccountEntity buildNewAccount(AccountIdDto accountId, BigDecimal initialBalance) {
        LocalDateTime now = LocalDateTime.now();
        AccountEntity account = new AccountEntity();
        account.setUid(accountId.chainupId());
        account.setType(accountId.assetType());
        account.setBalance(initialBalance);
        account.setCtime(now);
        account.setMtime(now);

        ConfigAccountTypeEntity configAccountType = configAccountTypeRepository.findByAssetType(accountId.assetType());
        if (configAccountType != null) {
            account.setTag(Optional.ofNullable(configAccountType.getTag()).map(String::toLowerCase).orElse(null));
        }
        return account;
    }

    public AccountEntity findByUidAndType(Integer uid, Integer type) {
        return accountRepository.findByUidAndType(uid, type);
    }

    public AccountEntity updateBalance(Integer chainupId, Integer assetType, BigDecimal amount) {
        int checkUpdate = accountRepository.updateBalance(chainupId, assetType, amount, LocalDateTime.now());
        if (checkUpdate != 1) {
            log.warn("update balance failed, chainupId={}, type={}, amount={}", chainupId, assetType, amount);
            return this.initialAccount(chainupId, assetType, amount);
        }
        return accountRepository.findByUidAndType(chainupId, assetType);
    }

    public AccountEntity initialAccount(Integer chainupId, Integer assetType, BigDecimal amount) {
        LocalDateTime now = LocalDateTime.now();

        AccountEntity account = new AccountEntity();
        account.setUid(chainupId);
        account.setType(assetType);
        account.setBalance(amount);
        account.setCtime(now);
        account.setMtime(now);

        ConfigAccountTypeEntity configAccountType = configAccountTypeRepository.findByAssetType(assetType);
        account.setTag(Optional.ofNullable(configAccountType.getTag()).map(String::toLowerCase).orElse(null));
        AccountEntity result = accountRepository.saveAndFlush(account);
        log.info("insert account, chainupId={}, type={}", chainupId, assetType);
        return result;
    }
}
