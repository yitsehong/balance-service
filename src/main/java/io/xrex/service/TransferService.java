package io.xrex.service;

import io.xrex.model.dto.AccountIdDto;
import io.xrex.model.dto.event.TransactionEventDto;
import io.xrex.model.entity.AccountEntity;
import io.xrex.model.entity.ConfigAccountTypeEntity;
import io.xrex.model.entity.LedgerBookEntity;
import io.xrex.model.entity.TransactionEntity;
import io.xrex.repository.AccountDao;
import io.xrex.repository.TransactionDao;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * This service handles the core logic for transferring funds between accounts.
 * It provides methods for batch-updating account balances and creating transaction records.
 * The service is designed to be transactional, ensuring that all parts of a transfer
 * either complete successfully or fail together.
 */
@Slf4j
@Service
public class TransferService {

    private final ConfigService configService;
    private final AccountDao accountDao;
    private final TransactionDao transactionDao;

    public TransferService(ConfigService configService,
                           AccountDao accountDao, TransactionDao transactionDao) {
        this.configService = configService;
        this.accountDao = accountDao;
        this.transactionDao = transactionDao;
    }

    /**
     * Performs a batch transfer operation within a single database transaction.
     * It first attempts to update the balances of existing accounts. If an account
     * does not exist, it creates a new one. Finally, it batch-inserts all the
     * transaction records.
     *
     * @param balanceAdjustments A map where the key is the account ID and the value is the amount to adjust the balance by.
     * @param transactions A list of transaction entities to be inserted.
     */
    @Transactional
    public void batchTransfer(Map<AccountIdDto, BigDecimal> balanceAdjustments,
                              List<TransactionEntity> transactions) {
        // Apply the aggregated balance updates to the account table in a single batch operation.
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
        transactionDao.batchInsert(transactions);
    }

    private AccountEntity buildNewAccount(AccountIdDto accountId, BigDecimal initialBalance) {
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
        AccountEntity account = new AccountEntity();
        account.setUid(accountId.getChainupId());
        account.setType(accountId.getAssetType());
        account.setBalance(initialBalance);
        account.setCtime(now);
        account.setMtime(now);

        ConfigAccountTypeEntity configAccountType = configService.findByAssetType(accountId.getAssetType());
        if (configAccountType != null) {
            account.setTag(Optional.ofNullable(configAccountType.getTag()).map(String::toLowerCase).orElse(null));
        }
        return account;
    }

    /**
     * Creates a TransactionEntity from a TransactionEventDto.
     * This method maps the data from the event to a persistable transaction entity.
     *
     * @param event The transaction event DTO.
     * @return A new TransactionEntity.
     */
    public TransactionEntity createTransactionEntityFromEvent(TransactionEventDto event) {
        long id = Long.parseLong(event.getTransactionId());
        LedgerBookEntity fromLedger = event.getFrom();
        LedgerBookEntity toLedger = event.getTo();

        TransactionEntity transaction = new TransactionEntity();
        transaction.setId(id);
        transaction.setFromUid(fromLedger.getChainupId());
        transaction.setFromType(fromLedger.getAssetType());
        transaction.setFromBalance(fromLedger.getAfterBalance());
        transaction.setToUid(toLedger.getChainupId());
        transaction.setToType(toLedger.getAssetType());
        transaction.setToBalance(toLedger.getAfterBalance());
        transaction.setAmount(toLedger.getAmount());
        transaction.setScene(toLedger.getScene());
        transaction.setMeta(event.getMeta());
        transaction.setRefType(fromLedger.getRefType());
        transaction.setRefId(fromLedger.getRefId());
        transaction.setOpUid(event.getOpUid());
        transaction.setOpIp(event.getOpIp());
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
        transaction.setCtime(now);
        transaction.setMtime(now);
        transaction.fingerprint();
        return transaction;
    }
}
