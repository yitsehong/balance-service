package io.xrex.service;

import io.xrex.model.dto.AccountIdDto;
import io.xrex.model.dto.event.TransactionEventDto;
import io.xrex.model.entity.LedgerBookEntity;
import io.xrex.model.entity.TransactionEntity;
import io.xrex.repository.LedgerBookDao;
import io.xrex.repository.TransactionDao;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class TransferService {

    private final AccountService accountService;
    private final TransactionDao transactionDao;
    private final LedgerBookDao ledgerBookDao;

    public TransferService(AccountService accountService,
                           TransactionDao transactionDao,
                           LedgerBookDao ledgerBookDao) {
        this.accountService = accountService;
        this.transactionDao = transactionDao;
        this.ledgerBookDao = ledgerBookDao;
    }

    @Transactional
    public void transfer(TransactionEventDto event) {
        processSingleTransfer(event);
    }

    @Transactional
    public void batchTransfer(Map<AccountIdDto, BigDecimal> balanceAdjustments,
                              List<LedgerBookEntity> ledgerBooks,
                              List<TransactionEntity> transactions) {
        // Step 1: Apply the aggregated balance updates to the account table in a single batch operation.
        accountService.batchUpdateBalances(balanceAdjustments);
        // Step 2: Batch insert all records using high-performance JdbcTemplate
        if (ledgerBooks != null) {

        }

        transactionDao.batchInsert(transactions);
    }

    private void processSingleTransfer(TransactionEventDto event) {
        LedgerBookEntity fromLedger = event.getFrom();
        LedgerBookEntity toLedger = event.getTo();

        // Update account balances
        accountService.updateBalance(fromLedger.getChainupId(), fromLedger.getAssetType(), fromLedger.getAmount());
        accountService.updateBalance(toLedger.getChainupId(), toLedger.getAssetType(), toLedger.getAmount());

        // Save ledger books
        ledgerBookDao.batchInsert(List.of(fromLedger, toLedger));
        // Create and persist the transaction record
        transactionDao.batchInsert(List.of(createTransactionEntityFromEvent(event)));
    }

    public TransactionEntity createTransactionEntityFromEvent(TransactionEventDto event) {
        long id = Long.parseLong(event.getEventKey());
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
        LocalDateTime now = LocalDateTime.now();
        transaction.setCtime(now);
        transaction.setMtime(now);
        transaction.fingerprint();
        return transaction;
    }
}
