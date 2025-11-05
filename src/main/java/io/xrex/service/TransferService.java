package io.xrex.service;

import io.xrex.enums.ReadConsistency;
import io.xrex.model.dto.AccountIdDto;
import io.xrex.model.dto.event.TransactionEventDto;
import io.xrex.model.entity.LedgerBookEntity;
import io.xrex.model.entity.TransactionEntity;
import io.xrex.repository.TransactionDao;
import io.xrex.service.raft.BatchTransferProcessorService;
import io.xrex.service.raft.CustomRaftClient;
import io.xrex.service.raft.TransferRaftRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.protocol.RaftClientReply;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
public class TransferService {

    private final AccountService accountService;
    private final TransactionDao transactionDao;
    private final BatchTransferProcessorService batchTransferProcessorService;
    private final CustomRaftClient raftClient;

    public TransferService(AccountService accountService,
                           TransactionDao transactionDao,
                           BatchTransferProcessorService batchTransferProcessorService,
                           CustomRaftClient raftClient) {
        this.accountService = accountService;
        this.transactionDao = transactionDao;
        this.batchTransferProcessorService = batchTransferProcessorService;
        this.raftClient = raftClient;
    }

    public CompletableFuture<RaftClientReply> transfer(TransactionEventDto event) {
        log.info("[TransferService] submit transfer event={}", event);
        TransferRaftRequest transferRaftRequest = new TransferRaftRequest(event);
        return batchTransferProcessorService.getBatchProcessor().submit(transferRaftRequest);
    }

    public CompletableFuture<RaftClientReply> queryBalance(AccountIdDto accountId, ReadConsistency readConsistency) {
        return raftClient.queryBalance(accountId, readConsistency);
    }

    public void batchTransfer(Map<AccountIdDto, BigDecimal> balanceAdjustments,
                              List<TransactionEntity> transactions) {
        // Apply the aggregated balance updates to the account table in a single batch operation.
        accountService.batchUpdateBalances(balanceAdjustments);
        transactionDao.batchInsert(transactions);
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
