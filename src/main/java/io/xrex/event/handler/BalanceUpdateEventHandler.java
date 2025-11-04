package io.xrex.event.handler;

import com.alibaba.fastjson2.JSON;
import com.github.benmanes.caffeine.cache.Cache;
import com.lmax.disruptor.EventHandler;
import io.xrex.event.disruptor.TransferRingBufferEvent;
import io.xrex.model.dto.AccountIdDto;
import io.xrex.model.dto.event.TransactionEventDto;
import io.xrex.model.entity.ConfigAccountTypeEntity;
import io.xrex.model.entity.LedgerBookEntity;
import io.xrex.repository.ConfigAccountTypeRepository;
import io.xrex.repository.LedgerBookDao;
import io.xrex.repository.TransactionDao;
import io.xrex.service.kafka.KafkaProducerService;
import io.xrex.util.SnowflakeIdGenerator;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class BalanceUpdateEventHandler implements EventHandler<TransferRingBufferEvent> {

    private final RocksDB db;
    private final Cache<AccountIdDto, BigDecimal> l1Cache;
    private final KafkaProducerService kafkaProducerService;
    private final ConfigAccountTypeRepository configRepo;
    private final TransactionDao transactionDao;
    private final LedgerBookDao ledgerBookDao;
    private final SnowflakeIdGenerator snowflakeIdGenerator;

    private final List<TransferRingBufferEvent> batch = new ArrayList<>();

    public BalanceUpdateEventHandler(RocksDB db, Cache<AccountIdDto, BigDecimal> l1Cache, KafkaProducerService kafkaProducerService, ConfigAccountTypeRepository configRepo, TransactionDao transactionDao, LedgerBookDao ledgerBookDao, SnowflakeIdGenerator snowflakeIdGenerator) {
        this.db = db;
        this.l1Cache = l1Cache;
        this.kafkaProducerService = kafkaProducerService;
        this.configRepo = configRepo;
        this.transactionDao = transactionDao;
        this.ledgerBookDao = ledgerBookDao;
        this.snowflakeIdGenerator = snowflakeIdGenerator;
    }

    @Override
    public void onEvent(TransferRingBufferEvent event, long sequence, boolean endOfBatch) {
        // Add event to internal batch
        batch.add(cloneEvent(event));

        // If this is the last event of the batch, process the whole batch.
        if (endOfBatch) {
            log.info("DEBUG: [EventHandler] End of batch detected. Processing {} events.", batch.size());
            try {
                processAggregatedBatch();
            } catch (Exception e) {
                log.error("DEBUG: [EventHandler] EXCEPTION processing aggregated batch.", e);
                // Handle exceptions for all futures in the batch
                batch.forEach(evt -> {
                    if (evt.getFuture() != null) {
                        evt.getFuture().completeExceptionally(e);
                    }
                });
            } finally {
                batch.clear();
            }
        }
    }

    private void processAggregatedBatch() throws RocksDBException {
        // 1. Aggregate balance deltas for all accounts in the batch
        Map<AccountIdDto, BigDecimal> deltaMap = new HashMap<>();
        for (TransferRingBufferEvent event : batch) {
            ConfigAccountTypeEntity fromConfig = configRepo.findByAssetType(event.getFromAssetType());
            AccountIdDto fromId = buildAccountId(event.getFromChainupId(), event.getFromAssetType(), fromConfig);
            deltaMap.merge(fromId, event.getAmount().negate(), BigDecimal::add);

            ConfigAccountTypeEntity toConfig = configRepo.findByAssetType(event.getToAssetType());
            AccountIdDto toId = buildAccountId(event.getToChainupId(), event.getToAssetType(), toConfig);
            deltaMap.merge(toId, event.getAmount(), BigDecimal::add);
        }

        // 2. Apply deltas to RocksDB in a single WriteBatch
        Map<AccountIdDto, BigDecimal> finalBalances = new HashMap<>();
        try (final WriteBatch writeBatch = new WriteBatch()) {
            for (Map.Entry<AccountIdDto, BigDecimal> entry : deltaMap.entrySet()) {
                AccountIdDto accountId = entry.getKey();
                BigDecimal delta = entry.getValue();
                BigDecimal newBalance = applyDeltaToRocksDB(writeBatch, accountId, delta);
                finalBalances.put(accountId, newBalance);
            }
            log.info("DEBUG: [EventHandler] Before RocksDB write for aggregated batch.");
            db.write(new WriteOptions(), writeBatch);
            log.info("DEBUG: [EventHandler] After RocksDB write for aggregated batch.");
        }

        for (TransferRingBufferEvent event : batch) {
            ConfigAccountTypeEntity fromConfig = configRepo.findByAssetType(event.getFromAssetType());
            AccountIdDto fromId = buildAccountId(event.getFromChainupId(), event.getFromAssetType(), fromConfig);
            BigDecimal fromFinalBalance = finalBalances.get(fromId);

            ConfigAccountTypeEntity toConfig = configRepo.findByAssetType(event.getToAssetType());
            AccountIdDto toId = buildAccountId(event.getToChainupId(), event.getToAssetType(), toConfig);
            BigDecimal toFinalBalance = finalBalances.get(toId);

            // Build TransactionEntity
            LocalDateTime now = LocalDateTime.now();
            // Build LedgerBookEntity (from)
            LedgerBookEntity fromLedger = buildLedgerBook(event.getEventKey(), fromId, event.getAmount().negate(), fromFinalBalance,
                    event.getScene(), event.getRefType(), event.getRefId(), now);
            // Build LedgerBookEntity (to)
            LedgerBookEntity toLedger = buildLedgerBook(event.getEventKey(), toId, event.getAmount(), toFinalBalance,
                    event.getScene(), event.getRefType(), event.getRefId(), now);

            // Re-integrate Kafka Producer Service
            TransactionEventDto kafkaEvent = TransactionEventDto.builder()
                    .eventKey(event.getEventKey()).meta(event.getMeta())
                    .opUid(event.getOpUid()).opIp(event.getOpIp())
                    .from(fromLedger).to(toLedger).build();
            kafkaProducerService.sendTransferEvent(kafkaEvent);

            // Optional: Complete future
            if (event.getFuture() != null) {
                event.getFuture().complete(event.getEventKey());
            }
        }
    }

    private BigDecimal applyDeltaToRocksDB(WriteBatch writeBatch, AccountIdDto accountId, BigDecimal delta) throws RocksDBException {
        byte[] key = JSON.toJSONBytes(accountId);
        byte[] currentValue = db.get(key);
        BigDecimal currentBalance = (currentValue == null) ? BigDecimal.ZERO : new BigDecimal(new String(currentValue, StandardCharsets.UTF_8));
        BigDecimal newBalance = currentBalance.add(delta);

        // TODO ignore check balance
        if (BigDecimal.ZERO.compareTo(delta) > 0 && accountId.getChainupId() != 1 && newBalance.compareTo(BigDecimal.ZERO) < 0) {
            log.error("Insufficient funds for account: {}. Current: {}, Delta: {}", accountId, currentBalance, delta);
            throw new IllegalStateException("Insufficient funds for account: " + accountId);
        }

        writeBatch.put(key, newBalance.toString().getBytes(StandardCharsets.UTF_8));
        l1Cache.put(accountId, newBalance);
        return newBalance;
    }

    private AccountIdDto buildAccountId(Integer chainupId, Integer assetType, ConfigAccountTypeEntity config) {
        return AccountIdDto.builder()
                .chainupId(chainupId)
                .assetType(assetType)
                .coinSymbol(config != null ? config.getCoinSymbol() : null)
                .accountTag(config != null ? config.getTag() : null)
                .build();
    }

    private LedgerBookEntity buildLedgerBook(String eventKey, AccountIdDto accountId,
                                             BigDecimal amount, BigDecimal finalBalance,
                                             String scene, String refType, Long refId, LocalDateTime now) {
        BigDecimal beforeBalance = finalBalance.subtract(amount);
        return LedgerBookEntity.builder()
                .idempotencyKey(eventKey)
                .chainupId(accountId.getChainupId())
                .assetType(accountId.getAssetType())
                .coinSymbol(accountId.getCoinSymbol())
                .accountTag(accountId.getAccountTag())
                .amount(amount)
                .beforeBalance(beforeBalance)
                .afterBalance(finalBalance)
                .coinSymbol(accountId.getCoinSymbol())
                .accountTag(accountId.getAccountTag()).scene(scene)
                .refType(refType).refId(refId)
                .createdTime(now)
                .updatedTime(now)
                .build();
    }

    private TransferRingBufferEvent cloneEvent(TransferRingBufferEvent original) {
        TransferRingBufferEvent clone = new TransferRingBufferEvent();
        clone.setEventKey(original.getEventKey());
        clone.setFromChainupId(original.getFromChainupId());
        clone.setFromAssetType(original.getFromAssetType());
        clone.setToChainupId(original.getToChainupId());
        clone.setToAssetType(original.getToAssetType());
        clone.setAmount(original.getAmount());
        clone.setScene(original.getScene());
        clone.setMeta(original.getMeta());
        clone.setRefType(original.getRefType());
        clone.setRefId(original.getRefId());
        clone.setOpUid(original.getOpUid());
        clone.setOpIp(original.getOpIp());
        clone.setFuture(original.getFuture());
        return clone;
    }
}

