package io.xrex.service.raft;

import com.alibaba.fastjson2.JSON;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.lmax.disruptor.EventTranslatorVararg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import io.xrex.enums.ReadConsistency;
import io.xrex.event.disruptor.TransferRingBufferEvent;
import io.xrex.event.handler.BalanceUpdateEventHandler;
import io.xrex.model.dto.AccountIdDto;
import io.xrex.model.dto.event.TransactionEventDto;
import io.xrex.repository.ConfigAccountTypeRepository;
import io.xrex.repository.LedgerBookDao;
import io.xrex.repository.TransactionDao;
import io.xrex.service.kafka.KafkaProducerService;
import io.xrex.service.raft.command.BatchCommand;
import io.xrex.service.raft.command.QueryCommand;
import io.xrex.util.SnowflakeIdGenerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.rocksdb.*;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
public class BalanceStateMachine extends BaseStateMachine {

    private final Map<String, Long> clientSequenceIds = new ConcurrentHashMap<>();
    private RocksDB db;
    private File dbDir;
    private RaftGroupId groupId;

    private final Cache<AccountIdDto, BigDecimal> l1Cache = Caffeine.newBuilder()
            .maximumSize(10_000)
            .expireAfterWrite(1, TimeUnit.SECONDS)
            .build();

    private final KafkaProducerService kafkaProducerService;
    private final ConfigAccountTypeRepository configRepo;
    private final TransactionDao transactionDao;
    private final LedgerBookDao ledgerBookDao;
    private final SnowflakeIdGenerator snowflakeIdGenerator;

    private Disruptor<TransferRingBufferEvent> disruptor;
    private RingBuffer<TransferRingBufferEvent> ringBuffer;

    public BalanceStateMachine(KafkaProducerService kafkaProducerService, ConfigAccountTypeRepository configRepo, TransactionDao transactionDao, LedgerBookDao ledgerBookDao, SnowflakeIdGenerator snowflakeIdGenerator) {
        this.kafkaProducerService = kafkaProducerService;
        this.configRepo = configRepo;
        this.transactionDao = transactionDao;
        this.ledgerBookDao = ledgerBookDao;
        this.snowflakeIdGenerator = snowflakeIdGenerator;
    }

    @Override
    public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage storage) throws IOException {
        super.initialize(server, groupId, storage);
        this.groupId = groupId;
        this.dbDir = new File(storage.getStorageDir().getRoot().getParentFile(), "rocksdb/" + groupId.getUuid());
        if (!dbDir.exists()) {
            dbDir.mkdirs();
        }
        try {
            this.db = RocksDB.open(new Options().setCreateIfMissing(true), dbDir.getAbsolutePath());
        } catch (RocksDBException e) {
            throw new IOException("Failed to initialize RocksDB", e);
        }

        log.info("DEBUG: Initializing Disruptor...");
        this.disruptor = new Disruptor<>(TransferRingBufferEvent::new, 1024, DaemonThreadFactory.INSTANCE);
        final BalanceUpdateEventHandler handler = new BalanceUpdateEventHandler(this.db, this.l1Cache, this.kafkaProducerService, this.configRepo, this.transactionDao, this.ledgerBookDao, this.snowflakeIdGenerator);
        this.disruptor.handleEventsWith(handler);
        this.ringBuffer = this.disruptor.start();
        log.info("DEBUG: Disruptor started.");
    }

    private static final EventTranslatorVararg<TransferRingBufferEvent> BATCH_TRANSLATOR = (event, sequence, args) -> {
        TransactionEventDto eventData = (TransactionEventDto) args[0];
        event.setEventKey(eventData.getEventKey());
        event.setFromChainupId(eventData.getFrom().getChainupId());
        event.setFromAssetType(eventData.getFrom().getAssetType());
        event.setToChainupId(eventData.getTo().getChainupId());
        event.setToAssetType(eventData.getTo().getAssetType());
        event.setAmount(eventData.getTo().getAmount());
        event.setScene(eventData.getFrom().getScene());
        event.setRefType(eventData.getFrom().getRefType());
        event.setRefId(eventData.getFrom().getRefId());
        event.setMeta(eventData.getMeta());
        event.setOpUid(eventData.getOpUid());
        event.setOpIp(eventData.getOpIp());
        event.setFuture(null);
    };

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        log.info("DEBUG: [StateMachine] applyTransaction START. Index={}", trx.getLogEntry().getIndex());

        final byte[] logData = trx.getStateMachineLogEntry().getLogData().toByteArray();
        final BatchCommand command = JSON.parseObject(logData, BatchCommand.class);

        final String clientId = command.getClientId();
        final long sequenceId = command.getSequenceId();

        if (isDuplicate(clientId, sequenceId)) {
            log.warn("DEBUG: [StateMachine] Duplicate request detected. ClientId={}, SequenceId={}", clientId, sequenceId);
            return CompletableFuture.completedFuture(Message.valueOf("Duplicate request"));
        }

        log.info("DEBUG: [StateMachine] Publishing {} events to RingBuffer using publishEvents...", command.getEvents().size());
        var events = command.getEvents().toArray(new TransactionEventDto[0]);
        ringBuffer.publishEvents(BATCH_TRANSLATOR, events);
        log.info("DEBUG: [StateMachine] All events published to RingBuffer.");

        clientSequenceIds.put(clientId, sequenceId);
        log.info("DEBUG: [StateMachine] applyTransaction END. Returning OK to Raft framework.");
        return CompletableFuture.completedFuture(Message.valueOf("OK"));
    }

    // ... (rest of the methods remain the same)

    @Override
    public CompletableFuture<Message> query(Message request) {
        final QueryCommand command = JSON.parseObject(request.getContent().toByteArray(), QueryCommand.class);
        final AccountIdDto accountId = command.getAccountId();
        final ReadConsistency consistency = command.getReadConsistency();

        switch (consistency) {
            case STRONG:
                return queryStrong(accountId);
            case BOUNDED:
                return queryBounded(accountId);
            case EVENTUAL:
                return queryEventual(accountId);
            default:
                return CompletableFuture.failedFuture(new IllegalArgumentException("Unknown read consistency level"));
        }
    }

    private CompletableFuture<Message> queryStrong(AccountIdDto accountId) {
        return CompletableFuture.completedFuture(Message.valueOf(readFromRocksDB(accountId).toString()));
    }

    private CompletableFuture<Message> queryBounded(AccountIdDto accountId) {
        long lag = getLag();
        if (lag < 10) {
            return CompletableFuture.completedFuture(Message.valueOf(readFromRocksDB(accountId).toString()));
        } else {
            return queryStrong(accountId);
        }
    }

    private CompletableFuture<Message> queryEventual(AccountIdDto accountId) {
        BigDecimal balance = l1Cache.getIfPresent(accountId);
        if (balance == null) {
            balance = readFromRocksDB(accountId);
            l1Cache.put(accountId, balance);
        }
        return CompletableFuture.completedFuture(Message.valueOf(balance.toString()));
    }

    private BigDecimal readFromRocksDB(AccountIdDto accountId) {
        try {
            byte[] key = JSON.toJSONBytes(accountId);
            byte[] value = db.get(key);
            return (value != null) ? new BigDecimal(new String(value, StandardCharsets.UTF_8)) : BigDecimal.ZERO;
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to query balance from RocksDB", e);
        }
    }

    private long getLag() {
        try {
            return getServer().get().getDivision(groupId).getStateMachine().getLastAppliedTermIndex().getIndex() - getLastAppliedTermIndex().getIndex();
        } catch (Exception e) {
            return Long.MAX_VALUE;
        }
    }

    @Override
    public long takeSnapshot() throws IOException {
        try (Checkpoint checkpoint = Checkpoint.create(db)) {
            final long lastAppliedIndex = getLastAppliedTermIndex().getIndex();
            File snapshotDir = new File(dbDir.getParent(), "snapshot-" + lastAppliedIndex);
            if (!snapshotDir.exists()) {
                snapshotDir.mkdirs();
            }
            checkpoint.createCheckpoint(snapshotDir.getAbsolutePath());
            return lastAppliedIndex;
        } catch (RocksDBException e) {
            throw new IOException("Failed to take snapshot", e);
        }
    }

    private boolean isDuplicate(String clientId, long sequenceId) {
        return clientSequenceIds.getOrDefault(clientId, -1L) >= sequenceId;
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (disruptor != null) {
            log.info("DEBUG: Shutting down Disruptor...");
            disruptor.shutdown();
            log.info("DEBUG: Disruptor shut down.");
        }
        if (db != null) {
            db.close();
        }
    }
}