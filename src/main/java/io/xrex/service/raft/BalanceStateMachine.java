package io.xrex.service.raft;

import com.alibaba.fastjson2.JSON;
import com.lmax.disruptor.EventTranslatorVararg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import io.xrex.enums.ReadConsistency;
import io.xrex.event.disruptor.TransferRingBufferEvent;
import io.xrex.event.handler.BalanceUpdateEventHandler;
import io.xrex.model.dto.AccountIdDto;
import io.xrex.model.dto.event.TransactionEventDto;
import io.xrex.service.ConfigService;
import io.xrex.service.RocksDBService;
import io.xrex.service.kafka.KafkaProducerService;
import io.xrex.service.raft.command.BatchCommand;
import io.xrex.service.raft.command.QueryCommand;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.rocksdb.Checkpoint;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class BalanceStateMachine extends BaseStateMachine {

    private final Map<String, Long> clientSequenceIds = new ConcurrentHashMap<>();
    private RocksDB db;
    private File dbDir;
    private RaftGroupId groupId;

    private final KafkaProducerService kafkaProducerService;
    private final ConfigService configService;
    private final RocksDBService rocksDBService;

    private Disruptor<TransferRingBufferEvent> disruptor;
    private RingBuffer<TransferRingBufferEvent> ringBuffer;

    public BalanceStateMachine(KafkaProducerService kafkaProducerService, ConfigService configService, RocksDBService rocksDBService) {
        this.kafkaProducerService = kafkaProducerService;
        this.configService = configService;
        this.rocksDBService = rocksDBService;
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

        this.disruptor = new Disruptor<>(TransferRingBufferEvent::new, 1024, DaemonThreadFactory.INSTANCE);
        final BalanceUpdateEventHandler handler = new BalanceUpdateEventHandler(this.kafkaProducerService, this.configService, this.rocksDBService);
        this.disruptor.handleEventsWith(handler);
        this.ringBuffer = this.disruptor.start();
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
        log.info("[BalanceStateMachine] applyTransaction START. Index={}", trx.getLogEntry().getIndex());

        final byte[] logData = trx.getStateMachineLogEntry().getLogData().toByteArray();
        final BatchCommand command = JSON.parseObject(logData, BatchCommand.class);

        final String clientId = command.getClientId();
        final long sequenceId = command.getSequenceId();

        if (isDuplicate(clientId, sequenceId)) {
            log.warn("[BalanceStateMachine] Duplicate request detected. ClientId={}, SequenceId={}", clientId, sequenceId);
            return CompletableFuture.completedFuture(Message.valueOf("Duplicate request"));
        }

        log.info("[BalanceStateMachine] Publishing {} events to RingBuffer using publishEvents...", command.getEvents().size());
        var events = command.getEvents().toArray(new TransactionEventDto[0]);
        ringBuffer.publishEvents(BATCH_TRANSLATOR, events);
        log.info("[BalanceStateMachine] All events published to RingBuffer.");

        clientSequenceIds.put(clientId, sequenceId);
        log.info("[BalanceStateMachine] applyTransaction END. Returning OK to Raft framework.");
        return CompletableFuture.completedFuture(Message.valueOf("OK"));
    }

    // ... (rest of the methods remain the same)

    @Override
    public CompletableFuture<Message> query(Message request) {
        final QueryCommand command = JSON.parseObject(request.getContent().toByteArray(), QueryCommand.class);
        final AccountIdDto accountId = command.getAccountId();
        final ReadConsistency consistency = command.getReadConsistency();

        return switch (consistency) {
            case STRONG -> queryStrong(accountId);
            case BOUNDED -> queryBounded(accountId);
            case EVENTUAL -> queryEventual(accountId);
        };
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
        BigDecimal balance = rocksDBService.queryEventual(accountId);
        return CompletableFuture.completedFuture(Message.valueOf(balance.toPlainString()));
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
            disruptor.shutdown();
        }
        if (db != null) {
            db.close();
        }
    }
}