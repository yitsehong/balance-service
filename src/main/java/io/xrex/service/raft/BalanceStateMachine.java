package io.xrex.service.raft;

import com.alibaba.fastjson2.JSON;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import io.xrex.enums.ReadConsistency;
import io.xrex.event.disruptor.TransferRingBufferEvent;
import io.xrex.event.handler.BalanceUpdateEventHandlerFactory;
import io.xrex.model.dto.AccountIdDto;
import io.xrex.model.dto.event.TransactionEventDto;
import io.xrex.service.ConfigService;
import io.xrex.service.DisruptorPartitionManager;
import io.xrex.service.RocksDBService;
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
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class BalanceStateMachine extends BaseStateMachine {

    // 1. 修改 Translator 類型為 EventTranslatorOneArg，並專注於處理單個 DTO
    private static final EventTranslatorOneArg<TransferRingBufferEvent, TransactionEventDto> TRANSACTION_EVENT_TRANSLATOR =
            (event, sequence, eventData) -> {
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

                CompletableFuture<String> future = new CompletableFuture<>();
                event.setFuture(future);
            };
    private final Map<String, Long> clientSequenceIds = new ConcurrentHashMap<>();
    private final KafkaTemplate<String, TransactionEventDto> kafkaTemplate;
    private final ConfigService configService;
    private final RocksDBService rocksDBService;
    private RocksDB db;
    private File dbDir;
    private RaftGroupId groupId;
    @Value("${app.kafka.balance-transfer.topic}")
    private String topic;

    private DisruptorPartitionManager disruptorPartitionManager;
    private BalanceUpdateEventHandlerFactory balanceUpdateEventHandlerFactory;


    public BalanceStateMachine(KafkaTemplate<String, TransactionEventDto> kafkaTemplate,
                               ConfigService configService, RocksDBService rocksDBService) {
        this.kafkaTemplate = kafkaTemplate;
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

        // Initialize partitioned disruptor
        this.balanceUpdateEventHandlerFactory = new BalanceUpdateEventHandlerFactory(this.kafkaTemplate, this.configService, this.rocksDBService);
        this.disruptorPartitionManager = new DisruptorPartitionManager(this.balanceUpdateEventHandlerFactory, this.topic, this.configService);
        this.disruptorPartitionManager.initialize();
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        try {
            final byte[] logData = trx.getStateMachineLogEntry().getLogData().toByteArray();
            final BatchCommand command = JSON.parseObject(logData, BatchCommand.class);

            final String clientId = command.getClientId();
            final long sequenceId = command.getSequenceId();

            if (isDuplicate(clientId, sequenceId)) {
                log.warn("[BalanceStateMachine] Duplicate request detected. ClientId={}, SequenceId={}", clientId, sequenceId);
                return CompletableFuture.completedFuture(Message.valueOf("Duplicate request"));
            }
            MDC.put("eventKeys", command.getEvents().get(0).getEventKey());
            log.info("[BalanceStateMachine] Publishing {} events to RingBuffer...", command.getEvents().size());

            // 2. 修改發布邏輯：遍歷 DTO 列表，為每個 DTO 單獨發布一個事件
            for (TransactionEventDto eventDto : command.getEvents()) {
                RingBuffer<TransferRingBufferEvent> ringBuffer = disruptorPartitionManager.getRingBuffer(eventDto.getFrom().getAssetType());
                if (ringBuffer != null) {
                    ringBuffer.publishEvent(TRANSACTION_EVENT_TRANSLATOR, eventDto);
                } else {
                    log.error("No ring buffer found for asset type: {}", eventDto.getFrom().getAssetType());
                    // Handle error: maybe push to a default queue or reject the transaction
                }
            }
            clientSequenceIds.put(clientId, sequenceId);
            log.info("[BalanceStateMachine] applyTransaction END. Returning OK to Raft framework.");
            return CompletableFuture.completedFuture(Message.valueOf("OK"));
        } finally {
            MDC.remove("eventKeys");
        }
    }

    // ... (其餘程式碼保持不變) ...

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
        if (disruptorPartitionManager != null) {
            disruptorPartitionManager.shutdown();
        }
        if (db != null) {
            db.close();
        }
    }
}
