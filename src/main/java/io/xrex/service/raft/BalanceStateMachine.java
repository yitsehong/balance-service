package io.xrex.service.raft;

import com.alibaba.fastjson2.JSON;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import io.xrex.enums.ReadConsistency;
import io.xrex.event.handler.BalanceUpdateEventHandlerFactory;
import io.xrex.dto.AccountIdDto;
import io.xrex.dto.event.TransactionEventDto;
import io.xrex.dto.event.TransferRingBufferEvent;
import io.xrex.dto.raft.BatchCommand;
import io.xrex.dto.raft.QueryCommand;
import io.xrex.service.ConfigService;
import io.xrex.service.DisruptorPartitionManager;
import io.xrex.service.RocksDBService;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.*;

/**
 * A Raft-managed state machine for processing balance transfers.
 * This state machine is the core of the consensus mechanism, ensuring that all nodes in the
 * Raft cluster agree on the state of account balances. It receives transaction commands,
 * applies them to a local RocksDB instance, and uses a partitioned LMAX Disruptor for
 * high-throughput, low-latency processing of balance updates.
 */
@Slf4j
public class BalanceStateMachine extends BaseStateMachine {

    // 1. Modify the Translator type to EventTranslatorOneArg and focus on processing a single DTO
    private static final EventTranslatorOneArg<TransferRingBufferEvent, TransactionEventDto> TRANSACTION_EVENT_TRANSLATOR =
            (event, sequence, eventData) -> {
                event.setTransactionId(eventData.getTransactionId());
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
    private static final ExecutorService VIRTUAL_THREAD_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();
    private final Map<String, Long> clientSequenceIds = new ConcurrentHashMap<>();
    private final KafkaTemplate<String, TransactionEventDto> kafkaTemplate;
    private final ConfigService configService;
    private final RocksDBService rocksDBService;
    private RocksDB db;
    private File dbDir;
    private RaftGroupId groupId;
    @Value("${app.kafka.transfer-persistence-event.topic}")
    private String topic;

    @Value("${raft.state-machine.max-in-flight-requests:3000}")
    private Integer maxInFlightRequests;

    private Semaphore inFlightRequestsSemaphore;
    private DisruptorPartitionManager disruptorPartitionManager;
    private BalanceUpdateEventHandlerFactory balanceUpdateEventHandlerFactory;

    public BalanceStateMachine(KafkaTemplate<String, TransactionEventDto> kafkaTemplate,
                               ConfigService configService, RocksDBService rocksDBService) {
        this.kafkaTemplate = kafkaTemplate;
        this.configService = configService;
        this.rocksDBService = rocksDBService;
    }

    /**
     * Initializes the state machine. This method is called by the Raft framework when the server starts.
     * It sets up the RocksDB instance, initializes the Disruptor partition manager, and prepares the
     * semaphore for controlling in-flight requests.
     *
     * @param server  The Raft server instance.
     * @param groupId The ID of the Raft group.
     * @param storage The storage for the Raft log and state machine.
     * @throws IOException if an I/O error occurs.
     */
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

        this.inFlightRequestsSemaphore = new Semaphore(maxInFlightRequests);
        log.info("Initialized in-flight request semaphore with {} permits", maxInFlightRequests);

        // Initialize partitioned disruptor
        this.balanceUpdateEventHandlerFactory = new BalanceUpdateEventHandlerFactory(this.kafkaTemplate, this.configService, this.rocksDBService, this.inFlightRequestsSemaphore);
        this.disruptorPartitionManager = new DisruptorPartitionManager(this.balanceUpdateEventHandlerFactory, this.topic, this.configService);
        this.disruptorPartitionManager.initialize();
    }

    /**
     * Applies a transaction to the state machine. This is the entry point for all write operations.
     * The method decodes the transaction command, checks for duplicates, and then publishes the
     * transaction events to the appropriate Disruptor ring buffer.
     *
     * @param trx The transaction context, containing the log entry.
     * @return A CompletableFuture that completes with a message indicating the result of the operation.
     */
    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        try {
            final byte[] logData = trx.getStateMachineLogEntry().getLogData().toByteArray();
            final BatchCommand command = JSON.parseObject(logData, BatchCommand.class);

            if (command == null) {
                log.error("[BalanceStateMachine] Failed to deserialize BatchCommand. JSON data might be invalid or reflection config missing.");
                return CompletableFuture.completedFuture(Message.valueOf("Deserialization Error"));
            }

            final String clientId = command.getClientId();
            final long sequenceId = command.getSequenceId();

            if (isDuplicate(clientId, sequenceId)) {
                log.error("[BalanceStateMachine] Duplicate request detected. ClientId={}, SequenceId={}", clientId, sequenceId);
                return CompletableFuture.completedFuture(Message.valueOf("Duplicate request"));
            }

            if (command.getEvents() == null) {
                log.error("[BalanceStateMachine] BatchCommand events list is null. ClientId={}, SequenceId={}", clientId, sequenceId);
                return CompletableFuture.completedFuture(Message.valueOf("Invalid Command: events is null"));
            }

            // 2. Modify the publishing logic: iterate through the DTO list and publish an event for each DTO
            for (TransactionEventDto eventDto : command.getEvents()) {
                try {
                    // Acquire a permit before publishing. This will block if the system is overloaded.
                    if (!inFlightRequestsSemaphore.tryAcquire(1, 10, TimeUnit.SECONDS)) {
                        log.error("Timeout acquiring semaphore permit. System is overloaded. Rejecting transaction for transactionId: {}", eventDto.getTransactionId());
                        // We can't easily fail just one part of a batch. Failing the whole batch.
                        return CompletableFuture.completedFuture(Message.valueOf("System overloaded. Please try again later."));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("Interrupted while waiting for semaphore permit. Rejecting transaction.", e);
                    return CompletableFuture.completedFuture(Message.valueOf("Transaction interrupted."));
                }

                if (eventDto == null) {
                     log.warn("[BalanceStateMachine] Encountered null eventDto in batch. Skipping.");
                     inFlightRequestsSemaphore.release();
                     continue;
                }

                if (eventDto.getFrom() == null) {
                     log.error("[BalanceStateMachine] eventDto.getFrom() is null. TransactionId={}", eventDto.getTransactionId());
                     inFlightRequestsSemaphore.release();
                     continue; // Or fail the batch
                }

                String coinSymbol = eventDto.getFrom().getCoinSymbol();
                if (coinSymbol == null) {
                    log.error("[BalanceStateMachine] Coin symbol is null. TransactionId={}", eventDto.getTransactionId());
                    inFlightRequestsSemaphore.release();
                    continue;
                }
                
                RingBuffer<TransferRingBufferEvent> ringBuffer = disruptorPartitionManager.getRingBuffer(coinSymbol);
                if (ringBuffer != null) {
                    ringBuffer.publishEvent(TRANSACTION_EVENT_TRANSLATOR, eventDto);
                } else {
                    // If there's no ring buffer, we must release the permit we just acquired.
                    inFlightRequestsSemaphore.release();
                    log.error("No ring buffer found for coin symbol: {}. Releasing permit.", coinSymbol);
                    // Handle error: maybe push to a default queue or reject the transaction
                }
            }
            clientSequenceIds.put(clientId, sequenceId);
            log.debug("[BalanceStateMachine] {} events applyTransaction END. Returning OK to Raft framework.", command.getEvents().size());
            return CompletableFuture.completedFuture(Message.valueOf("OK"));
        } catch (Exception e) {
            log.error("[BalanceStateMachine] Unexpected error in applyTransaction.", e);
            return CompletableFuture.completedFuture(Message.valueOf("Internal Server Error: " + e.getMessage()));
        }
    }

    /**
     * Executes a query against the state machine. This is the entry point for all read operations.
     * The method supports different read consistencies (STRONG, BOUNDED, EVENTUAL).
     *
     * @param request The query request message.
     * @return A CompletableFuture that completes with the query result.
     */
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

    /**
     * Performs a strongly consistent query by reading directly from the local RocksDB instance.
     *
     * @param accountId The account to query.
     * @return A CompletableFuture with the balance.
     */
    private CompletableFuture<Message> queryStrong(AccountIdDto accountId) {
        return CompletableFuture.completedFuture(Message.valueOf(readFromRocksDB(accountId).toString()));
    }

    /**
     * Performs a boundedly stale query. If the replica is not too far behind the leader,
     * it reads from the local RocksDB. Otherwise, it performs a strong query.
     *
     * @param accountId The account to query.
     * @return A CompletableFuture with the balance.
     */
    private CompletableFuture<Message> queryBounded(AccountIdDto accountId) {
        long lag = getLag();
        if (lag < 10) {
            return CompletableFuture.completedFuture(Message.valueOf(readFromRocksDB(accountId).toString()));
        } else {
            return queryStrong(accountId);
        }
    }

    /**
     * Performs an eventually consistent query using the RocksDBService, which may use a cache.
     *
     * @param accountId The account to query.
     * @return A CompletableFuture with the balance.
     */
    private CompletableFuture<Message> queryEventual(AccountIdDto accountId) {
        return CompletableFuture.supplyAsync(() -> {
            BigDecimal balance = rocksDBService.queryEventual(accountId);
            return Message.valueOf(balance.toPlainString());
        }, VIRTUAL_THREAD_EXECUTOR);
    }

    /**
     * Reads an account's balance directly from the local RocksDB instance.
     *
     * @param accountId The account to read.
     * @return The account balance, or BigDecimal.ZERO if the account is not found.
     */
    private BigDecimal readFromRocksDB(AccountIdDto accountId) {
        try {
            byte[] key = JSON.toJSONBytes(accountId);
            byte[] value = db.get(key);
            return (value != null) ? new BigDecimal(new String(value, StandardCharsets.UTF_8)) : BigDecimal.ZERO;
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to query balance from RocksDB", e);
        }
    }

    /**
     * Calculates the lag of this state machine replica behind the leader.
     *
     * @return The lag in terms of log index.
     */
    private long getLag() {
        try {
            return getServer().get().getDivision(groupId).getStateMachine().getLastAppliedTermIndex().getIndex() - getLastAppliedTermIndex().getIndex();
        } catch (Exception e) {
            return Long.MAX_VALUE;
        }
    }

    /**
     * Takes a snapshot of the current state machine state.
     * This is done by creating a checkpoint of the RocksDB database.
     *
     * @return The log index of the last applied transaction included in the snapshot.
     * @throws IOException if an I/O error occurs during snapshot creation.
     */
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

    /**
     * Checks if a request is a duplicate based on the client ID and sequence ID.
     *
     * @param clientId   The ID of the client that sent the request.
     * @param sequenceId The sequence ID of the request.
     * @return true if the request is a duplicate, false otherwise.
     */
    private boolean isDuplicate(String clientId, long sequenceId) {
        return clientSequenceIds.getOrDefault(clientId, -1L) >= sequenceId;
    }

    /**
     * Closes the state machine and releases all resources.
     * This includes shutting down the Disruptor and closing the RocksDB instance.
     *
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public void close() throws IOException {
        if (disruptorPartitionManager != null) {
            disruptorPartitionManager.shutdown();
        }
        if (db != null) {
            db.close();
        }
        super.close();
    }
}
