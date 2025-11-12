package io.xrex.service.raft;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import io.xrex.model.dto.event.TransactionEventDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.protocol.RaftClientReply;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;

/**
 * A processor that uses the LMAX Disruptor pattern to batch transfer requests before sending them to the Raft cluster.
 * This approach is highly efficient for handling a large volume of small requests, as it reduces the overhead
 * of network communication and serialization by grouping multiple requests into a single batch.
 */
@Slf4j
public class BatchTransferProcessor { // No longer implements Runnable

    private final CustomRaftClient raftClient;
    private final int batchSize;
    private final int bufferSize;
    private Disruptor<BatchRaftRequestEvent> disruptor;
    private RingBuffer<BatchRaftRequestEvent> ringBuffer;

    /**
     * The event that is placed on the Disruptor's ring buffer. It wraps a single TransferRaftRequest.
     */
    public static class BatchRaftRequestEvent {
        private TransferRaftRequest request;
        public void set(TransferRaftRequest request) { this.request = request; }
        public TransferRaftRequest get() { return request; }
        public void clear() { request = null; }
    }

    /**
     * The event handler that consumes events from the ring buffer. It collects events into a batch
     * and sends the batch to the Raft client when it's full or when the `endOfBatch` flag is true.
     */
    public static class BatchSendEventHandler implements EventHandler<BatchRaftRequestEvent> {
        private final CustomRaftClient raftClient;
        private final int batchSize;
        private final List<TransferRaftRequest> batch;

        public BatchSendEventHandler(CustomRaftClient raftClient, int batchSize) {
            this.raftClient = raftClient;
            this.batchSize = batchSize;
            this.batch = new ArrayList<>(batchSize);
        }

        @Override
        public void onEvent(BatchRaftRequestEvent event, long sequence, boolean endOfBatch) {
            batch.add(event.get());

            if (endOfBatch || batch.size() >= batchSize) {
                processBatch();
            }
        }

        private void processBatch() {
            if (batch.isEmpty()) {
                return;
            }
            final List<TransferRaftRequest> currentBatch = new ArrayList<>(batch);
            batch.clear();

            log.debug("Processing batch of {} requests via Disruptor.", currentBatch.size());
            List<TransactionEventDto> events = currentBatch.stream()
                    .map(TransferRaftRequest::getEvent)
                    .toList();

            CompletableFuture<RaftClientReply> batchFuture = raftClient.sendBatch(events);

            batchFuture.whenComplete((reply, ex) -> {
                if (ex != null) {
                    log.error("[BatchSendEventHandler] Batch processing failed.", ex);
                    currentBatch.forEach(req -> req.getFuture().completeExceptionally(ex));
                } else {
                    currentBatch.forEach(req -> req.getFuture().complete(reply));
                }
            });
        }
    }

    public BatchTransferProcessor(CustomRaftClient raftClient, int batchSize, int bufferSize) {
        this.raftClient = raftClient;
        this.batchSize = batchSize;
        this.bufferSize = bufferSize;
    }

    /**
     * Starts the Disruptor-based batch processor.
     * This initializes the Disruptor, sets up the event handler, and starts the consumer thread.
     */
    public void start() {
        ThreadFactory threadFactory = r -> {
            Thread t = new Thread(r);
            t.setName("disruptor-batch-processor-thread");
            t.setDaemon(true);
            return t;
        };

        BatchSendEventHandler handler = new BatchSendEventHandler(raftClient, batchSize);

        this.disruptor = new Disruptor<>(
                BatchRaftRequestEvent::new,
                bufferSize,
                threadFactory,
                ProducerType.MULTI,
                new com.lmax.disruptor.BlockingWaitStrategy()
        );

        disruptor.handleEventsWith(handler);
        this.ringBuffer = disruptor.start();
        log.info("Disruptor-based BatchTransferProcessor started with buffer size {}.", bufferSize);
    }

    /**
     * Stops the Disruptor-based batch processor gracefully.
     */
    public void stop() {
        if (disruptor != null) {
            log.info("Shutting down Disruptor-based BatchTransferProcessor...");
            disruptor.shutdown();
            log.info("Disruptor-based BatchTransferProcessor shut down.");
        }
    }

    /**
     * Submits a transfer request to the batch processor.
     * The request is published to the Disruptor's ring buffer and will be processed
     * asynchronously by the event handler.
     *
     * @param raftRequest The transfer request to be processed.
     * @return A CompletableFuture that will be completed with the Raft client's reply
     *         once the batch containing this request has been processed.
     */
    public CompletableFuture<RaftClientReply> submit(TransferRaftRequest raftRequest) {
        if (!disruptor.getRingBuffer().hasAvailableCapacity(1)) {
             log.warn("RingBuffer is full. Rejecting request for eventKey: {}", raftRequest.getEvent().getEventKey());
             raftRequest.getFuture().completeExceptionally(new IllegalStateException("System overloaded. RingBuffer is full."));
             return raftRequest.getFuture();
        }

        this.ringBuffer.publishEvent((event, sequence, request) -> event.set(request), raftRequest);
        return raftRequest.getFuture();
    }
}
