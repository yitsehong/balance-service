package io.xrex.service.raft;

import io.xrex.model.dto.event.TransactionEventDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.protocol.RaftClientReply;
import org.slf4j.MDC;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Slf4j
public class BatchTransferProcessor implements Runnable {

    private final CustomRaftClient raftClient;
    private final BlockingQueue<TransferRaftRequest> queue = new LinkedBlockingQueue<>();
    private final int batchSize = 3000;
    private final long timeout = 300; // 300ms
    private volatile boolean running = true;
    private Thread workerThread;

    public BatchTransferProcessor(CustomRaftClient raftClient) {
        this.raftClient = raftClient;
    }

    public void stop() {
        running = false;
        if (workerThread != null) {
            workerThread.interrupt();
        }
    }

    public CompletableFuture<RaftClientReply> submit(TransferRaftRequest raftRequest) {
        if (!running) {
            raftRequest.getFuture().completeExceptionally(new IllegalStateException("Batch processor is shutting down."));
            return raftRequest.getFuture();
        }
        boolean check = queue.offer(raftRequest);
        if (!check) {
            log.error("Failed to add transfer request, event={}", raftRequest);
            raftRequest.getFuture().completeExceptionally(new IllegalStateException("Queue is full."));
        }
        return raftRequest.getFuture();
    }

    @Override
    public void run() {
        this.workerThread = Thread.currentThread();
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                List<TransferRaftRequest> batch = new ArrayList<>();
                long startTime = System.currentTimeMillis();

                // Drain the queue to form a batch
                TransferRaftRequest firstRequest = queue.poll(timeout, TimeUnit.MILLISECONDS);
                if (firstRequest != null) {
                    batch.add(firstRequest);
                    queue.drainTo(batch, batchSize - 1);
                }


                if (!batch.isEmpty()) {
                    processBatch(batch);
                }
            } catch (InterruptedException e) {
                log.info("BatchTransferProcessor interrupted, shutting down.");
                Thread.currentThread().interrupt(); // Preserve the interrupted status
            }
        }
        log.info("BatchTransferProcessor has stopped.");
    }

    private void processBatch(List<TransferRaftRequest> batch) {
        MDC.put("eventKeys", batch.get(0).getEvent().getEventKey());
        try {
            List<TransactionEventDto> events = batch.stream().map(TransferRaftRequest::getEvent).toList();
            CompletableFuture<RaftClientReply> batchFuture = raftClient.sendBatch(events);
            batchFuture.whenComplete((reply, ex) -> {
                if (ex != null) {
                    log.error("[BatchTransferProcessor] Batch processing failed.", ex);
                    for (TransferRaftRequest request : batch) {
                        request.getFuture().completeExceptionally(ex);
                    }
                } else {
                    for (TransferRaftRequest request : batch) {
                        request.getFuture().complete(reply);
                    }
                }
            });
        } finally {
            // 確保在操作結束後清除 MDC，以防線程重用時數據污染
            MDC.remove("eventKeys");
        }
    }
}
