package io.xrex.service.raft;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class BatchTransferProcessorService {

    private final BatchTransferProcessor batchTransferProcessor;
    private final ExecutorService executor;

    public BatchTransferProcessorService(
            CustomRaftClient raftClient,
            @Value("${raft.batch-processor.thread-pool.core-size:2}") int corePoolSize,
            @Value("${raft.batch-processor.thread-pool.max-size:2}") int maxPoolSize,
            @Value("${raft.batch-processor.thread-pool.queue-capacity:10000}") int queueCapacity) {

        this.batchTransferProcessor = new BatchTransferProcessor(raftClient);

        // Create a ThreadFactory that names the threads for easier debugging.
        final ThreadFactory threadFactory = new ThreadFactory() {
            private final AtomicLong count = new AtomicLong(0);

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("batch-processor-" + count.getAndIncrement());
                return thread;
            }
        };

        // Create a more robust ThreadPoolExecutor
        this.executor = new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                0L, TimeUnit.MILLISECONDS, // keepAliveTime: not needed for fixed-size pool
                new LinkedBlockingQueue<>(queueCapacity),
                threadFactory);
    }

    @PostConstruct
    public void start() {
        executor.submit(batchTransferProcessor);
    }

    @PreDestroy
    public void stop() {
        executor.shutdownNow();
    }

    public BatchTransferProcessor getBatchProcessor() {
        return batchTransferProcessor;
    }
}
