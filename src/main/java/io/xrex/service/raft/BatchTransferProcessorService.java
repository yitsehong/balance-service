package io.xrex.service.raft;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

/**
 * A Spring service that manages the lifecycle of the {@link BatchTransferProcessor}.
 * This service is responsible for starting the processor when the application starts
 * and stopping it gracefully when the application shuts down. It also provides access
 * to the processor instance.
 */
@Service
@DependsOn("raftClient")
public class BatchTransferProcessorService {

    private final BatchTransferProcessor batchTransferProcessor;

    public BatchTransferProcessorService(
            CustomRaftClient raftClient,
            @Value("${raft.batch-processor.batch-size:3000}") int batchSize,
            @Value("${raft.batch-processor.buffer-size:16384}") int bufferSize) {

        this.batchTransferProcessor = new BatchTransferProcessor(raftClient, batchSize, bufferSize);
    }

    /**
     * Starts the batch transfer processor. This method is called automatically
     * by Spring after the service has been initialized.
     */
    @PostConstruct
    public void start() {
        batchTransferProcessor.start();
    }

    /**
     * Stops the batch transfer processor. This method is called automatically
     * by Spring when the application is shutting down.
     */
    @PreDestroy
    public void stop() {
        batchTransferProcessor.stop();
    }

    /**
     * Gets the underlying {@link BatchTransferProcessor} instance.
     *
     * @return The batch transfer processor.
     */
    public BatchTransferProcessor getBatchProcessor() {
        return batchTransferProcessor;
    }
}
