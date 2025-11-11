package io.xrex.service.raft;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

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

    @PostConstruct
    public void start() {
        batchTransferProcessor.start();
    }

    @PreDestroy
    public void stop() {
        batchTransferProcessor.stop();
    }

    public BatchTransferProcessor getBatchProcessor() {
        return batchTransferProcessor;
    }
}
