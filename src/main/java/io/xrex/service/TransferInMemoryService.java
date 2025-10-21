package io.xrex.service;

import com.lmax.disruptor.RingBuffer;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.xrex.event.disruptor.TransferRingBufferEvent;
import io.xrex.exception.InsufficientFundsException;
import io.xrex.grpc.TransferRequest;
import io.xrex.grpc.TransferResponse;
import io.xrex.util.SnowflakeIdGenerator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransferInMemoryService {

    private final RingBuffer<TransferRingBufferEvent> ringBuffer;
    private final SnowflakeIdGenerator snowflakeIdGenerator;

    /**
     * Asynchronously processes a transfer request.
     * This method is non-blocking. It publishes the request to the Disruptor RingBuffer
     * and uses a CompletableFuture callback to send the response via the StreamObserver.
     *
     * @param request The gRPC transfer request.
     * @param responseObserver The observer to send the response to.
     */
    public void transfer(TransferRequest request, StreamObserver<TransferResponse> responseObserver) {
        // The future now completes with the eventKey (String) on success
        final CompletableFuture<String> future = new CompletableFuture<>();
        final String eventKey = snowflakeIdGenerator.nextIdString();

        // Publish the event to the Disruptor
        long sequence = ringBuffer.next();
        try {
            TransferRingBufferEvent event = ringBuffer.get(sequence);
            event.setEventKey(eventKey);
            event.setFromChainupId(request.getFromUid());
            event.setFromAssetType(request.getFromType());
            event.setToChainupId(request.getToUid());
            event.setToAssetType(request.getToType());
            event.setAmount(new BigDecimal(request.getAmount()));
            event.setScene(request.getScene());
            event.setMeta(request.getMeta());
            event.setRefType(request.getRefType());
            event.setRefId(request.getRefId());
            event.setOpUid(request.getOpUid());
            event.setOpIp(request.getOpIp());
            event.setFuture(future);
        } finally {
            ringBuffer.publish(sequence);
        }

        // Register a non-blocking callback to handle the result
        future.whenComplete((resultEventKey, throwable) -> {
            if (throwable != null) {
                // An exception occurred during processing
                log.error("Event [{}] failed processing", eventKey, throwable);
                Status status;
                if (throwable instanceof InsufficientFundsException) {
                    status = Status.FAILED_PRECONDITION.withDescription(throwable.getMessage());
                } else {
                    status = Status.INTERNAL.withDescription("Internal error: " + throwable.getMessage());
                }
                responseObserver.onError(status.asRuntimeException());
            } else {
                // In-memory processing was successful
                TransferResponse response = TransferResponse.newBuilder()
                        .setSuccess(true)
                        .setEventKey(resultEventKey)
                        .setMessage("Transfer accepted for processing.")
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        });
    }
}

