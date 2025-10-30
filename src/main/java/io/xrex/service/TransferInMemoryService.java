package io.xrex.service;

import com.lmax.disruptor.RingBuffer;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.xrex.event.disruptor.TransferRingBufferEvent;
import io.xrex.grpc.TransferListRequest;
import io.xrex.grpc.TransferRequest;
import io.xrex.grpc.TransferResponse;
import io.xrex.util.SnowflakeIdGenerator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransferInMemoryService {

    private final RingBuffer<TransferRingBufferEvent> ringBuffer;
    private final SnowflakeIdGenerator snowflakeIdGenerator;

    /**
     * Asynchronously processes a list of transfer requests.
     * This method is non-blocking. It publishes each request to the Disruptor RingBuffer
     * and uses CompletableFuture.allOf() to wait for all transfers to complete before sending a single response.
     *
     * @param request          The gRPC request containing a list of transfers.
     * @param responseObserver The observer to send the response to.
     */
    public void transfer(TransferListRequest request, StreamObserver<TransferResponse> responseObserver) {
        List<CompletableFuture<String>> futures = request.getRequestsList().stream()
                .map(this::processSingleTransfer).toList();

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .whenComplete((voidResult, throwable) -> {
                    if (throwable != null) {
                        log.error("Error processing batch transfer", throwable);
                        Status status = Status.INTERNAL.withDescription("Internal error: " + throwable.getMessage());
                        responseObserver.onError(status.asRuntimeException());
                    } else {
                        List<String> eventKeys = futures.stream()
                                .map(CompletableFuture::join)
                                .collect(Collectors.toList());

                        TransferResponse response = TransferResponse.newBuilder()
                                .setSuccess(true)
                                .addAllEventKey(eventKeys)
                                .setMessage("All transfers accepted for processing.")
                                .build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                    }
                });
    }

    /**
     * Processes a single transfer request and returns a CompletableFuture.
     *
     * @param request The gRPC transfer request.
     * @return A CompletableFuture that will be completed with the event key or an exception.
     */
    private CompletableFuture<String> processSingleTransfer(TransferRequest request) {
        final CompletableFuture<String> future = new CompletableFuture<>();
        final String eventKey = snowflakeIdGenerator.nextIdString();

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

        return future;
    }
}

