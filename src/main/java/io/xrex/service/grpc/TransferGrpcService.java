package io.xrex.service.grpc;

import io.grpc.stub.StreamObserver;
import io.xrex.grpc.TransferListRequest;
import io.xrex.grpc.TransferResponse;
import io.xrex.grpc.TransferServiceGrpc;
import io.xrex.service.TransferInMemoryService;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;

@Slf4j
@GrpcService
public class TransferGrpcService extends TransferServiceGrpc.TransferServiceImplBase {

    private final TransferInMemoryService transferInMemoryService;

    public TransferGrpcService(TransferInMemoryService transferInMemoryService) {
        this.transferInMemoryService = transferInMemoryService;
    }

    @Override
    public void transfer(TransferListRequest request, StreamObserver<TransferResponse> responseObserver) {
        // The underlying service now handles the asynchronous response via the observer.
        transferInMemoryService.transfer(request, responseObserver);
    }
}
