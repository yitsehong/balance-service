package io.xrex.service.grpc;

import com.google.protobuf.Timestamp;
import io.grpc.stub.StreamObserver;
import io.xrex.grpc.BalanceRequest;
import io.xrex.grpc.BalanceResponse;
import io.xrex.grpc.TransferListRequest;
import io.xrex.grpc.TransferResponse;
import io.xrex.grpc.TransferServiceGrpc;
import io.xrex.model.dto.AccountIdDto;
import io.xrex.model.dto.BalanceDto;
import io.xrex.model.entity.AccountEntity;
import io.xrex.service.AccountService;
import io.xrex.service.InMemoryBalanceStore;
import io.xrex.service.TransferInMemoryService;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;

import java.time.ZoneOffset;

@Slf4j
@GrpcService
public class TransferGrpcService extends TransferServiceGrpc.TransferServiceImplBase {

    private final TransferInMemoryService transferInMemoryService;
    private final InMemoryBalanceStore inMemoryBalanceStore;
    private final AccountService accountService;


    public TransferGrpcService(TransferInMemoryService transferInMemoryService, InMemoryBalanceStore inMemoryBalanceStore, AccountService accountService) {
        this.transferInMemoryService = transferInMemoryService;
        this.inMemoryBalanceStore = inMemoryBalanceStore;
        this.accountService = accountService;
    }

    @Override
    public void transfer(TransferListRequest request, StreamObserver<TransferResponse> responseObserver) {
        // The underlying service now handles the asynchronous response via the observer.
        transferInMemoryService.transfer(request, responseObserver);
    }

    @Override
    public void getBalanceFromMemory(BalanceRequest request, StreamObserver<BalanceResponse> responseObserver) {
        BalanceDto balance = inMemoryBalanceStore.getBalance(new AccountIdDto(request.getUid(), request.getType()));
        BalanceResponse response = BalanceResponse.newBuilder()
                .setUid(request.getUid())
                .setType(request.getType())
                .setBalance(balance.getAmount().toPlainString())
                .setCurrency(balance.getCoinSymbol())
                .setTag(balance.getAccountTag())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getBalanceFromDB(BalanceRequest request, StreamObserver<BalanceResponse> responseObserver) {
        AccountEntity accountEntity = accountService.findByUidAndType(request.getUid(), request.getType());
        if (accountEntity == null) {
            responseObserver.onError(new RuntimeException("Account not found"));
            return;
        }

        Timestamp createdTimestamp = Timestamp.newBuilder()
                .setSeconds(accountEntity.getCtime().toEpochSecond(ZoneOffset.UTC)).build();

        Timestamp updatedTimestamp = Timestamp.newBuilder()
                .setSeconds(accountEntity.getMtime().toEpochSecond(ZoneOffset.UTC)).build();

        BalanceResponse.Builder responseBuilder = BalanceResponse.newBuilder()
                .setId(accountEntity.getId())
                .setUid(accountEntity.getUid())
                .setType(accountEntity.getType())
                .setBalance(accountEntity.getBalance().toPlainString())
                .setTag(accountEntity.getTag())
                .setCreatedTime(createdTimestamp)
                .setUpdatedTime(updatedTimestamp);

        responseBuilder.setCurrency(inMemoryBalanceStore.getConfigAccountType(accountEntity.getType()).getCoinSymbol());

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
}
