package io.xrex.service.grpc;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.xrex.enums.ReadConsistency;
import io.xrex.grpc.*;
import io.xrex.model.dto.AccountIdDto;
import io.xrex.model.dto.event.TransactionEventDto;
import io.xrex.model.entity.ConfigAccountTypeEntity;
import io.xrex.model.entity.LedgerBookEntity;
import io.xrex.repository.ConfigAccountTypeRepository;
import io.xrex.service.raft.BatchTransferProcessorService;
import io.xrex.service.raft.CustomRaftClient;
import io.xrex.service.raft.TransferRaftRequest;
import io.xrex.util.SnowflakeIdGenerator;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.apache.ratis.protocol.RaftClientReply;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * gRPC 服務的實作，提供轉帳和餘額查詢的 API 端點。
 * 已重構為完全接入 Raft 狀態機。
 */
@Slf4j
@GrpcService
public class TransferGrpcService extends TransferServiceGrpc.TransferServiceImplBase {

    private final BatchTransferProcessorService batchTransferProcessorService;
    private final CustomRaftClient raftClient;
    private final SnowflakeIdGenerator snowflakeIdGenerator;
    private final ConfigAccountTypeRepository configAccountTypeRepository;

    public TransferGrpcService(BatchTransferProcessorService batchTransferProcessorService, CustomRaftClient raftClient, SnowflakeIdGenerator snowflakeIdGenerator, ConfigAccountTypeRepository configAccountTypeRepository) {
        this.batchTransferProcessorService = batchTransferProcessorService;
        this.raftClient = raftClient;
        this.snowflakeIdGenerator = snowflakeIdGenerator;
        this.configAccountTypeRepository = configAccountTypeRepository;
    }

    /**
     * 處理轉帳請求，將其提交到 Raft 批次處理器。
     */
    @Override
    public void transfer(TransferListRequest request, StreamObserver<TransferResponse> responseObserver) {
        List<Map<String, CompletableFuture<RaftClientReply>>> futures = request.getRequestsList().stream()
                .map(this::processSingleTransfer).toList();

        CompletableFuture<?>[] allFutures = futures.stream()
                .flatMap(map -> map.values().stream()).toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(allFutures)
                .whenComplete((voidResult, throwable) -> {
                    if (throwable != null) {
                        log.error("Error processing batch transfer via Raft", throwable);
                        Status status = Status.INTERNAL.withDescription("Internal error: " + throwable.getMessage());
                        responseObserver.onError(status.asRuntimeException());
                    } else {
                        List<String> eventKeys = futures.stream()
                                .flatMap(map -> map.keySet().stream()).toList();

                        TransferResponse response = TransferResponse.newBuilder()
                                .setSuccess(true)
                                .addAllEventKey(eventKeys)
                                .setMessage("All transfers submitted to Raft cluster for processing.")
                                .build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                    }
                });
    }

    private Map<String, CompletableFuture<RaftClientReply>> processSingleTransfer(TransferRequest grpcRequest) {
        final String eventKey = snowflakeIdGenerator.nextIdString();
        BigDecimal amount = new BigDecimal(grpcRequest.getAmount());
        LedgerBookEntity fromLedger = LedgerBookEntity.builder()
                .idempotencyKey(eventKey)
                .chainupId(grpcRequest.getFromUid())
                .assetType(grpcRequest.getFromType())
                .amount(amount.negate()).scene(grpcRequest.getScene())
                .refType(grpcRequest.getRefType()).refId(grpcRequest.getRefId()).build();

        LedgerBookEntity toLedger = LedgerBookEntity.builder()
                .idempotencyKey(eventKey)
                .chainupId(grpcRequest.getToUid())
                .assetType(grpcRequest.getToType())
                .amount(amount).scene(grpcRequest.getScene())
                .refType(grpcRequest.getRefType()).refId(grpcRequest.getRefId()).build();

        TransactionEventDto event = TransactionEventDto.builder()
                .eventKey(eventKey).from(fromLedger).to(toLedger)
                .meta(grpcRequest.getMeta())
                .opUid(grpcRequest.getOpUid()).opIp(grpcRequest.getOpIp()).build();
        TransferRaftRequest transferRaftRequest = new TransferRaftRequest(event);
        CompletableFuture<RaftClientReply> future = batchTransferProcessorService.getBatchProcessor().submit(transferRaftRequest);
        return Map.of(eventKey, future);
    }

    /**
     * 處理餘額查詢請求，使用 EVENTUAL 一致性從 Raft 狀態機讀取。
     */
    @Override
    public void getBalanceFromMemory(BalanceRequest request, StreamObserver<BalanceResponse> responseObserver) {
        ConfigAccountTypeEntity config = configAccountTypeRepository.findByAssetType(request.getType());
        AccountIdDto accountId = AccountIdDto.builder()
                .chainupId(request.getUid())
                .assetType(request.getType())
                .coinSymbol(config != null ? config.getCoinSymbol() : null)
                .accountTag(config != null ? config.getTag() : null)
                .build();

        raftClient.queryBalance(accountId, ReadConsistency.EVENTUAL)
                .whenComplete((reply, ex) -> {
                    if (ex != null) {
                        responseObserver.onError(ex);
                    } else {
                        String balance = reply.getMessage().getContent().toStringUtf8();
                        BalanceResponse response = BalanceResponse.newBuilder()
                                .setUid(request.getUid())
                                .setType(request.getType())
                                .setBalance(balance)
                                .build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                    }
                });
    }

    /**
     * 處理餘額查詢請求，使用 STRONG 一致性從 Raft 狀態機讀取。
     */
    @Override
    public void getBalanceFromDB(BalanceRequest request, StreamObserver<BalanceResponse> responseObserver) {
        ConfigAccountTypeEntity config = configAccountTypeRepository.findByAssetType(request.getType());
        AccountIdDto accountId = AccountIdDto.builder()
                .chainupId(request.getUid())
                .assetType(request.getType())
                .coinSymbol(config != null ? config.getCoinSymbol() : null)
                .accountTag(config != null ? config.getTag() : null)
                .build();

        raftClient.queryBalance(accountId, ReadConsistency.STRONG)
                .whenComplete((reply, ex) -> {
                    if (ex != null) {
                        responseObserver.onError(ex);
                    } else {
                        String balance = reply.getMessage().getContent().toStringUtf8();
                        BalanceResponse response = BalanceResponse.newBuilder()
                                .setUid(request.getUid())
                                .setType(request.getType())
                                .setBalance(balance)
                                .build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                    }
                });
    }
}
