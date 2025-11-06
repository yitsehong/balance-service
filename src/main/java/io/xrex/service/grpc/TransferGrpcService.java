package io.xrex.service.grpc;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.xrex.enums.ReadConsistency;
import io.xrex.grpc.*;
import io.xrex.model.dto.AccountIdDto;
import io.xrex.model.dto.event.TransactionEventDto;
import io.xrex.model.entity.AccountEntity;
import io.xrex.model.entity.ConfigAccountTypeEntity;
import io.xrex.model.entity.LedgerBookEntity;
import io.xrex.repository.AccountRepository;
import io.xrex.service.ConfigService;
import io.xrex.service.raft.BatchTransferProcessorService;
import io.xrex.service.raft.CustomRaftClient;
import io.xrex.service.raft.TransferRaftRequest;
import io.xrex.util.SnowflakeIdGenerator;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.apache.ratis.protocol.RaftClientReply;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.xrex.util.XrexConstant.ISO_DATE_FORMATTER;

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
    private final ConfigService configService;
    private final AccountRepository accountRepository;

    public TransferGrpcService(BatchTransferProcessorService batchTransferProcessorService, CustomRaftClient raftClient,
                               SnowflakeIdGenerator snowflakeIdGenerator, ConfigService configService,
                               AccountRepository accountRepository) {
        this.batchTransferProcessorService = batchTransferProcessorService;
        this.raftClient = raftClient;
        this.snowflakeIdGenerator = snowflakeIdGenerator;
        this.configService = configService;
        this.accountRepository = accountRepository;
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


    /**
     * 處理餘額查詢請求，使用 EVENTUAL 一致性從 Raft 狀態機讀取。
     */
    @Override
    public void getLedgerFromMemory(LedgerRequest request, StreamObserver<LedgerResponse> responseObserver) {
        AccountIdDto accountId = findAccountIdByChainupIdAndAssetType(request.getChainupId(), request.getType());
        raftClient.queryBalance(accountId, ReadConsistency.EVENTUAL)
                .whenComplete((reply, ex) -> {
                    if (ex != null) {
                        responseObserver.onError(ex);
                    } else {
                        String balance = reply.getMessage().getContent().toStringUtf8();
                        LedgerResponse response = LedgerResponse.newBuilder()
                                .setChainupId(accountId.getChainupId())
                                .setType(accountId.getAssetType())
                                .setCurrency(accountId.getCoinSymbol())
                                .setTag(accountId.getAccountTag())
                                .setBalance(balance).build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                    }
                });
    }

    @Override
    public void getLedgerFromDB(LedgerRequest request, StreamObserver<LedgerResponse> responseObserver) {
        try {
            AccountEntity accountEntity = accountRepository.findByUidAndType(request.getChainupId(), request.getType());
            LedgerResponse response = toLedgerResponse(accountEntity);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getBalanceFromDB(BalanceRequest request, StreamObserver<BalanceResponse> responseObserver) {
        try {
            List<AccountEntity> accounts = accountRepository.findByUid(request.getChainupId());
            List<LedgerResponse> results = new ArrayList<>();
            for (AccountEntity account : accounts) {
                results.add(toLedgerResponse(account));
            }

            Map<String, List<LedgerResponse>> result = results.stream().collect(Collectors.groupingBy(LedgerResponse::getCurrency));
            BalanceResponse.Builder responseBuilder = BalanceResponse.newBuilder();
            for (Map.Entry<String, List<LedgerResponse>> entry : result.entrySet()) {
                LedgerListResponse ledgers = LedgerListResponse.newBuilder().addAllLedgers(entry.getValue()).build();
                responseBuilder.putCoinLedgers(entry.getKey(), ledgers);
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
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

    private AccountIdDto findAccountIdByChainupIdAndAssetType(Integer chainupId, Integer assetType) {
        ConfigAccountTypeEntity config = configService.findByAssetType(assetType);
        return AccountIdDto.builder().chainupId(chainupId)
                .assetType(config.getAssetType())
                .coinSymbol(config.getCoinSymbol())
                .accountTag(config.getTag()).build();
    }

    private LedgerResponse toLedgerResponse(AccountEntity accountEntity) {
        ConfigAccountTypeEntity configAccountType = configService.findByAssetType(accountEntity.getType());
        return LedgerResponse.newBuilder()
                .setId(accountEntity.getId())
                .setChainupId(accountEntity.getUid())
                .setType(accountEntity.getType())
                .setCurrency(configAccountType.getCoinSymbol())
                .setTag(configAccountType.getTag())
                .setBalance(accountEntity.getBalance().toPlainString())
                .setCreatedTime(accountEntity.getCtime().format(ISO_DATE_FORMATTER))
                .setUpdatedTime(accountEntity.getMtime().format(ISO_DATE_FORMATTER)).build();
    }
}
