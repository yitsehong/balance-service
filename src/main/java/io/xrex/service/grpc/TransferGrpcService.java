package io.xrex.service.grpc;

import com.alibaba.fastjson2.JSON;
import com.google.common.base.Strings;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.xrex.enums.ReadConsistency;
import io.xrex.grpc.*;
import io.xrex.model.dto.AccountIdDto;
import io.xrex.model.dto.IdempotencyRecordDto;
import io.xrex.model.dto.event.TransactionEventDto;
import io.xrex.model.entity.AccountEntity;
import io.xrex.model.entity.ConfigAccountTypeEntity;
import io.xrex.model.entity.LedgerBookEntity;
import io.xrex.repository.AccountRepository;
import io.xrex.service.ConfigService;
import io.xrex.service.IdempotencyService;
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
import java.util.Optional;
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
    private final IdempotencyService idempotencyService;

    public TransferGrpcService(BatchTransferProcessorService batchTransferProcessorService, CustomRaftClient raftClient,
                               SnowflakeIdGenerator snowflakeIdGenerator, ConfigService configService,
                               AccountRepository accountRepository, IdempotencyService idempotencyService) {
        this.batchTransferProcessorService = batchTransferProcessorService;
        this.raftClient = raftClient;
        this.snowflakeIdGenerator = snowflakeIdGenerator;
        this.configService = configService;
        this.accountRepository = accountRepository;
        this.idempotencyService = idempotencyService;
    }

    /**
     * 處理轉帳請求，將其提交到 Raft 批次處理器。
     * 此方法現在具備冪等性。
     */
    @Override
    public void transfer(TransferListRequest request, StreamObserver<TransferResponse> responseObserver) {
        List<String> cachedEventKeys = new ArrayList<>();
        List<CompletableFuture<String>> processingFutures = new ArrayList<>();

        for (TransferRequest grpcRequest : request.getRequestsList()) {
            // 1. 檢查 request_id
            if (Strings.isNullOrEmpty(grpcRequest.getRequestId())) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("request_id is required for idempotency.")
                        .asRuntimeException());
                return;
            }

            // 2. 檢查冪等性紀錄
            Optional<IdempotencyRecordDto> recordOpt = idempotencyService.findRecord(grpcRequest.getRequestId());

            if (recordOpt.isPresent()) {
                // 3a. 紀錄已存在，直接從快取處理
                IdempotencyRecordDto record = recordOpt.get();
                if ("SUCCESS".equals(record.getStatus())) {
                    TransferResponse cachedResponse = JSON.parseObject(record.getResponseData(), TransferResponse.class);
                    cachedEventKeys.addAll(cachedResponse.getEventKeyList());
                }
                // 如果是失敗的紀錄，我們這次將其視為新請求重新處理
            } else {
                // 3b. 新請求，提交給Raft處理
                processingFutures.add(processAndSaveTransfer(grpcRequest));
            }
        }

        if (processingFutures.isEmpty()) {
            // 所有請求都已快取
            TransferResponse response = TransferResponse.newBuilder()
                    .setSuccess(true)
                    .addAllEventKey(cachedEventKeys)
                    .setMessage("All transfers were already processed.")
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        CompletableFuture.allOf(processingFutures.toArray(new CompletableFuture[0]))
                .whenComplete((voidResult, throwable) -> {
                    if (throwable != null) {
                        log.error("Error processing batch transfer via Raft", throwable);
                        Status status = Status.INTERNAL.withDescription("Internal error: " + throwable.getMessage());
                        responseObserver.onError(status.asRuntimeException());
                    } else {
                        List<String> processedEventKeys = processingFutures.stream()
                                .map(CompletableFuture::join)
                                .toList();

                        List<String> allEventKeys = new ArrayList<>(cachedEventKeys);
                        allEventKeys.addAll(processedEventKeys);

                        TransferResponse response = TransferResponse.newBuilder()
                                .setSuccess(true)
                                .addAllEventKey(allEventKeys)
                                .setMessage("All transfers submitted to Raft cluster for processing.")
                                .build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                    }
                });
    }

    private CompletableFuture<String> processAndSaveTransfer(TransferRequest grpcRequest) {
        final String eventKey = snowflakeIdGenerator.nextIdString();
        final String requestId = grpcRequest.getRequestId();

        CompletableFuture<RaftClientReply> raftFuture = processSingleTransfer(grpcRequest, eventKey);
        CompletableFuture<String> resultFuture = new CompletableFuture<>();

        raftFuture.whenComplete((reply, ex) -> {
            if (ex != null) {
                log.error("Transfer failed for requestId: {}", requestId, ex);
                IdempotencyRecordDto failRecord = new IdempotencyRecordDto("FAILED", ex.getMessage());
                idempotencyService.saveRecord(requestId, failRecord);
                resultFuture.completeExceptionally(ex);
            } else {
                TransferResponse transferResponse = TransferResponse.newBuilder()
                        .setSuccess(true)
                        .addEventKey(eventKey)
                        .build();
                IdempotencyRecordDto successRecord = new IdempotencyRecordDto("SUCCESS", JSON.toJSONString(transferResponse));
                idempotencyService.saveRecord(requestId, successRecord);
                resultFuture.complete(eventKey);
            }
        });

        return resultFuture;
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

    private CompletableFuture<RaftClientReply> processSingleTransfer(TransferRequest grpcRequest, String eventKey) {
        BigDecimal amount = new BigDecimal(grpcRequest.getAmount());

        ConfigAccountTypeEntity configAccountType = configService.findByAssetType(grpcRequest.getFromType());
        LedgerBookEntity fromLedger = LedgerBookEntity.builder()
                .idempotencyKey(eventKey)
                .chainupId(grpcRequest.getFromUid())
                .assetType(grpcRequest.getFromType())
                .coinSymbol(configAccountType.getCoinSymbol())
                .amount(amount.negate()).scene(grpcRequest.getScene())
                .refType(grpcRequest.getRefType()).refId(grpcRequest.getRefId()).build();

        LedgerBookEntity toLedger = LedgerBookEntity.builder()
                .idempotencyKey(eventKey)
                .chainupId(grpcRequest.getToUid())
                .assetType(grpcRequest.getToType())
                .coinSymbol(configAccountType.getCoinSymbol())
                .amount(amount).scene(grpcRequest.getScene())
                .refType(grpcRequest.getRefType()).refId(grpcRequest.getRefId()).build();

        TransactionEventDto event = TransactionEventDto.builder()
                .eventKey(eventKey).from(fromLedger).to(toLedger)
                .meta(grpcRequest.getMeta())
                .opUid(grpcRequest.getOpUid()).opIp(grpcRequest.getOpIp()).build();
        TransferRaftRequest transferRaftRequest = new TransferRaftRequest(event);
        return batchTransferProcessorService.getBatchProcessor().submit(transferRaftRequest);
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
