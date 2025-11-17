package io.xrex.service.grpc;

import com.google.common.base.Strings;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.xrex.enums.ErrorCodes;
import io.xrex.enums.ReadConsistency;
import io.xrex.grpc.*;
import io.xrex.dto.AccountIdDto;
import io.xrex.dto.IdempotencyRecordDto;
import io.xrex.dto.event.TransactionEventDto;
import io.xrex.persistence.entity.AccountEntity;
import io.xrex.persistence.entity.ConfigAccountTypeEntity;
import io.xrex.persistence.entity.LedgerBookEntity;
import io.xrex.persistence.repository.AccountRepository;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static io.xrex.util.XrexConstant.ISO_DATE_FORMATTER;

/**
 * Implements the gRPC service for handling transfers and balance queries.
 * This service provides API endpoints for creating transfers and retrieving ledger information.
 * It is fully integrated with the Raft state machine for consensus and fault tolerance.
 */
@Slf4j
@GrpcService
public class TransferGrpcService extends TransferServiceGrpc.TransferServiceImplBase {

    private static final ExecutorService virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();

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
     * Processes a transfer request by submitting it to the Raft batch processor.
     * This method is idempotent, meaning that submitting the same request multiple times
     * will not result in duplicate transfers.
     *
     * @param request The transfer request, containing a list of individual transfers.
     * @param responseObserver The observer to which the response is sent.
     */
    @Override
    public void transfer(TransferListRequest request, StreamObserver<TransferResponse> responseObserver) {
        final String requestId = request.getRequestId();
        if (Strings.isNullOrEmpty(requestId)) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("request_id is required for idempotency.")
                    .asRuntimeException());
            return;
        }

        Optional<IdempotencyRecordDto> recordOpt = idempotencyService.findRecord(requestId);
        if (recordOpt.isPresent()) {
            IdempotencyRecordDto record = recordOpt.get();
            if ("SUCCESS".equals(record.getStatus())) {
                try {
                    TransferResponse.Builder builder = TransferResponse.newBuilder();
                    JsonFormat.parser().merge(record.getResponseData(), builder);
                    responseObserver.onNext(builder.build());
                    responseObserver.onCompleted();
                    return;
                } catch (Exception e) {
                    log.error("Failed to parse cached response for requestId: {}", requestId, e);
                    // Fall through to re-process the request if parsing fails
                }
            }
        }

        List<CompletableFuture<String>> processingFutures = new ArrayList<>();
        for (TransferRequest grpcRequest : request.getRequestsList()) {
            processingFutures.add(processAndSaveTransfer(grpcRequest, requestId));
        }

        if (processingFutures.isEmpty()) {
            // No requests to process
            TransferResponse response = TransferResponse.newBuilder()
                    .setCode(ErrorCodes.SUCCESS.getCode())
                    .setDesc(ErrorCodes.SUCCESS.getDescription()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        CompletableFuture<Void> allFutures = CompletableFuture.allOf(processingFutures.toArray(new CompletableFuture[0]));

        allFutures.whenCompleteAsync((voidResult, throwable) -> {
            if (throwable != null) {
                // This block now runs on a virtual thread.
                log.error("Error processing batch transfer via Raft", throwable);
                IdempotencyRecordDto failRecord = new IdempotencyRecordDto("FAILED", throwable.getMessage());
                idempotencyService.saveRecord(requestId, failRecord); // Blocking I/O
                Status status = Status.INTERNAL.withDescription("Internal error: " + throwable.getMessage());
                responseObserver.onError(status.asRuntimeException());
            } else {
                // This block also runs on a virtual thread.
                List<String> transactionIds = processingFutures.stream().map(CompletableFuture::join).toList();

                TransferResponse response = TransferResponse.newBuilder()
                        .setCode(ErrorCodes.SUCCESS.getCode())
                        .setData(TransferData.newBuilder().addAllTransactionIds(transactionIds).build())
                        .setDesc("All transfers submitted to Raft cluster for processing.").build();

                try {
                    String responseJson = JsonFormat.printer().print(response);
                    IdempotencyRecordDto successRecord = new IdempotencyRecordDto("SUCCESS", responseJson);
                    idempotencyService.saveRecord(requestId, successRecord); // Blocking I/O
                } catch (Exception e) {
                    log.error("Failed to serialize response for idempotency record, requestId: {}", requestId, e);
                    // Continue to send response to client even if caching fails
                }

                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        }, virtualThreadExecutor);
    }

    /**
     * Processes and saves a single transfer request.
     * This method generates a unique event key for the transfer, submits it to the Raft cluster,
     * and saves an idempotency record upon completion.
     *
     * @param grpcRequest The individual transfer request.
     * @param requestId The idempotency key for the overall request.
     * @return A CompletableFuture that will complete with the event key of the transfer.
     */
    private CompletableFuture<String> processAndSaveTransfer(TransferRequest grpcRequest, String requestId) {
        final String transactionId = snowflakeIdGenerator.nextIdString();

        CompletableFuture<RaftClientReply> raftFuture = processSingleTransfer(grpcRequest, transactionId);
        CompletableFuture<String> resultFuture = new CompletableFuture<>();
        raftFuture.whenComplete((_, ex) -> {
            if (ex != null) {
                log.error("Transfer failed for requestId: {}", requestId, ex);
                resultFuture.completeExceptionally(ex);
            } else {
                resultFuture.complete(transactionId);
            }
        });
        return resultFuture;
    }


    /**
     * Retrieves ledger information from memory with eventual consistency.
     * This method queries the Raft state machine for the balance.
     *
     * @param request The ledger request, containing the account ID.
     * @param responseObserver The observer to which the response is sent.
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
                        Ledger ledger = Ledger.newBuilder().setChainupId(accountId.getChainupId())
                                .setType(accountId.getAssetType())
                                .setCurrency(accountId.getCoinSymbol())
                                .setTag(accountId.getAccountTag())
                                .setBalance(balance).build();
                        LedgerResponse response = LedgerResponse.newBuilder()
                                .setCode(ErrorCodes.SUCCESS.getCode())
                                .setDesc(ErrorCodes.SUCCESS.getDescription())
                                .setData(ledger).build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                    }
                });
    }

    /**
     * Retrieves ledger information from the database.
     * This provides strongly consistent data but may have higher latency.
     *
     * @param request The ledger request.
     * @param responseObserver The observer for the response.
     */
    @Override
    public void getLedgerFromDB(LedgerRequest request, StreamObserver<LedgerResponse> responseObserver) {
        try {
            AccountEntity accountEntity = accountRepository.findByUidAndType(request.getChainupId(), request.getType());
            Ledger ledger = toLedger(accountEntity);
            LedgerResponse response = LedgerResponse.newBuilder()
                    .setCode(ErrorCodes.SUCCESS.getCode())
                    .setDesc(ErrorCodes.SUCCESS.getDescription())
                    .setData(ledger).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    /**
     * Retrieves all balances for a given user from the database.
     *
     * @param request The balance request.
     * @param responseObserver The observer for the response.
     */
    @Override
    public void getBalanceFromDB(BalanceRequest request, StreamObserver<BalanceResponse> responseObserver) {
        try {
            List<AccountEntity> accounts = accountRepository.findByUid(request.getChainupId());
            List<Ledger> ledgers = new ArrayList<>();
            for (AccountEntity account : accounts) {
                ledgers.add(toLedger(account));
            }

            Map<String, List<Ledger>> result = ledgers.stream().collect(Collectors.groupingBy(Ledger::getCurrency));
            BalanceResponse.Builder responseBuilder = BalanceResponse.newBuilder()
                    .setCode(ErrorCodes.SUCCESS.getCode()).setDesc(ErrorCodes.SUCCESS.getDescription());
            for (Map.Entry<String, List<Ledger>> entry : result.entrySet()) {
                LedgerListResponse ledgerListResponse = LedgerListResponse.newBuilder().addAllLedgers(entry.getValue()).build();
                responseBuilder.putData(entry.getKey(), ledgerListResponse);
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    /**
     * Processes a single transfer by creating ledger entries and submitting them to the Raft service.
     *
     * @param grpcRequest The gRPC transfer request.
     * @param transactionId A unique key for the event.
     * @return A CompletableFuture that completes with the Raft client reply.
     */
    private CompletableFuture<RaftClientReply> processSingleTransfer(TransferRequest grpcRequest, String transactionId) {
        BigDecimal amount = new BigDecimal(grpcRequest.getAmount());

        ConfigAccountTypeEntity configAccountType = configService.findByAssetType(grpcRequest.getFromType());
        LedgerBookEntity fromLedger = LedgerBookEntity.builder()
                .idempotencyKey(transactionId)
                .chainupId(grpcRequest.getFromUid())
                .assetType(grpcRequest.getFromType())
                .coinSymbol(configAccountType.getCoinSymbol())
                .amount(amount.negate()).scene(grpcRequest.getScene())
                .refType(grpcRequest.getRefType()).refId(grpcRequest.getRefId()).build();

        LedgerBookEntity toLedger = LedgerBookEntity.builder()
                .idempotencyKey(transactionId)
                .chainupId(grpcRequest.getToUid())
                .assetType(grpcRequest.getToType())
                .coinSymbol(configAccountType.getCoinSymbol())
                .amount(amount).scene(grpcRequest.getScene())
                .refType(grpcRequest.getRefType()).refId(grpcRequest.getRefId()).build();

        TransactionEventDto event = TransactionEventDto.builder()
                .transactionId(transactionId).from(fromLedger).to(toLedger)
                .meta(grpcRequest.getMeta())
                .opUid(grpcRequest.getOpUid()).opIp(grpcRequest.getOpIp()).build();
        TransferRaftRequest transferRaftRequest = new TransferRaftRequest(event);
        return batchTransferProcessorService.getBatchProcessor().submit(transferRaftRequest);
    }

    /**
     * Finds an account ID DTO by its chainup ID and asset type.
     *
     * @param chainupId The user's chainup ID.
     * @param assetType The type of the asset.
     * @return An AccountIdDto.
     */
    private AccountIdDto findAccountIdByChainupIdAndAssetType(Integer chainupId, Integer assetType) {
        ConfigAccountTypeEntity config = configService.findByAssetType(assetType);
        return AccountIdDto.builder().chainupId(chainupId)
                .assetType(config.getAssetType())
                .coinSymbol(config.getCoinSymbol())
                .accountTag(config.getTag()).build();
    }

    /**
     * Converts an AccountEntity to a LedgerResponse.
     *
     * @param accountEntity The account entity to convert.
     * @return A Ledger.
     */
    private Ledger toLedger(AccountEntity accountEntity) {
        ConfigAccountTypeEntity configAccountType = configService.findByAssetType(accountEntity.getType());
        return Ledger.newBuilder().setId(accountEntity.getId())
                .setChainupId(accountEntity.getUid())
                .setType(accountEntity.getType())
                .setCurrency(configAccountType.getCoinSymbol())
                .setTag(configAccountType.getTag())
                .setBalance(accountEntity.getBalance().toPlainString())
                .setCreatedTime(accountEntity.getCtime().format(ISO_DATE_FORMATTER))
                .setUpdatedTime(accountEntity.getMtime().format(ISO_DATE_FORMATTER)).build();
    }
}
