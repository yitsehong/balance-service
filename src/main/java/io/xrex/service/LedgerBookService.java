package io.xrex.service;

import io.xrex.grpc.TransferRequest;
import io.xrex.model.dto.BalanceDto;
import io.xrex.model.dto.event.TransactionEventDto;
import io.xrex.model.entity.LedgerBookEntity;
import io.xrex.util.UUIDv7Generator;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Service
public class LedgerBookService {

    public TransactionEventDto produceLedgerBook(TransferRequest request, BalanceDto fromAccountBalance, BalanceDto toAccountBalance) {
        String eventKey = UUIDv7Generator.generate();
        BigDecimal amount = new BigDecimal(request.getAmount());
        String scene = request.getScene();
        String refType = request.getRefType();
        Long refId = request.getRefId();

        LocalDateTime now = LocalDateTime.now();
        LedgerBookEntity from = LedgerBookEntity.builder()
                .idempotencyKey(eventKey)
                .chainupId(request.getFromUid())
                .assetType(request.getFromType())
                .amount(amount.negate())
                .beforeBalance(fromAccountBalance.getAmount())
                .afterBalance(fromAccountBalance.getAmount().subtract(amount))
                .coinSymbol(fromAccountBalance.getCoinSymbol())
                .accountTag(fromAccountBalance.getAccountTag())
                .scene(scene)
                .refType(refType)
                .refId(refId)
                .createdTime(now)
                .updatedTime(now).build();

        LedgerBookEntity to = LedgerBookEntity.builder()
                .idempotencyKey(eventKey)
                .chainupId(request.getToUid())
                .assetType(request.getToType())
                .amount(amount)
                .beforeBalance(toAccountBalance.getAmount())
                .afterBalance(toAccountBalance.getAmount().add(amount))
                .coinSymbol(toAccountBalance.getCoinSymbol())
                .accountTag(toAccountBalance.getAccountTag())
                .scene(scene)
                .refType(refType)
                .refId(refId)
                .createdTime(now)
                .updatedTime(now).build();

        return TransactionEventDto.builder().eventKey(eventKey)
                .from(from).to(to).meta(request.getMeta()).opUid(request.getOpUid()).opIp(request.getOpIp()).build();
    }

}
