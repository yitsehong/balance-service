package io.xrex.service;

import io.xrex.model.dto.event.TransactionEventDto;
import io.xrex.model.entity.LedgerBookEntity;
import io.xrex.repository.LedgerBookDao;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Stream;

/**
 * This service is responsible for creating and persisting ledger book entries.
 * Ledger book entries are detailed records of every transaction, capturing both the debit and credit sides.
 */
@Service
@RequiredArgsConstructor
public class LedgerBookService {

    private final LedgerBookDao ledgerBookDao;

    /**
     * Asynchronously creates and batch-inserts ledger book entries from a list of transaction events.
     * This method is executed in a separate thread to avoid blocking the caller.
     *
     * @param events A list of TransactionEventDto objects, each representing a single transaction.
     */
    @Async
    public void produceLedgerBook(List<TransactionEventDto> events) {
        List<LedgerBookEntity> ledgerBooks = events.stream().flatMap(event -> Stream.of(event.getFrom(), event.getTo())).toList();
        ledgerBookDao.batchInsert(ledgerBooks);
    }

}
