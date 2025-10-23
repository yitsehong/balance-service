package io.xrex.service;

import io.xrex.model.dto.event.TransactionEventDto;
import io.xrex.model.entity.LedgerBookEntity;
import io.xrex.repository.LedgerBookDao;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Stream;

@Service
@RequiredArgsConstructor
public class LedgerBookService {

    private final LedgerBookDao ledgerBookDao;

    @Async
    public void produceLedgerBook(List<TransactionEventDto> events) {
        List<LedgerBookEntity> ledgerBooks = events.stream().flatMap(event -> Stream.of(event.getFrom(), event.getTo())).toList();
        ledgerBookDao.batchInsert(ledgerBooks);
    }

}
