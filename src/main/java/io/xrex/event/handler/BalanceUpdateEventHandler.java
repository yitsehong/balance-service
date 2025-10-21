package io.xrex.event.handler;

import com.lmax.disruptor.EventHandler;
import io.xrex.event.disruptor.TransferRingBufferEvent;
import io.xrex.model.dto.AccountIdDto;
import io.xrex.model.dto.event.TransactionEventDto;
import io.xrex.service.InMemoryBalanceStore;
import io.xrex.service.kafka.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class BalanceUpdateEventHandler implements EventHandler<TransferRingBufferEvent> {

    private final InMemoryBalanceStore inMemoryBalanceStore;
    private final KafkaProducerService kafkaProducerService;

    @Override
    public void onEvent(TransferRingBufferEvent event, long sequence, boolean endOfBatch) {
        try {
            AccountIdDto fromAccountId = new AccountIdDto(event.getFromChainupId(), event.getFromAssetType());
            AccountIdDto toAccountId = new AccountIdDto(event.getToChainupId(), event.getToAssetType());
            if (fromAccountId.equals(toAccountId)) {
                log.warn("Transaction [{}]: From and to accounts are the same. Skipping.", event.getEventKey());
                throw new RuntimeException("From and to accounts are the same. Skipping, eventKey=" + event.getEventKey());
            }
            // 核心內存帳本操作
            TransactionEventDto kafkaEvent = inMemoryBalanceStore.processTransfer(event, fromAccountId, toAccountId);
            kafkaProducerService.sendTransferEvent(kafkaEvent);
            // 處理成功，用 eventKey 完成 future
            event.getFuture().complete(event.getEventKey());
        } catch (Exception e) {
            log.error("Error processing transfer event: {}", event, e);
            // 其他未知異常
            event.getFuture().completeExceptionally(e);
        } finally {
            // 清理 Event 以便重用
            event.clear();
        }
    }
}
