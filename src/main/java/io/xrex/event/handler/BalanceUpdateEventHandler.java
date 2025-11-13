package io.xrex.event.handler;

import com.lmax.disruptor.EventHandler;
import io.xrex.event.disruptor.TransferRingBufferEvent;
import io.xrex.model.dto.AccountIdDto;
import io.xrex.model.dto.event.TransactionEventDto;
import io.xrex.model.entity.ConfigAccountTypeEntity;
import io.xrex.service.ConfigService;
import io.xrex.service.RocksDBService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.Semaphore;

@Slf4j
public class BalanceUpdateEventHandler implements EventHandler<TransferRingBufferEvent> {
    private final String topic;
    private final KafkaTemplate<String, TransactionEventDto> kafkaTemplate;
    private final ConfigService configService;
    private final RocksDBService rocksDBService;
    private final Semaphore inFlightRequestsSemaphore;

    public BalanceUpdateEventHandler(@Value("${app.kafka.balance-transfer.topic}") String topic,
                                     KafkaTemplate<String, TransactionEventDto> kafkaTemplate,
                                     ConfigService configService,
                                     RocksDBService rocksDBService,
                                     Semaphore inFlightRequestsSemaphore) {
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
        this.configService = configService;
        this.rocksDBService = rocksDBService;
        this.inFlightRequestsSemaphore = inFlightRequestsSemaphore;
    }

    @Override
    public void onEvent(TransferRingBufferEvent event, long sequence, boolean endOfBatch) {
        try {
            ConfigAccountTypeEntity fromConfigAccountType = configService.findByAssetType(event.getFromAssetType());
            AccountIdDto fromAccountId = new AccountIdDto(event.getFromChainupId(), fromConfigAccountType);

            ConfigAccountTypeEntity toConfigAccountType = configService.findByAssetType(event.getToAssetType());
            AccountIdDto toAccountId = new AccountIdDto(event.getToChainupId(), toConfigAccountType);
            if (fromAccountId.equals(toAccountId)) {
                log.error("[BalanceUpdateEventHandler] Transaction [{}]: From and to accounts are the same. Skipping.", event.getEventKey());
                // TODO handle exception
                throw new RuntimeException("From and to accounts are the same. Skipping, eventKey=" + event.getEventKey());
            }

            TransactionEventDto persistenceEvent = rocksDBService.updateBalanceOnRocksDB(event, fromAccountId, toAccountId);
            kafkaTemplate.send(topic, event.getEventKey(), persistenceEvent);
            event.getFuture().complete(event.getEventKey());
        } catch (Exception e) {
            log.error("[BalanceUpdateEventHandler] Error processing transfer event: {}", event, e);
            event.getFuture().completeExceptionally(e); // 其他未知異常
        } finally {
            inFlightRequestsSemaphore.release(); // Release the permit
            event.clear(); // 清理 Event 以便重用
        }
    }

}

