package io.xrex.event.handler;

import com.lmax.disruptor.EventHandler;
import io.xrex.event.disruptor.TransferRingBufferEvent;
import io.xrex.model.dto.AccountIdDto;
import io.xrex.model.dto.event.TransactionEventDto;
import io.xrex.model.entity.ConfigAccountTypeEntity;
import io.xrex.model.entity.LedgerBookEntity;
import io.xrex.service.ConfigService;
import io.xrex.service.RocksDBService;
import io.xrex.service.kafka.KafkaProducerService;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class BalanceUpdateEventHandler implements EventHandler<TransferRingBufferEvent> {

    private final KafkaProducerService kafkaProducerService;
    private final ConfigService configService;
    private final RocksDBService rocksDBService;
    private final List<TransferRingBufferEvent> batch = new ArrayList<>();

    public BalanceUpdateEventHandler(KafkaProducerService kafkaProducerService, ConfigService configService, RocksDBService rocksDBService) {
        this.kafkaProducerService = kafkaProducerService;
        this.configService = configService;
        this.rocksDBService = rocksDBService;
    }

    @Override
    public void onEvent(TransferRingBufferEvent event, long sequence, boolean endOfBatch) {
        try {
            ConfigAccountTypeEntity fromConfigAccountType = configService.findByAssetType(event.getFromAssetType());
            AccountIdDto fromAccountId = new AccountIdDto(event.getFromChainupId(), fromConfigAccountType);

            ConfigAccountTypeEntity toConfigAccountType = configService.findByAssetType(event.getToAssetType());
            AccountIdDto toAccountId = new AccountIdDto(event.getToChainupId(), toConfigAccountType);
            if (fromAccountId.equals(toAccountId)) {
                log.error("Transaction [{}]: From and to accounts are the same. Skipping.", event.getEventKey());
                // TODO handle exception
                throw new RuntimeException("From and to accounts are the same. Skipping, eventKey=" + event.getEventKey());
            }

            TransactionEventDto kafkaEvent = rocksDBService.updateBalanceOnRocksDB(event, fromAccountId, toAccountId);
            kafkaProducerService.sendTransferEvent(kafkaEvent);
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

