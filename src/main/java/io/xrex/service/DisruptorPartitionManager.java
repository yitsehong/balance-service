package io.xrex.service;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import io.xrex.event.disruptor.TransferRingBufferEvent;
import io.xrex.event.handler.BalanceUpdateEventHandler;
import io.xrex.event.handler.BalanceUpdateEventHandlerFactory;
import io.xrex.model.entity.ConfigAccountTypeEntity;
import io.xrex.model.entity.ConfigCoinSymbolEntity;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class DisruptorPartitionManager {

    private final Map<String, Disruptor<TransferRingBufferEvent>> disruptors = new ConcurrentHashMap<>();
    private final Map<String, RingBuffer<TransferRingBufferEvent>> ringBuffers = new ConcurrentHashMap<>();

    private final BalanceUpdateEventHandlerFactory handlerFactory;
    private final String topic;
    private final ConfigService configService;

    public DisruptorPartitionManager(BalanceUpdateEventHandlerFactory handlerFactory,
                                     String topic, ConfigService configService) {
        this.handlerFactory = handlerFactory;
        this.topic = topic;
        this.configService = configService;
    }

    public void initialize() {
        Map<String, ConfigCoinSymbolEntity> openCoins = configService.findAllOpenCoinMap();
        log.info("Initializing DisruptorPartitionManager for {} coins.", openCoins.size());
        for (Map.Entry<String, ConfigCoinSymbolEntity> entry : openCoins.entrySet()) {
            String coinSymbol = entry.getKey();
            log.info("Creating disruptor for coin: {}", coinSymbol);
            Disruptor<TransferRingBufferEvent> disruptor = new Disruptor<>(
                    TransferRingBufferEvent::new, 1024, DaemonThreadFactory.INSTANCE);

            BalanceUpdateEventHandler handler = handlerFactory.create(topic);
            disruptor.handleEventsWith(handler);

            RingBuffer<TransferRingBufferEvent> ringBuffer = disruptor.start();
            disruptors.put(coinSymbol, disruptor);
            ringBuffers.put(coinSymbol, ringBuffer);
        }
        log.info("DisruptorPartitionManager initialized with partitions for: {}", ringBuffers.keySet());
    }

    public RingBuffer<TransferRingBufferEvent> getRingBuffer(Integer assetType) {
        ConfigAccountTypeEntity config = configService.findByAssetType(assetType);
        if (config == null) {
            log.warn("No config found for assetType: {}. Cannot find RingBuffer.", assetType);
            return null; // Or return a default/global ring buffer
        }
        String coinSymbol = config.getCoinSymbol();
        RingBuffer<TransferRingBufferEvent> buffer = ringBuffers.get(coinSymbol);
        if (buffer == null) {
            log.warn("No RingBuffer partition found for coin: {}. AssetType: {}", coinSymbol, assetType);
        }
        return buffer;
    }

    public void shutdown() {
        log.info("Shutting down all disruptor partitions...");
        for (Disruptor<?> disruptor : disruptors.values()) {
            disruptor.shutdown();
        }
        log.info("All disruptor partitions shut down.");
    }
}
