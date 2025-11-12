package io.xrex.service;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import io.xrex.event.disruptor.TransferRingBufferEvent;
import io.xrex.event.handler.BalanceUpdateEventHandler;
import io.xrex.event.handler.BalanceUpdateEventHandlerFactory;
import io.xrex.model.entity.ConfigCoinSymbolEntity;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;

/**
 * Manages a collection of LMAX Disruptors, partitioned by coin symbol.
 * Each coin has its own Disruptor instance, ensuring that transactions for different
 * cryptocurrencies are processed independently and in parallel. This class is responsible
 * for initializing, providing access to, and shutting down these disruptors.
 */
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

    /**
     * Initializes a separate Disruptor instance for each open coin symbol.
     * Each disruptor is configured with a dedicated thread and a specific Kafka topic
     * for handling balance updates for that coin.
     */
    public void initialize() {
        Map<String, ConfigCoinSymbolEntity> openCoins = configService.findAllOpenCoinMap();
        log.info("Initializing DisruptorPartitionManager for {} coins.", openCoins.size());
        for (Map.Entry<String, ConfigCoinSymbolEntity> entry : openCoins.entrySet()) {
            String coinSymbol = entry.getKey();
            log.info("Creating disruptor for coin: {}", coinSymbol);

            // 1. Create custom ThreadFactory to include coin in thread name
            final String threadName = "disruptor-partition-" + coinSymbol;
            ThreadFactory threadFactory = r -> {
                Thread t = new Thread(r);
                t.setName(threadName);
                t.setDaemon(true);
                return t;
            };

            Disruptor<TransferRingBufferEvent> disruptor = new Disruptor<>(
                    TransferRingBufferEvent::new, 1024, threadFactory);

            // 2. Create coin-specific topic and handler
            String coinSpecificTopic = topic + "-" + coinSymbol.toLowerCase();
            BalanceUpdateEventHandler handler = handlerFactory.create(coinSpecificTopic);
            disruptor.handleEventsWith(handler);

            RingBuffer<TransferRingBufferEvent> ringBuffer = disruptor.start();
            disruptors.put(coinSymbol, disruptor);
            ringBuffers.put(coinSymbol, ringBuffer);
        }
        log.info("DisruptorPartitionManager initialized with partitions for: {}", ringBuffers.keySet());
    }

    /**
     * Retrieves the RingBuffer for a specific coin symbol.
     *
     * @param coinSymbol The symbol of the coin (e.g., "BTC", "ETH").
     * @return The RingBuffer for the specified coin, or null if no partition exists for that coin.
     */
    public RingBuffer<TransferRingBufferEvent> getRingBuffer(String coinSymbol) {
        ConfigCoinSymbolEntity config = configService.findByCoinSymbol(coinSymbol);
        if (config == null) {
            log.warn("No config found for coinSymbol: {}. Cannot find RingBuffer.", coinSymbol);
            return null; // Or return a default/global ring buffer
        }
        RingBuffer<TransferRingBufferEvent> buffer = ringBuffers.get(coinSymbol);
        if (buffer == null) {
            log.warn("No RingBuffer partition found for coin: {}", coinSymbol);
        }
        return buffer;
    }

    /**
     * Asynchronously shuts down all disruptor partitions.
     * This method initiates a graceful shutdown in a background thread to avoid
     * blocking the main application shutdown process.
     */
    public void shutdown() {
        log.info("Initiating asynchronous shutdown of all disruptor partitions...");
        new Thread(() -> {
            log.info("Background shutdown thread started.");
            for (Map.Entry<String, Disruptor<TransferRingBufferEvent>> entry : disruptors.entrySet()) {
                String coinSymbol = entry.getKey();
                Disruptor<?> disruptor = entry.getValue();
                log.info("Shutting down disruptor for coin: {}", coinSymbol);
                disruptor.shutdown(); // This is a blocking call
                log.info("Successfully shut down disruptor for coin: {}", coinSymbol);
            }
            log.info("All disruptor partitions have been shut down gracefully.");
        }, "disruptor-shutdown-thread").start();
    }
}
