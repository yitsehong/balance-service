package io.xrex.config;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import io.xrex.event.disruptor.TransferRingBufferEvent;
import io.xrex.event.disruptor.TransferEventFactory;
import io.xrex.event.handler.BalanceUpdateEventHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

@Configuration
public class DisruptorConfig {

    // RingBuffer size, must be a power of 2
    private static final int RING_BUFFER_SIZE = 1024 * 16;

    @Bean
    public RingBuffer<TransferRingBufferEvent> ringBuffer(BalanceUpdateEventHandler balanceUpdateEventHandler) {
        // Use a dedicated single thread for the core balance update logic to ensure sequential processing
        // and maximize CPU cache efficiency.
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        Disruptor<TransferRingBufferEvent> disruptor = new Disruptor<>(
                new TransferEventFactory(),
                RING_BUFFER_SIZE,
                threadFactory,
                ProducerType.MULTI, // Multiple gRPC threads can produce events
                new BlockingWaitStrategy() // A balanced wait strategy
        );

        // Set the single event handler that contains the core logic
        disruptor.handleEventsWith(balanceUpdateEventHandler);

        // Start the Disruptor and return the RingBuffer for injection
        return disruptor.start();
    }
}
