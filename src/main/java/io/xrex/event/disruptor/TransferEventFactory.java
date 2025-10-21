package io.xrex.event.disruptor;

import com.lmax.disruptor.EventFactory;

public class TransferEventFactory implements EventFactory<TransferRingBufferEvent> {

    @Override
    public TransferRingBufferEvent newInstance() {
        return new TransferRingBufferEvent();
    }
}
