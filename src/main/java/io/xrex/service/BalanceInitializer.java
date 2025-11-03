package io.xrex.service;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

@Component
public class BalanceInitializer implements ApplicationListener<ApplicationReadyEvent> {

    private final InMemoryBalanceStore inMemoryBalanceStore;

    public BalanceInitializer(InMemoryBalanceStore inMemoryBalanceStore) {
        this.inMemoryBalanceStore = inMemoryBalanceStore;
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        System.out.println("Application started. Balances will be loaded on-demand.");
        // inMemoryBalanceStore.loadInitialBalances(); // This is no longer needed.
    }
}
