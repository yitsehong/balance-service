package io.xrex.service;

import io.xrex.event.disruptor.TransferRingBufferEvent;
import io.xrex.exception.InsufficientFundsException;
import io.xrex.model.dto.AccountIdDto;
import io.xrex.model.dto.BalanceDto;
import io.xrex.model.dto.event.TransactionEventDto;
import io.xrex.model.entity.AccountEntity;
import io.xrex.model.entity.ConfigAccountTypeEntity;
import io.xrex.model.entity.LedgerBookEntity;
import io.xrex.repository.AccountRepository;
import io.xrex.repository.ConfigAccountTypeRepository;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An in-memory store for managing user balances.
 * This provides a fast, thread-safe way to access and update balances without database locks.
 */
@Slf4j
@Service
public class InMemoryBalanceStore {

    private final AccountRepository accountRepository;
    private final ConfigAccountTypeRepository configAccountTypeRepository;
    @Getter
    private ConcurrentHashMap<AccountIdDto, BalanceDto> balanceMap;
    @Getter
    private ConcurrentHashMap<Integer, ConfigAccountTypeEntity> configAccountTypeMap;

    public InMemoryBalanceStore(AccountRepository accountRepository, ConfigAccountTypeRepository configAccountTypeRepository) {
        this.accountRepository = accountRepository;
        this.configAccountTypeRepository = configAccountTypeRepository;
    }

    public void loadInitialBalances() {
        log.info("Starting to load all account balances into memory...");
        List<AccountEntity> accounts = accountRepository.findAll();
        balanceMap = new ConcurrentHashMap<>(accounts.size());

        List<ConfigAccountTypeEntity> configAccountTypes = configAccountTypeRepository.findAll();
        configAccountTypeMap = new ConcurrentHashMap<>(configAccountTypes.size());
        for (ConfigAccountTypeEntity configAccountType : configAccountTypes) {
            configAccountTypeMap.put(configAccountType.getAssetType(), configAccountType);
        }
        for (AccountEntity account : accounts) {
            ConfigAccountTypeEntity configAccountType = configAccountTypeMap.get(account.getType());
            AccountIdDto accountIdDto = new AccountIdDto(account.getUid(), account.getType());
            setBalance(accountIdDto, configAccountType.getCoinSymbol(), account.getBalance(), configAccountType.getTag());
        }
        log.info("Finished loading {} account balances into memory.", accounts.size());
    }

    /**
     * Retrieves a balance for a given account ID. If it doesn't exist, it creates a new one with a zero balance.
     *
     * @param accountIdDto The composite account ID.
     * @return The Balance object.
     */
    public BalanceDto getBalance(AccountIdDto accountIdDto) {
        ConfigAccountTypeEntity configAccountType = configAccountTypeMap.get(accountIdDto.assetType());
        if (configAccountType == null) {
            configAccountType = configAccountTypeRepository.findByAssetType(accountIdDto.assetType());
            configAccountTypeMap.put(accountIdDto.assetType(), configAccountType);
        }
        final String coinSymbol = configAccountType.getCoinSymbol();
        final String accountTag = configAccountType.getTag();
        return balanceMap.computeIfAbsent(accountIdDto, id -> new BalanceDto(id, coinSymbol, BigDecimal.ZERO, accountTag));
    }

    /**
     * A method to initialize or update a balance, for example, when loading data from a database at startup.
     *
     * @param accountIdDto The composite account ID.
     * @param amount       The initial amount.
     */
    public void setBalance(AccountIdDto accountIdDto, String coinSymbol, BigDecimal amount, String accountTag) {
        BalanceDto balanceDto = getBalance(accountIdDto);
        balanceDto.setAmount(amount);
        balanceDto.setCoinSymbol(coinSymbol);
        balanceDto.setAccountTag(accountTag);
    }

    /**
     * Processes a transfer between two accounts in a thread-safe manner.
     *
     * @param event
     * @param fromAccountId
     * @param toAccountId
     * @return TransactionEventDto.
     */
    public synchronized TransactionEventDto processTransfer(TransferRingBufferEvent event,
                                                            AccountIdDto fromAccountId, AccountIdDto toAccountId) {
        String eventKey = event.getEventKey();
        BigDecimal amount = event.getAmount();
        String scene = event.getScene();
        String refType = event.getRefType();
        Long refId = event.getRefId();

        BalanceDto fromAccountBalance = getBalance(fromAccountId);
        boolean ignoreCheck = fromAccountId.chainupId() == 1;
        if (!ignoreCheck && fromAccountBalance.getAmount().compareTo(amount) < 0) {
            log.error("Transaction [{}]: Insufficient funds for user {}. Required: {}, Available: {}",
                    eventKey, fromAccountId.chainupId(), amount, fromAccountBalance.getAmount());
            throw new InsufficientFundsException(fromAccountId.chainupId() + "has insufficient funds, amount=" + amount + ", now=" + fromAccountBalance.getAmount());
        }

        BalanceDto toAccountBalance = getBalance(toAccountId);

        BigDecimal fromBeforeBalance = fromAccountBalance.getAmount();
        BigDecimal fromAfterBalance = fromBeforeBalance.subtract(amount);

        BigDecimal toBeforeBalance = toAccountBalance.getAmount();
        BigDecimal toAfterBalance = toBeforeBalance.add(amount);
        // Perform the transfer
        fromAccountBalance.setAmount(fromAfterBalance);
        toAccountBalance.setAmount(toAfterBalance);

        LocalDateTime now = LocalDateTime.now();
        LedgerBookEntity from = LedgerBookEntity.builder()
                .idempotencyKey(eventKey)
                .chainupId(fromAccountId.chainupId())
                .assetType(fromAccountId.assetType())
                .amount(amount.negate())
                .beforeBalance(fromBeforeBalance).afterBalance(fromAfterBalance)
                .coinSymbol(fromAccountBalance.getCoinSymbol())
                .accountTag(fromAccountBalance.getAccountTag())
                .scene(scene).refType(refType).refId(refId)
                .createdTime(now).updatedTime(now).build();

        LedgerBookEntity to = LedgerBookEntity.builder()
                .idempotencyKey(eventKey)
                .chainupId(toAccountId.chainupId())
                .assetType(toAccountId.assetType())
                .amount(amount)
                .beforeBalance(toBeforeBalance).afterBalance(toAfterBalance)
                .coinSymbol(toAccountBalance.getCoinSymbol())
                .accountTag(toAccountBalance.getAccountTag())
                .scene(scene).refType(refType).refId(refId)
                .createdTime(now).updatedTime(now).build();
        return TransactionEventDto.builder().eventKey(eventKey)
                .from(from).to(to).meta(event.getMeta()).opUid(event.getOpUid()).opIp(event.getOpIp()).build();
    }
}
