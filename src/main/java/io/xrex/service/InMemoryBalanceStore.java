package io.xrex.service;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
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
import io.xrex.repository.RocksDBBalanceRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class InMemoryBalanceStore {

    private final RocksDBBalanceRepository rocksDBBalanceRepository;
    private final AccountRepository accountRepository; 
    private final ConfigAccountTypeRepository configAccountTypeRepository;

    private final LoadingCache<AccountIdDto, BalanceDto> balanceCache;
    // Keep a cache for config types as it's small and frequently accessed
    private final ConcurrentHashMap<Integer, ConfigAccountTypeEntity> configAccountTypeMap;


    public InMemoryBalanceStore(RocksDBBalanceRepository rocksDBBalanceRepository,
                              AccountRepository accountRepository,
                              ConfigAccountTypeRepository configAccountTypeRepository) {
        this.rocksDBBalanceRepository = rocksDBBalanceRepository;
        this.accountRepository = accountRepository;
        this.configAccountTypeRepository = configAccountTypeRepository;

        // Load all config types into memory at startup
        this.configAccountTypeMap = new ConcurrentHashMap<>();
        configAccountTypeRepository.findAll().forEach(config -> configAccountTypeMap.put(config.getAssetType(), config));

        this.balanceCache = Caffeine.newBuilder()
                .maximumSize(1_000_000) // Max 1 million balances in memory
                .expireAfterAccess(1, TimeUnit.HOURS) // Evict if not accessed for 1 hour
                .build(this::loadBalance); // Use a method reference for the loader
    }

    private BalanceDto loadBalance(AccountIdDto accountId) {
        log.debug("Cache miss for account: {}. Loading from persistent store.", accountId);

        // 1. Try to load from RocksDB first
        Optional<BalanceDto> balanceFromRocks = rocksDBBalanceRepository.findById(accountId);
        if (balanceFromRocks.isPresent()) {
            log.debug("Loaded balance for {} from RocksDB.", accountId);
            return balanceFromRocks.get();
        }

        // 2. If not in RocksDB, fall back to the primary database (e.g., MySQL)
        Optional<AccountEntity> accountFromDb = Optional.ofNullable(accountRepository.findByUidAndType(accountId.chainupId(), accountId.assetType()));
        ConfigAccountTypeEntity config = getConfigAccountType(accountId.assetType());

        BalanceDto balanceDto;
        if (accountFromDb.isPresent()) {
            AccountEntity account = accountFromDb.get();
            log.debug("Loaded balance for {} from primary DB.", accountId);
            balanceDto = new BalanceDto(accountId, config.getCoinSymbol(), account.getBalance(), config.getTag());
        } else {
            // 3. If it doesn't exist anywhere, create a new zero-balance account
            log.debug("Account {} not found anywhere. Creating a new zero-balance DTO.", accountId);
            balanceDto = new BalanceDto(accountId, config.getCoinSymbol(), BigDecimal.ZERO, config.getTag());
        }

        // 4. Save the newly loaded/created balance to RocksDB for future requests
        rocksDBBalanceRepository.save(balanceDto);
        log.debug("Saved newly loaded balance for {} to RocksDB.", accountId);

        return balanceDto;
    }

    public ConfigAccountTypeEntity getConfigAccountType(int assetType) {
        return configAccountTypeMap.computeIfAbsent(assetType, configAccountTypeRepository::findByAssetType);
    }

    public BalanceDto getBalance(AccountIdDto accountIdDto) {
        return balanceCache.get(accountIdDto);
    }

    public void setBalance(AccountIdDto accountIdDto, String coinSymbol, BigDecimal amount, String accountTag) {
        BalanceDto balanceDto = new BalanceDto(accountIdDto, coinSymbol, amount, accountTag);
        balanceCache.put(accountIdDto, balanceDto);
        rocksDBBalanceRepository.save(balanceDto);
    }

    public synchronized TransactionEventDto processTransfer(TransferRingBufferEvent event,
                                                            AccountIdDto fromAccountId, AccountIdDto toAccountId) {
        String eventKey = event.getEventKey();
        BigDecimal amount = event.getAmount();
        String scene = event.getScene();
        String refType = event.getRefType();
        Long refId = event.getRefId();

        // Get balances from the cache
        BalanceDto fromAccountBalance = getBalance(fromAccountId);
        BalanceDto toAccountBalance = getBalance(toAccountId);

        // Check for sufficient funds
        boolean ignoreCheck = fromAccountId.chainupId() == 1; // Assuming chainupId 1 is a system/internal account
        if (!ignoreCheck && fromAccountBalance.getAmount().compareTo(amount) < 0) {
            log.error("Transaction [{}]: Insufficient funds for user {}. Required: {}, Available: {}",
                    eventKey, fromAccountId.chainupId(), amount, fromAccountBalance.getAmount());
            throw new InsufficientFundsException(fromAccountId.chainupId() + " has insufficient funds, amount=" + amount + ", now=" + fromAccountBalance.getAmount());
        }

        // Calculate new balances
        BigDecimal fromBeforeBalance = fromAccountBalance.getAmount();
        BigDecimal fromAfterBalance = fromBeforeBalance.subtract(amount);
        BigDecimal toBeforeBalance = toAccountBalance.getAmount();
        BigDecimal toAfterBalance = toBeforeBalance.add(amount);

        // Update balance objects
        fromAccountBalance.setAmount(fromAfterBalance);
        toAccountBalance.setAmount(toAfterBalance);

        // Write the updated balances back to the cache AND to RocksDB (write-through)
        balanceCache.put(fromAccountId, fromAccountBalance);
        rocksDBBalanceRepository.save(fromAccountId, fromAccountBalance);

        balanceCache.put(toAccountId, toAccountBalance);
        rocksDBBalanceRepository.save(toAccountId, toAccountBalance);

        // Create ledger book entries (this part remains the same)
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