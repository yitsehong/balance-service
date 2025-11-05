package io.xrex.service;

import com.alibaba.fastjson2.JSON;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.xrex.event.disruptor.TransferRingBufferEvent;
import io.xrex.exception.InsufficientFundsException;
import io.xrex.model.dto.AccountIdDto;
import io.xrex.model.dto.BalanceDto;
import io.xrex.model.dto.event.TransactionEventDto;
import io.xrex.model.entity.AccountEntity;
import io.xrex.model.entity.ConfigAccountTypeEntity;
import io.xrex.model.entity.LedgerBookEntity;
import io.xrex.repository.AccountRepository;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

import java.io.File;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class RocksDBService {

    private final String DB_PATH = "rocksdb_balances";
    private final Cache<AccountIdDto, BigDecimal> l1Cache = Caffeine.newBuilder()
            .maximumSize(10_000).expireAfterWrite(1, TimeUnit.SECONDS).build();
    private final ConfigService configService;
    private final AccountRepository accountRepository;
    private RocksDB db;

    public RocksDBService(ConfigService configService, AccountRepository accountRepository) {
        this.configService = configService;
        this.accountRepository = accountRepository;
    }

    @PostConstruct
    public void initialize() {
        RocksDB.loadLibrary();
        final Options options = new Options().setCreateIfMissing(true);

        try {
            File dbDir = new File(DB_PATH);
            db = RocksDB.open(options, dbDir.getAbsolutePath());
            log.info("RocksDB initialized at: {}", dbDir.getAbsolutePath());

            warmUpCacheFromDB();
        } catch (RocksDBException e) {
            log.error("Error initializing RocksDB", e);
            throw new RuntimeException(e);
        }
    }

    public TransactionEventDto updateBalanceOnRocksDB(TransferRingBufferEvent event,
                                                      AccountIdDto fromAccountId, AccountIdDto toAccountId) {
        MDC.put("eventKeys", event.getEventKey());
        try {
            String eventKey = event.getEventKey();
            BigDecimal amount = event.getAmount();
            String scene = event.getScene();
            String refType = event.getRefType();
            Long refId = event.getRefId();

            // Get balances from the cache
            BalanceDto fromAccountBalance = findById(fromAccountId).orElse(new BalanceDto(fromAccountId, BigDecimal.ZERO));
            BalanceDto toAccountBalance = findById(toAccountId).orElse(new BalanceDto(toAccountId, BigDecimal.ZERO));
            // Check for sufficient funds
            boolean ignoreCheck = fromAccountId.getChainupId() == 1; // Assuming chainupId 1 is a system/internal account
            if (!ignoreCheck && fromAccountBalance.getAmount().compareTo(amount) < 0) {
                log.error("Transaction [{}]: Insufficient funds for user {}. Required: {}, Available: {}",
                        eventKey, fromAccountId.getChainupId(), amount, fromAccountBalance.getAmount());
                throw new InsufficientFundsException(fromAccountId.getChainupId() + " has insufficient funds, amount=" + amount + ", now=" + fromAccountBalance.getAmount());
            }

            // Calculate new balances
            BigDecimal fromBeforeBalance = fromAccountBalance.getAmount();
            BigDecimal fromAfterBalance = fromBeforeBalance.subtract(amount);
            BigDecimal toBeforeBalance = toAccountBalance.getAmount();
            BigDecimal toAfterBalance = toBeforeBalance.add(amount);

            // Update balance objects
            fromAccountBalance.setAmount(fromAfterBalance);
            toAccountBalance.setAmount(toAfterBalance);

            save(JSON.toJSONBytes(fromAccountId), JSON.toJSONBytes(fromAccountBalance));
            save(JSON.toJSONBytes(toAccountId), JSON.toJSONBytes(toAccountBalance));

            l1Cache.put(fromAccountId, fromAfterBalance);
            l1Cache.put(toAccountId, toAfterBalance);

            // Create ledger book entries (this part remains the same)
            LocalDateTime now = LocalDateTime.now();
            LedgerBookEntity from = LedgerBookEntity.builder()
                    .idempotencyKey(eventKey)
                    .chainupId(fromAccountId.getChainupId())
                    .assetType(fromAccountId.getAssetType())
                    .amount(amount.negate())
                    .beforeBalance(fromBeforeBalance).afterBalance(fromAfterBalance)
                    .coinSymbol(fromAccountId.getCoinSymbol())
                    .accountTag(fromAccountId.getAccountTag())
                    .scene(scene).refType(refType).refId(refId)
                    .createdTime(now).updatedTime(now).build();

            LedgerBookEntity to = LedgerBookEntity.builder()
                    .idempotencyKey(eventKey)
                    .chainupId(toAccountId.getChainupId())
                    .assetType(toAccountId.getAssetType())
                    .amount(amount)
                    .beforeBalance(toBeforeBalance).afterBalance(toAfterBalance)
                    .coinSymbol(toAccountId.getCoinSymbol())
                    .accountTag(toAccountId.getAccountTag())
                    .scene(scene).refType(refType).refId(refId)
                    .createdTime(now).updatedTime(now).build();

            return TransactionEventDto.builder().eventKey(eventKey)
                    .from(from).to(to).meta(event.getMeta()).opUid(event.getOpUid()).opIp(event.getOpIp()).build();
        } finally {
            MDC.remove("eventKeys");
        }
    }

    public BigDecimal queryEventual(AccountIdDto accountId) {
        BigDecimal balance = l1Cache.getIfPresent(accountId);
        if (balance == null) {
            balance = findById(accountId).orElse(new BalanceDto(accountId, BigDecimal.ZERO)).getAmount();
            l1Cache.put(accountId, balance);
        }
        return balance;
    }

    private void warmUpCacheFromDB() {
        List<AccountEntity> allAccounts = accountRepository.findAll();
        for (AccountEntity account : allAccounts) {
            ConfigAccountTypeEntity configAccountType = configService.findByAssetType(account.getType());
            AccountIdDto accountId = new AccountIdDto(account.getUid(), configAccountType);
            BalanceDto balance = new BalanceDto(accountId, account.getBalance());
            save(JSON.toJSONBytes(accountId), JSON.toJSONBytes(balance));
        }
        log.info("Finished warming up cache for {} accounts.", allAccounts.size());
    }

    private Optional<BalanceDto> findById(AccountIdDto accountId) {
        byte[] balanceBytes = get(JSON.toJSONBytes(accountId));
        if (balanceBytes == null) {
            return Optional.empty();
        }
        return Optional.of(JSON.parseObject(balanceBytes, BalanceDto.class));
    }

    private byte[] get(byte[] key) {
        try {
            return db.get(key);
        } catch (RocksDBException e) {
            log.error("Error getting value for key from RocksDB", e);
            return null;
        }
    }

    private void save(byte[] key, byte[] value) {
        try {
            db.put(key, value);
        } catch (RocksDBException e) {
            log.error("Error saving value to RocksDB", e);
        }
    }

    @PreDestroy
    public void close() {
        if (db != null) {
            db.close();
        }
    }
}
