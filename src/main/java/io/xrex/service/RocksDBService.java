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
import org.rocksdb.*;
import org.slf4j.MDC;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.io.File;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
public class RocksDBService {

    private final String DB_PATH = "rocksdb_balances";
    private final Cache<AccountIdDto, BigDecimal> l1Cache = Caffeine.newBuilder()
            .maximumSize(1000_000).expireAfterWrite(50, TimeUnit.SECONDS).build();
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

            // Get balances
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

            // --- Atomic Update using WriteBatch ---
            try (final WriteOptions writeOpts = new WriteOptions();
                 final WriteBatch batch = new WriteBatch()) {

                // Update balance objects for serialization
                fromAccountBalance.setAmount(fromAfterBalance);
                toAccountBalance.setAmount(toAfterBalance);

                // Add updates to the batch
                batch.put(JSON.toJSONBytes(fromAccountId), JSON.toJSONBytes(fromAccountBalance));
                batch.put(JSON.toJSONBytes(toAccountId), JSON.toJSONBytes(toAccountBalance));
                // Execute the atomic write
                db.write(writeOpts, batch);

                // Update L1 cache AFTER successful DB write
                l1Cache.put(fromAccountId, fromAfterBalance);
                l1Cache.put(toAccountId, toAfterBalance);

            } catch (RocksDBException e) {
                log.error("Error during atomic balance update for eventKey: {}", eventKey, e);
                // Re-throw as a runtime exception to be caught by the EventHandler
                throw new RuntimeException("Failed to atomically update balances in RocksDB", e);
            }
            // --- End of Atomic Update ---

            // Create ledger book entries
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
        int pageSize = 5000;
        int threadPoolSize = Runtime.getRuntime().availableProcessors(); // 或自定義執行緒數

        // 先取得總頁數
        Pageable initialPageable = PageRequest.of(0, pageSize);
        Page<AccountEntity> firstPage = accountRepository.findAll(initialPageable);
        int totalPages = firstPage.getTotalPages();
        long totalElements = firstPage.getTotalElements();

        log.info("Starting cache warm-up with {} threads for {} pages, total {} accounts",
                threadPoolSize, totalPages, totalElements);

        ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);
        CountDownLatch latch = new CountDownLatch(totalPages);
        AtomicInteger processedCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        try {
            // 提交所有分頁任務
            for (int pageNumber = 0; pageNumber < totalPages; pageNumber++) {
                final int currentPage = pageNumber;
                executorService.submit(() -> {
                    try {
                        processPage(currentPage, pageSize, processedCount);
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                        log.error("Error processing page {}: {}", currentPage, e.getMessage(), e);
                    } finally {
                        latch.countDown();
                        int processed = processedCount.get();
                        if (processed % 10000 == 0 || currentPage % 10 == 0) {
                            log.info("Progress: processed {} accounts from {} pages",
                                    processed, currentPage + 1);
                        }
                    }
                });
            }

            // 等待所有任務完成
            latch.await();

            log.info("Finished warming up cache: {} accounts processed, {} errors, {} total pages",
                    processedCount.get(), errorCount.get(), totalPages);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Cache warm-up interrupted", e);
        } finally {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private void processPage(int pageNumber, int pageSize, AtomicInteger processedCount) {
        Pageable pageable = PageRequest.of(pageNumber, pageSize);
        Page<AccountEntity> page = accountRepository.findAll(pageable);

        int pageProcessed = 0;
        for (AccountEntity account : page.getContent()) {
            try {
                ConfigAccountTypeEntity configAccountType = configService.findByAssetType(account.getType());
                if (configAccountType == null) {
                    continue;
                }

                AccountIdDto accountId = new AccountIdDto(account.getUid(), configAccountType);
                BalanceDto balance = new BalanceDto(accountId, account.getBalance());
                save(JSON.toJSONBytes(accountId), JSON.toJSONBytes(balance));

                pageProcessed++;
            } catch (Exception e) {
                log.error("Error processing account uid={}, type={}: {}",
                        account.getUid(), account.getType(), e.getMessage());
            }
        }

        processedCount.addAndGet(pageProcessed);
        log.debug("Completed page {} with {} accounts", pageNumber, pageProcessed);
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