package io.xrex.service;

import com.alibaba.fastjson2.JSON;
import io.xrex.dto.IdempotencyRecordDto;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Optional;

/**
 * Manages idempotency for operations to prevent duplicate processing.
 * This service uses RocksDB to store a record of processed request IDs.
 * It provides an atomic locking mechanism to prevent race conditions in concurrent scenarios.
 */
@Service
@RequiredArgsConstructor
public class IdempotencyService {

    private final RocksDBService rocksDBService;

    /**
     * Finds an idempotency record by request ID.
     *
     * @param requestId The unique identifier for the request.
     * @return An Optional containing the IdempotencyRecordDto if found, otherwise empty.
     */
    public Optional<IdempotencyRecordDto> findRecord(String requestId) {
        byte[] key = JSON.toJSONBytes(requestId);
        byte[] value = rocksDBService.getFromIdempotency(key);
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(JSON.parseObject(value, IdempotencyRecordDto.class));
    }

    /**
     * Attempts to acquire a lock for a given request ID by creating a "PENDING" record.
     * This method is synchronized to ensure atomicity within a single service instance.
     *
     * NOTE: In a distributed environment with multiple instances of this service, this
     * `synchronized` block is NOT sufficient to guarantee global atomicity. A true distributed
     * lock (e.g., using Redis SETNX or a database unique constraint) would be required for
     * bulletproof protection across multiple nodes. However, this implementation significantly
     * reduces the window for race conditions.
     *
     * @param requestId The unique identifier for the request.
     * @return {@code true} if the lock was acquired (i.e., the record did not exist),
     *         {@code false} otherwise.
     */
    public synchronized boolean tryLockRequest(String requestId) {
        if (findRecord(requestId).isPresent()) {
            return false; // Record already exists, lock failed.
        }
        IdempotencyRecordDto pendingRecord = new IdempotencyRecordDto("PENDING", "Request is being processed.");
        updateRecord(requestId, pendingRecord);
        return true;
    }


    /**
     * Saves or updates an idempotency record.
     *
     * @param requestId The unique identifier for the request.
     * @param record    The IdempotencyRecordDto to save.
     */
    public void updateRecord(String requestId, IdempotencyRecordDto record) {
        byte[] key = JSON.toJSONBytes(requestId);
        byte[] value = JSON.toJSONBytes(record);
        rocksDBService.saveToIdempotency(key, value);
    }
}
