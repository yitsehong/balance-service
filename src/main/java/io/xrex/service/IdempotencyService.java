package io.xrex.service;

import com.alibaba.fastjson2.JSON;
import io.xrex.model.dto.IdempotencyRecordDto;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Optional;

/**
 * Service to handle idempotency logic by storing and retrieving request records.
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
     * Saves an idempotency record.
     *
     * @param requestId The unique identifier for the request.
     * @param record    The IdempotencyRecordDto to save.
     */
    public void saveRecord(String requestId, IdempotencyRecordDto record) {
        byte[] key = JSON.toJSONBytes(requestId);
        byte[] value = JSON.toJSONBytes(record);
        rocksDBService.saveToIdempotency(key, value);
    }
}
