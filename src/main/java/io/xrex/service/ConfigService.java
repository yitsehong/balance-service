package io.xrex.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.xrex.model.entity.ConfigAccountTypeEntity;
import io.xrex.repository.ConfigAccountTypeRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
public class ConfigService {
    private final Cache<Integer, ConfigAccountTypeEntity> configAccountTypeCache = Caffeine.newBuilder()
            .maximumSize(100_000).expireAfterWrite(1, TimeUnit.DAYS).build();

    private final ConfigAccountTypeRepository configAccountTypeRepository;

    public ConfigAccountTypeEntity findByAssetType(Integer assetType) {
        ConfigAccountTypeEntity result = configAccountTypeCache.getIfPresent(assetType);
        if (result == null) {
            result = configAccountTypeRepository.findByAssetType(assetType);
            configAccountTypeCache.put(assetType, result);
        }
        return result;
    }
}
