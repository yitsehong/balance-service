package io.xrex.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.xrex.model.entity.ConfigAccountTypeEntity;
import io.xrex.repository.ConfigAccountTypeRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
public class ConfigService {
    private final Cache<Integer, ConfigAccountTypeEntity> configAccountTypeCache = Caffeine.newBuilder()
            .maximumSize(1000_000).expireAfterWrite(1, TimeUnit.DAYS).build();

    private final ConfigAccountTypeRepository configAccountTypeRepository;

    @PostConstruct
    public void initialize() {
        List<ConfigAccountTypeEntity> configAccountTypeEntityList = configAccountTypeRepository.findAll();
        for (ConfigAccountTypeEntity c : configAccountTypeEntityList) {
            configAccountTypeCache.put(c.getAssetType(), c);
        }
    }

    public ConfigAccountTypeEntity findByAssetType(Integer assetType) {
        ConfigAccountTypeEntity result = configAccountTypeCache.getIfPresent(assetType);
        if (result == null) {
            result = configAccountTypeRepository.findByAssetType(assetType);
            if (result == null) {
                return null;
            }
            configAccountTypeCache.put(assetType, result);
        }
        return result;
    }
}
