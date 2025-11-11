package io.xrex.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.xrex.model.entity.ConfigAccountTypeEntity;
import io.xrex.model.entity.ConfigCoinSymbolEntity;
import io.xrex.repository.ConfigAccountTypeRepository;
import io.xrex.repository.ConfigCoinSymbolRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class ConfigService {
    private final Cache<Integer, ConfigAccountTypeEntity> configAccountTypeCache = Caffeine.newBuilder()
            .maximumSize(1000_000).expireAfterWrite(1, TimeUnit.DAYS).build();

    private final Cache<String, ConfigCoinSymbolEntity> configCoinSymbolCache = Caffeine.newBuilder()
            .maximumSize(1_000).expireAfterWrite(1, TimeUnit.DAYS).build();

    private final ConfigAccountTypeRepository configAccountTypeRepository;
    private final ConfigCoinSymbolRepository configCoinSymbolRepository;

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

    public Map<String, ConfigCoinSymbolEntity> findAllOpenCoinMap() {
        return configCoinSymbolRepository.findAll().stream().filter(c -> c.getIsOpen() == 1)
                .collect(Collectors.toMap(ConfigCoinSymbolEntity::getCoinSymbol, Function.identity()));
    }

    public ConfigCoinSymbolEntity findByCoinSymbol(String coinSymbol) {
        coinSymbol = coinSymbol.toLowerCase();
        ConfigCoinSymbolEntity result = configCoinSymbolCache.getIfPresent(coinSymbol);
        if (result == null) {
            Optional<ConfigCoinSymbolEntity> optional = configCoinSymbolRepository.findByCoinSymbol(coinSymbol);
            if (optional.isEmpty()) {
                return null;
            }
            result = optional.get();
            configCoinSymbolCache.put(coinSymbol, result);
        }
        return result;
    }
}
