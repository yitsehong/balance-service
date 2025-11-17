package io.xrex.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.xrex.persistence.entity.ConfigAccountTypeEntity;
import io.xrex.persistence.entity.ConfigCoinSymbolEntity;
import io.xrex.persistence.repository.ConfigAccountTypeRepository;
import io.xrex.persistence.repository.ConfigCoinSymbolRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This service manages the application's configuration, including account types and coin symbols.
 * It uses Caffeine caches to store frequently accessed configuration data in memory, reducing database load.
 * The caches are populated at startup and are refreshed as needed.
 */
@Service
@RequiredArgsConstructor
public class ConfigService {
    private final Cache<Integer, ConfigAccountTypeEntity> configAccountTypeCache = Caffeine.newBuilder()
            .maximumSize(1000_000).expireAfterWrite(1, TimeUnit.DAYS).build();

    private final Cache<String, ConfigCoinSymbolEntity> configCoinSymbolCache = Caffeine.newBuilder()
            .maximumSize(1_000).expireAfterWrite(1, TimeUnit.DAYS).build();

    private final ConfigAccountTypeRepository configAccountTypeRepository;
    private final ConfigCoinSymbolRepository configCoinSymbolRepository;

    /**
     * Initializes the configuration caches at application startup.
     * This method pre-populates the account type cache with all available account types.
     */
    @PostConstruct
    public void initialize() {
        List<ConfigAccountTypeEntity> configAccountTypeEntityList = configAccountTypeRepository.findAll();
        for (ConfigAccountTypeEntity c : configAccountTypeEntityList) {
            configAccountTypeCache.put(c.getAssetType(), c);
        }
    }

    /**
     * Finds a configuration account type by its asset type.
     * It first checks the cache, and if the entity is not found, it fetches it from the repository
     * and stores it in the cache for future use.
     *
     * @param assetType The asset type to search for.
     * @return The ConfigAccountTypeEntity, or null if not found.
     */
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

    /**
     * Retrieves a map of all open coin symbols.
     * "Open" coins are those that are actively traded or used in the system.
     *
     * @return A map where the key is the coin symbol and the value is the ConfigCoinSymbolEntity.
     */
    public Map<String, ConfigCoinSymbolEntity> findAllOpenCoinMap() {
        return configCoinSymbolRepository.findAll().stream().filter(c -> c.getIsOpen() == 1)
                .collect(Collectors.toMap(ConfigCoinSymbolEntity::getCoinSymbol, Function.identity()));
    }

    /**
     * Finds a configuration coin symbol by its symbol string.
     * It first checks the cache, and if the entity is not found, it fetches it from the repository
     * and stores it in the cache for future use.
     *
     * @param coinSymbol The coin symbol (e.g., "BTC", "ETH").
     * @return The ConfigCoinSymbolEntity, or null if not found.
     */
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
