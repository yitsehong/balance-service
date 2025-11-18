package io.xrex.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.xrex.dto.PairConfigDto;
import io.xrex.enums.AssetType_A_BC;
import io.xrex.persistence.entity.ConfigAccountTypeEntity;
import io.xrex.persistence.entity.ConfigCoinSymbolEntity;
import io.xrex.persistence.entity.ConfigSymbolEntity;
import io.xrex.persistence.repository.ConfigAccountTypeRepository;
import io.xrex.persistence.repository.ConfigCoinSymbolRepository;
import io.xrex.persistence.repository.ConfigSymbolRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.xrex.util.XrexConstant.SYSTEM_CHAINUP_ID;

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
    private final Cache<String, ConfigSymbolEntity> configSymbolCache = Caffeine.newBuilder()
            .maximumSize(1_000).expireAfterWrite(1, TimeUnit.DAYS).build();
    private final Cache<String, PairConfigDto> pairConfigCache = Caffeine.newBuilder()
            .maximumSize(1_000).expireAfterWrite(1, TimeUnit.DAYS).build();

    private final ConfigAccountTypeRepository configAccountTypeRepository;
    private final ConfigCoinSymbolRepository configCoinSymbolRepository;
    private final ConfigSymbolRepository configSymbolRepository;

    private final Map<String, List<ConfigAccountTypeEntity>> coinConfigAccountTypeMap = new HashMap<>();
    private final Map<String, Map<String, ConfigAccountTypeEntity>> coinPairConfigAccountTypeMap = new HashMap<>();

    /**
     * Initializes the configuration caches at application startup.
     * This method pre-populates the account type cache with all available account types.
     */
    @PostConstruct
    public void initialize() {
        cacheAccountTypes();
        cacheSymbols();
        cachePairConfigs();
        coinConfigAccountTypeMap.clear();
        coinPairConfigAccountTypeMap.clear();
    }

    private void cachePairConfigs() {
        List<ConfigSymbolEntity> configSymbolEntityList = configSymbolRepository.findAll();
        for (ConfigSymbolEntity c : configSymbolEntityList) {
            String pair = c.getSymbol().toLowerCase();
            String base = c.getBase().toLowerCase();
            String quote = c.getQuote().toLowerCase();

            PairConfigDto pairConfig = PairConfigDto.builder()
                    .pair(pair).sysUid(SYSTEM_CHAINUP_ID)
                    .base(base).quote(quote)
                    .pricePre(c.getPricePre())
                    .volumePre(c.getVolumePre())
                    .minBaseAmount(BigDecimal.ONE.movePointLeft(c.getVolumePre()))
                    .minQuoteAmount(BigDecimal.ONE.movePointLeft(c.getPricePre()))
                    .build();

            List<ConfigAccountTypeEntity> baseAccountTypes = coinConfigAccountTypeMap.get(base);
            pairConfig.setBaseAccountNormal(findAccountType(baseAccountTypes, AssetType_A_BC.U_NORMAL));
            pairConfig.setBaseAccountLock(findAccountType(baseAccountTypes, AssetType_A_BC.U_LOCK));
            Integer baseMmAccount = findAccountType(baseAccountTypes, AssetType_A_BC.U_MM_NORMAL);
            pairConfig.setBaseMmAccountNormal(baseMmAccount);
            pairConfig.setBaseMmAccountLock(baseMmAccount);
            pairConfig.setSysBaseAccount(coinPairConfigAccountTypeMap.get(base).get(pair).getAssetType());

            List<ConfigAccountTypeEntity> quoteAccountTypes = coinConfigAccountTypeMap.get(quote);
            pairConfig.setQuoteAccountNormal(findAccountType(quoteAccountTypes, AssetType_A_BC.U_NORMAL));
            pairConfig.setQuoteAccountLock(findAccountType(quoteAccountTypes, AssetType_A_BC.U_LOCK));
            Integer quoteMmAccount = findAccountType(quoteAccountTypes, AssetType_A_BC.U_MM_NORMAL);
            pairConfig.setQuoteMmAccountNormal(quoteMmAccount);
            pairConfig.setQuoteMmAccountLock(quoteMmAccount);
            pairConfig.setSysQuoteAccount(coinPairConfigAccountTypeMap.get(quote).get(pair).getAssetType());

            pairConfigCache.put(pairConfig.getPair(), pairConfig);
        }
    }

    private Integer findAccountType(List<ConfigAccountTypeEntity> accountTypes, AssetType_A_BC assetType) {
        return accountTypes.stream()
                .filter(acc -> acc.getAssetA().equals(assetType.account_A) && acc.getAssetBc().equals(assetType.account_BC))
                .map(ConfigAccountTypeEntity::getAssetType)
                .findFirst()
                .orElse(null);
    }

    private void cacheSymbols() {
        List<ConfigSymbolEntity> configSymbolEntityList = configSymbolRepository.findAll();
        for (ConfigSymbolEntity c : configSymbolEntityList) {
            String pair = c.getSymbol().toLowerCase();
            configSymbolCache.put(pair, c);
        }
    }

    private void cacheAccountTypes() {
        List<ConfigAccountTypeEntity> configAccountTypeEntityList = configAccountTypeRepository.findAll();
        configAccountTypeEntityList.forEach(c -> configAccountTypeCache.put(c.getAssetType(), c));

        coinConfigAccountTypeMap.putAll(
                configAccountTypeEntityList.stream()
                        .collect(Collectors.groupingBy(c -> c.getCoinSymbol().toLowerCase()))
        );

        coinPairConfigAccountTypeMap.putAll(
                configAccountTypeEntityList.stream()
                        .filter(c -> "1".equals(c.getAssetA()) && "01".equals(c.getAssetBc()))
                        .collect(Collectors.groupingBy(c -> c.getCoinSymbol().toLowerCase(),
                                Collectors.toMap(c -> c.getSymbol().toLowerCase(), Function.identity(), (existing, replacement) -> existing)))
        );
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
        return configAccountTypeCache.get(assetType, configAccountTypeRepository::findByAssetType);
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
        return configCoinSymbolCache.get(coinSymbol, configCoinSymbolRepository::findByCoinSymbol);
    }

    public ConfigSymbolEntity findByPair(String pair) {
        pair = pair.toLowerCase();
        ConfigSymbolEntity result = configSymbolCache.getIfPresent(pair);
        if (result == null) {
            Optional<ConfigSymbolEntity> optional = configSymbolRepository.findBySymbol(pair);
            if (optional.isEmpty()) {
                return null;
            }
            result = optional.get();
            configSymbolCache.put(pair, result);
        }
        return result;
    }

    public PairConfigDto findPairConfigByPair(String pair) {
        pair = pair.toLowerCase();
        return pairConfigCache.getIfPresent(pair);
    }
}
