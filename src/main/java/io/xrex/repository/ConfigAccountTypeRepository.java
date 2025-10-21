package io.xrex.repository;

import io.xrex.model.entity.ConfigAccountTypeEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ConfigAccountTypeRepository extends JpaRepository<ConfigAccountTypeEntity, Long> {

    ConfigAccountTypeEntity findByAssetType(Integer assetType);

    List<ConfigAccountTypeEntity> findAllByCoinSymbol(String coinSymbol);

    ConfigAccountTypeEntity findByAssetAAndAssetBcAndCoinSymbol(String assetA, String assetBc, String coinSymbol);

    ConfigAccountTypeEntity findByAssetAAndAssetBcAndCoinSymbolAndSymbol(String assetA, String assetBc, String coinSymbol, String symbol);

    List<ConfigAccountTypeEntity> findByAssetAAndAssetBc(String assetA, String assetBc);
}
