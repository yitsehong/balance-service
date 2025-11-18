package io.xrex.persistence.repository;

import io.xrex.persistence.entity.ConfigCoinSymbolEntity;
import org.checkerframework.checker.nullness.qual.PolyNull;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ConfigCoinSymbolRepository extends JpaRepository<ConfigCoinSymbolEntity, Integer> {

    @PolyNull ConfigCoinSymbolEntity findByCoinSymbol(String coinSymbol);
}
