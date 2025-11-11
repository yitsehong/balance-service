package io.xrex.repository;

import io.xrex.model.entity.ConfigCoinSymbolEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface ConfigCoinSymbolRepository extends JpaRepository<ConfigCoinSymbolEntity, Integer> {

    Optional<ConfigCoinSymbolEntity> findByCoinSymbol(String coinSymbol);
}
