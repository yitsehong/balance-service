package io.xrex.persistence.repository;

import io.xrex.persistence.entity.ConfigSymbolEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface ConfigSymbolRepository extends JpaRepository<ConfigSymbolEntity, Integer> {
    Optional<ConfigSymbolEntity> findBySymbol(String symbol);
}
