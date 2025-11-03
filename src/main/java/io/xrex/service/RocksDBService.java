
package io.xrex.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.springframework.stereotype.Service;

import java.io.File;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

@Slf4j
@Service
public class RocksDBService {

    private final String DB_PATH = "rocksdb_balances";
    private RocksDB db;

    @PostConstruct
    public void initialize() {
        RocksDB.loadLibrary();
        final Options options = new Options().setCreateIfMissing(true);

        try {
            File dbDir = new File(DB_PATH);
            db = RocksDB.open(options, dbDir.getAbsolutePath());
            log.info("RocksDB initialized at: {}", dbDir.getAbsolutePath());
        } catch (RocksDBException e) {
            log.error("Error initializing RocksDB", e);
            throw new RuntimeException(e);
        }
    }

    public byte[] get(byte[] key) {
        try {
            return db.get(key);
        } catch (RocksDBException e) {
            log.error("Error getting value for key from RocksDB", e);
            return null;
        }
    }

    public void save(byte[] key, byte[] value) {
        try {
            db.put(key, value);
        } catch (RocksDBException e) {
            log.error("Error saving value to RocksDB", e);
        }
    }

    @PreDestroy
    public void close() {
        if (db != null) {
            db.close();
            log.info("RocksDB closed.");
        }
    }
}
