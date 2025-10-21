package io.xrex.util;

import java.time.Instant;
import java.util.Random;
import java.util.UUID;

public class UUIDv7Generator {
    private static final Random RANDOM = new Random();

    public static String generate() {
        long timestamp = Instant.now().toEpochMilli(); // 48 bits timestamp
        long timeHigh = (timestamp >> 16) & 0xFFFFFFFFFFFFL;
        long timeLow = timestamp & 0xFFFF;

        // 48 bits timestamp + 4 bits version (0111)
        long msb = (timeHigh << 16) | timeLow;
        msb &= 0xFFFFFFFFFFFF0FFFL;
        msb |= 0x0000000000007000L; // version 7

        // 12 bits sub-second randomness + 62 bits random
        long lsb = (RANDOM.nextLong() & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L; // variant bits 10x

        return new UUID(msb, lsb).toString();
    }

}
