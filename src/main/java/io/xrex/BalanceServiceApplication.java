package io.xrex;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableKafka
@EnableAsync
@SpringBootApplication
public class BalanceServiceApplication {

    static void main(String[] args) {
        // Force Netty to use NIO by disabling native transport.
        // This is crucial for running the Native Image on macOS where Epoll is not supported.
        System.setProperty("io.netty.transport.noNative", "true");
        System.setProperty("ratis.thirdparty.io.netty.transport.noNative", "true");
        System.setProperty("org.apache.ratis.thirdparty.io.netty.transport.noNative", "true");
        SpringApplication.run(BalanceServiceApplication.class, args);
    }

}
