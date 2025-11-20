package io.xrex.config;

import io.xrex.controller.exception.RestApiException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;

@Slf4j
@Configuration
public class RetryLoggingConfig {

    @Bean
    public RetryListener retryLoggingListener() {
        return new RetryListener() {

            @Override
            public <T, E extends Throwable> boolean open(RetryContext context, RetryCallback<T, E> callback) {
                return true;
            }

            @Override
            public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
                log.warn("Retry attempt #{} failed. Exception: {} - {}",
                        context.getRetryCount() + 1,
                        throwable.getClass().getSimpleName(),
                        throwable.getMessage());
            }

            @Override
            public <T, E extends Throwable> void close(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
                if (throwable != null) {
                    if (throwable instanceof RestApiException) {
                        // because it will be balance not enough or others exception
                        log.warn("[transfer-retry] Retry exhausted after {} attempts. Final exception: {} - {}",
                                context.getRetryCount(),
                                throwable.getClass().getSimpleName(),
                                throwable.getMessage());
                    } else {
                        log.error("[transfer-retry] Retry exhausted after {} attempts. Final exception: {} - {}",
                                context.getRetryCount(),
                                throwable.getClass().getSimpleName(),
                                throwable.getMessage());
                    }
                }
            }
        };
    }
}
