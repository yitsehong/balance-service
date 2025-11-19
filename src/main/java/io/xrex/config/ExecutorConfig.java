package io.xrex.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

@Configuration
public class ExecutorConfig {

    /**
     * Defines a central, shared ExecutorService that uses virtual threads.
     * <p>
     * This approach is preferred over creating static executors within components because:
     * 1.  <b>Lifecycle Management</b>: Spring will automatically manage the lifecycle of this bean,
     *     ensuring a graceful shutdown by calling {@code shutdown()} when the application context is closed.
     * 2.  <b>Centralized Configuration</b>: The executor is configured in one place, making it easy to
     *     modify or monitor across the entire application.
     * 3.  <b>Resource Sharing</b>: Promotes sharing of executor resources, adhering to best practices.
     * <p>
     * The underlying thread factory names the virtual threads for better observability in logs and thread dumps.
     *
     * @return A shared ExecutorService instance backed by virtual threads.
     */
    @Bean("virtualThreadExecutor")
    public ExecutorService virtualThreadExecutor() {
        // Use a ThreadFactory to name the virtual threads for easier debugging and monitoring.
        ThreadFactory virtualThreadFactory = Thread.ofVirtual().name("virtual-task-", 0).factory();
        return Executors.newThreadPerTaskExecutor(virtualThreadFactory);
    }
}
