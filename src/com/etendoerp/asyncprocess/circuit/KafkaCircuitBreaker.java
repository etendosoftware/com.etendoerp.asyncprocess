package com.etendoerp.asyncprocess.circuit;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Circuit breaker implementation for Kafka operations.
 * Provides automatic fallback when Kafka is unavailable and prevents cascade failures.
 */
public class KafkaCircuitBreaker {
  private static final Logger log = LogManager.getLogger();

  public enum State {
    CLOSED,      // Normal operation - requests are allowed
    OPEN,        // Circuit is open - requests are blocked
    HALF_OPEN    // Testing if service has recovered
  }

  public static class CircuitBreakerConfig {
    private final int failureThreshold;
    private final Duration timeout;
    private final Duration retryInterval;
    private final int slowCallDurationThreshold;
    private final int slowCallRateThreshold;
    private final int minimumNumberOfCalls;

    public CircuitBreakerConfig(int failureThreshold, Duration timeout, Duration retryInterval,
        int slowCallDurationThreshold, int slowCallRateThreshold,
        int minimumNumberOfCalls) {
      this.failureThreshold = failureThreshold;
      this.timeout = timeout;
      this.retryInterval = retryInterval;
      this.slowCallDurationThreshold = slowCallDurationThreshold;
      this.slowCallRateThreshold = slowCallRateThreshold;
      this.minimumNumberOfCalls = minimumNumberOfCalls;
    }

    /**
     * Returns a default configuration for the circuit breaker.
     */
    public static CircuitBreakerConfig defaultConfig() {
      return new CircuitBreakerConfig(
          5,                              // failureThreshold
          Duration.ofSeconds(60),         // timeout
          Duration.ofSeconds(30),         // retryInterval
          5000,                           // slowCallDurationThreshold (ms)
          50,                             // slowCallRateThreshold (%)
          10                              // minimumNumberOfCalls
      );
    }

    public int getFailureThreshold() {
      return failureThreshold;
    }

    public Duration getTimeout() {
      return timeout;
    }

    public Duration getRetryInterval() {
      return retryInterval;
    }

    public int getSlowCallDurationThreshold() {
      return slowCallDurationThreshold;
    }

    public int getSlowCallRateThreshold() {
      return slowCallRateThreshold;
    }

    public int getMinimumNumberOfCalls() {
      return minimumNumberOfCalls;
    }
  }

  public static class CircuitBreakerException extends RuntimeException {
    public CircuitBreakerException(String message) {
      super(message);
    }
  }

  private final String name;
  private final CircuitBreakerConfig config;
  private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
  private final AtomicInteger failureCount = new AtomicInteger(0);
  private final AtomicInteger successCount = new AtomicInteger(0);
  private final AtomicInteger slowCallCount = new AtomicInteger(0);
  private final AtomicLong lastFailureTime = new AtomicLong(0);
  private final AtomicLong lastSuccessTime = new AtomicLong(0);
  private final AtomicInteger totalCalls = new AtomicInteger(0);
  private final AtomicLong windowStart = new AtomicLong(System.currentTimeMillis());
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private StateChangeListener stateChangeListener;
  private MetricsListener metricsListener;

  @FunctionalInterface
  public interface StateChangeListener {
    void onStateChange(String circuitBreakerName, State from, State to, String reason);
  }

  @FunctionalInterface
  public interface MetricsListener {
    void onMetricsUpdate(String circuitBreakerName, CircuitBreakerMetrics metrics);
  }

  /**
   * Base class for metrics related to call counts and rates.
   */
  public static class CallMetrics {
    private final int totalCalls;
    private final int failureCount;
    private final int successCount;
    private final int slowCallCount;
    private final double failureRate;
    private final double slowCallRate;

    public CallMetrics(int totalCalls, int failureCount, int successCount, int slowCallCount,
        double failureRate, double slowCallRate) {
      this.totalCalls = totalCalls;
      this.failureCount = failureCount;
      this.successCount = successCount;
      this.slowCallCount = slowCallCount;
      this.failureRate = failureRate;
      this.slowCallRate = slowCallRate;
    }

    public int getTotalCalls() {
      return totalCalls;
    }

    public int getFailureCount() {
      return failureCount;
    }

    public int getSuccessCount() {
      return successCount;
    }

    public int getSlowCallCount() {
      return slowCallCount;
    }

    public double getFailureRate() {
      return failureRate;
    }

    public double getSlowCallRate() {
      return slowCallRate;
    }
  }

  /**
   * Helper class that groups timestamp information for last failure and success.
   */
  public static class TimestampInfo {
    private final LocalDateTime lastFailureTime;
    private final LocalDateTime lastSuccessTime;

    public TimestampInfo(LocalDateTime lastFailureTime, LocalDateTime lastSuccessTime) {
      this.lastFailureTime = lastFailureTime;
      this.lastSuccessTime = lastSuccessTime;
    }

    public LocalDateTime getLastFailureTime() {
      return lastFailureTime;
    }

    public LocalDateTime getLastSuccessTime() {
      return lastSuccessTime;
    }
  }

  /**
   * A snapshot of the circuit breaker's metrics, extending basic call metrics.
   */
  public static class CircuitBreakerMetrics extends CallMetrics {
    private final State state;
    private final LocalDateTime lastFailureTime;
    private final LocalDateTime lastSuccessTime;

    public CircuitBreakerMetrics(CallMetrics callMetrics, State state, TimestampInfo timestampInfo) {
      super(callMetrics.getTotalCalls(), callMetrics.getFailureCount(), callMetrics.getSuccessCount(),
          callMetrics.getSlowCallCount(), callMetrics.getFailureRate(), callMetrics.getSlowCallRate());
      this.state = state;
      this.lastFailureTime = timestampInfo.getLastFailureTime();
      this.lastSuccessTime = timestampInfo.getLastSuccessTime();
    }

    public State getState() {
      return state;
    }

    public LocalDateTime getLastFailureTime() {
      return lastFailureTime;
    }

    public LocalDateTime getLastSuccessTime() {
      return lastSuccessTime;
    }
  }


  public KafkaCircuitBreaker(String name) {
    this(name, CircuitBreakerConfig.defaultConfig());
  }

  public KafkaCircuitBreaker(String name, CircuitBreakerConfig config) {
    this.name = name;
    this.config = config;
    scheduler.scheduleWithFixedDelay(this::resetMetricsWindow,
        config.getTimeout().toMillis(),
        config.getTimeout().toMillis(),
        TimeUnit.MILLISECONDS);
  }

  public <T> CompletableFuture<T> executeAsync(Supplier<CompletableFuture<T>> supplier) {
    if (!canExecute()) {
      return CompletableFuture.failedFuture(
          new CircuitBreakerException("Circuit breaker is OPEN for: " + name));
    }
    long startTime = System.currentTimeMillis();
    return supplier.get().whenComplete((result, throwable) -> recordOutcome(startTime, throwable));
  }

  /**
   * Synchronous execution with circuit breaker protection.
   * @param supplier
   * @return
   * @param <T>
   */
  public <T> T execute(Supplier<T> supplier) {
    if (!canExecute()) {
      throw new CircuitBreakerException("Circuit breaker is OPEN for: " + name);
    }
    long startTime = System.currentTimeMillis();
    try {
      T result = supplier.get();
      recordOutcome(startTime, null);
      return result;
    } catch (Exception e) {
      recordOutcome(startTime, e);
      throw e;
    }
  }

  private void recordOutcome(long startTime, Throwable throwable) {
    long duration = System.currentTimeMillis() - startTime;
    if (throwable != null) {
      recordFailure();
    } else {
      recordSuccess(duration);
    }
    updateMetrics();
  }

  private boolean canExecute() {
    switch (state.get()) {
      case CLOSED:
        return true;
      case OPEN:
        if (shouldAttemptReset()) {
          transitionTo(State.HALF_OPEN, "Testing service recovery");
          return true;
        }
        return false;
      case HALF_OPEN:
        return totalCalls.get() < config.getMinimumNumberOfCalls();
      default:
        return false;
    }
  }

  private void recordSuccess(long duration) {
    successCount.incrementAndGet();
    totalCalls.incrementAndGet();
    lastSuccessTime.set(System.currentTimeMillis());
    if (duration > config.getSlowCallDurationThreshold()) {
      slowCallCount.incrementAndGet();
    }
    if (state.get() == State.HALF_OPEN && successCount.get() >= config.getMinimumNumberOfCalls()) {
      transitionTo(State.CLOSED, "Sufficient successful calls received");
    }
  }

  private void recordFailure() {
    failureCount.incrementAndGet();
    totalCalls.incrementAndGet();
    lastFailureTime.set(System.currentTimeMillis());
    if (shouldOpenCircuit()) {
      transitionTo(State.OPEN, "Failure threshold exceeded");
    }
  }

  private boolean shouldOpenCircuit() {
    CircuitBreakerMetrics metrics = getMetrics();
    if (metrics.getTotalCalls() < config.getMinimumNumberOfCalls()) {
      return false;
    }
    return metrics.getFailureRate() >= config.getFailureThreshold() ||
        metrics.getSlowCallRate() >= config.getSlowCallRateThreshold();
  }

  private boolean shouldAttemptReset() {
    return System.currentTimeMillis() - lastFailureTime.get() >= config.getRetryInterval().toMillis();
  }

  private void transitionTo(State newState, String reason) {
    State previousState = state.getAndSet(newState);
    if (previousState != newState) {
      log.info("Circuit breaker '{}' transitioned from {} to {}: {}", name, previousState, newState, reason);
      resetCounters();
      notifyStateChange(previousState, newState, reason);
      if (newState == State.OPEN) {
        scheduleRetryAttempt();
      }
    }
  }

  private void scheduleRetryAttempt() {
    scheduler.schedule(() -> {
      if (state.get() == State.OPEN) {
        transitionTo(State.HALF_OPEN, "Retry interval elapsed");
      }
    }, config.getRetryInterval().toMillis(), TimeUnit.MILLISECONDS);
  }

  private void resetCounters() {
    failureCount.set(0);
    successCount.set(0);
    slowCallCount.set(0);
    totalCalls.set(0);
  }

  private void resetMetricsWindow() {
    long now = System.currentTimeMillis();
    if (now - windowStart.get() >= config.getTimeout().toMillis()) {
      resetCounters();
      windowStart.set(now);
    }
  }

  private void updateMetrics() {
    if (metricsListener != null) {
      try {
        metricsListener.onMetricsUpdate(name, getMetrics());
      } catch (Exception e) {
        log.warn("Error notifying metrics listener for circuit breaker {}: {}", name, e.getMessage());
      }
    }
  }

  private void notifyStateChange(State from, State to, String reason) {
    if (stateChangeListener != null) {
      try {
        stateChangeListener.onStateChange(name, from, to, reason);
      } catch (Exception e) {
        log.warn("Error notifying state change listener for circuit breaker {}: {}", name, e.getMessage());
      }
    }
  }

  public State getState() {
    return state.get();
  }

  public CircuitBreakerMetrics getMetrics() {
    int total = totalCalls.get();
    int failures = failureCount.get();
    int successes = successCount.get();
    int slowCalls = slowCallCount.get();
    double failureRate = total > 0 ? (double) failures / total * 100 : 0;
    double slowCallRate = total > 0 ? (double) slowCalls / total * 100 : 0;
    LocalDateTime lastFailure = lastFailureTime.get() > 0 ?
        LocalDateTime.now().minus(Duration.ofMillis(System.currentTimeMillis() - lastFailureTime.get())) : null;
    LocalDateTime lastSuccess = lastSuccessTime.get() > 0 ?
        LocalDateTime.now().minus(Duration.ofMillis(System.currentTimeMillis() - lastSuccessTime.get())) : null;
    CallMetrics callMetrics = new CallMetrics(total, failures, successes, slowCalls, failureRate, slowCallRate);
    TimestampInfo timestampInfo = new TimestampInfo(lastFailure, lastSuccess);
    return new CircuitBreakerMetrics(callMetrics, state.get(), timestampInfo);
  }

  public void forceOpen() {
    log.warn("Forcing circuit breaker '{}' to OPEN state", name);
    transitionTo(State.OPEN, "Forced open");
  }

  public void forceClose() {
    log.info("Forcing circuit breaker '{}' to CLOSED state", name);
    transitionTo(State.CLOSED, "Forced close");
  }

  public void reset() {
    log.info("Resetting circuit breaker '{}'", name);
    resetCounters();
    state.set(State.CLOSED);
    lastFailureTime.set(0);
    lastSuccessTime.set(0);
    windowStart.set(System.currentTimeMillis());
  }

  public void setStateChangeListener(StateChangeListener listener) {
    this.stateChangeListener = listener;
  }

  public void setMetricsListener(MetricsListener listener) {
    this.metricsListener = listener;
  }

  public String getName() {
    return name;
  }

  public CircuitBreakerConfig getConfig() {
    return config;
  }

  public void shutdown() {
    log.info("Shutting down circuit breaker '{}'", name);
    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      scheduler.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  public String getStatusReport() {
    CircuitBreakerMetrics metrics = getMetrics();
    return String.format(
        "=== Circuit Breaker Status: %s ===\n" +
            "State: %s\n" +
            "Total Calls: %d\n" +
            "Success Count: %d\n" +
            "Failure Count: %d\n" +
            "Slow Call Count: %d\n" +
            "Failure Rate: %.2f%%\n" +
            "Slow Call Rate: %.2f%%\n" +
            "Last Failure: %s\n" +
            "Last Success: %s\n" +
            "Configuration:\n" +
            "  Failure Threshold: %d%%\n" +
            "  Timeout: %s\n" +
            "  Retry Interval: %s\n" +
            "  Slow Call Duration Threshold: %dms\n" +
            "  Slow Call Rate Threshold: %d%%\n" +
            "  Minimum Number of Calls: %d\n",
        name, metrics.getState(), metrics.getTotalCalls(), metrics.getSuccessCount(),
        metrics.getFailureCount(), metrics.getSlowCallCount(), metrics.getFailureRate(),
        metrics.getSlowCallRate(), metrics.getLastFailureTime(), metrics.getLastSuccessTime(),
        config.getFailureThreshold(), config.getTimeout(), config.getRetryInterval(),
        config.getSlowCallDurationThreshold(), config.getSlowCallRateThreshold(),
        config.getMinimumNumberOfCalls()
    );
  }
}
