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
    
    public static CircuitBreakerConfig defaultConfig() {
      return new CircuitBreakerConfig(
          5,                              // failureThreshold
          Duration.ofSeconds(60),         // timeout
          Duration.ofSeconds(30),         // retryInterval
          5000,                          // slowCallDurationThreshold (ms)
          50,                            // slowCallRateThreshold (%)
          10                             // minimumNumberOfCalls
      );
    }
    
    // Getters
    public int getFailureThreshold() { return failureThreshold; }
    public Duration getTimeout() { return timeout; }
    public Duration getRetryInterval() { return retryInterval; }
    public int getSlowCallDurationThreshold() { return slowCallDurationThreshold; }
    public int getSlowCallRateThreshold() { return slowCallRateThreshold; }
    public int getMinimumNumberOfCalls() { return minimumNumberOfCalls; }
  }
  
  public static class CircuitBreakerException extends RuntimeException {
    public CircuitBreakerException(String message) {
      super(message);
    }
    
    public CircuitBreakerException(String message, Throwable cause) {
      super(message, cause);
    }
  }
  
  private final String name;
  private final CircuitBreakerConfig config;
  private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
  
  // Metrics
  private final AtomicInteger failureCount = new AtomicInteger(0);
  private final AtomicInteger successCount = new AtomicInteger(0);
  private final AtomicInteger slowCallCount = new AtomicInteger(0);
  private final AtomicLong lastFailureTime = new AtomicLong(0);
  private final AtomicLong lastSuccessTime = new AtomicLong(0);
  
  // For sliding window
  private final AtomicInteger totalCalls = new AtomicInteger(0);
  private final AtomicLong windowStart = new AtomicLong(System.currentTimeMillis());
  
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  
  // Listeners
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
  
  public static class CircuitBreakerMetrics {
    private final int totalCalls;
    private final int failureCount;
    private final int successCount;
    private final int slowCallCount;
    private final double failureRate;
    private final double slowCallRate;
    private final State state;
    private final LocalDateTime lastFailureTime;
    private final LocalDateTime lastSuccessTime;
    
    /**
     * Helper class to group call counts and rates
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

      public int getTotalCalls() { return totalCalls; }
      public int getFailureCount() { return failureCount; }
      public int getSuccessCount() { return successCount; }
      public int getSlowCallCount() { return slowCallCount; }
      public double getFailureRate() { return failureRate; }
      public double getSlowCallRate() { return slowCallRate; }
    }

    /**
     * Helper class to group timestamp information
     */
    public static class TimestampInfo {
      private final LocalDateTime lastFailureTime;
      private final LocalDateTime lastSuccessTime;

      public TimestampInfo(LocalDateTime lastFailureTime, LocalDateTime lastSuccessTime) {
        this.lastFailureTime = lastFailureTime;
        this.lastSuccessTime = lastSuccessTime;
      }

      public LocalDateTime getLastFailureTime() { return lastFailureTime; }
      public LocalDateTime getLastSuccessTime() { return lastSuccessTime; }
    }
    
    public CircuitBreakerMetrics(CallMetrics callMetrics, State state, TimestampInfo timestampInfo) {
      this.totalCalls = callMetrics.getTotalCalls();
      this.failureCount = callMetrics.getFailureCount();
      this.successCount = callMetrics.getSuccessCount();
      this.slowCallCount = callMetrics.getSlowCallCount();
      this.failureRate = callMetrics.getFailureRate();
      this.slowCallRate = callMetrics.getSlowCallRate();
      this.state = state;
      this.lastFailureTime = timestampInfo.getLastFailureTime();
      this.lastSuccessTime = timestampInfo.getLastSuccessTime();
    }

    // Getters
    public int getTotalCalls() { return totalCalls; }
    public int getFailureCount() { return failureCount; }
    public int getSuccessCount() { return successCount; }
    public int getSlowCallCount() { return slowCallCount; }
    public double getFailureRate() { return failureRate; }
    public double getSlowCallRate() { return slowCallRate; }
    public State getState() { return state; }
    public LocalDateTime getLastFailureTime() { return lastFailureTime; }
    public LocalDateTime getLastSuccessTime() { return lastSuccessTime; }
  }
  
  public KafkaCircuitBreaker(String name) {
    this(name, CircuitBreakerConfig.defaultConfig());
  }
  
  public KafkaCircuitBreaker(String name, CircuitBreakerConfig config) {
    this.name = name;
    this.config = config;
    
    // Schedule periodic metrics reset for sliding window
    scheduler.scheduleWithFixedDelay(this::resetMetricsWindow, 
        config.getTimeout().toMillis(), 
        config.getTimeout().toMillis(), 
        TimeUnit.MILLISECONDS);
  }
  
  /**
   * Executes a supplier with circuit breaker protection.
   */
  public <T> CompletableFuture<T> executeAsync(Supplier<CompletableFuture<T>> supplier) {
    if (!canExecute()) {
      return CompletableFuture.failedFuture(
          new CircuitBreakerException("Circuit breaker is OPEN for: " + name));
    }
    
    long startTime = System.currentTimeMillis();
    
    return supplier.get()
        .whenComplete((result, throwable) -> {
          long duration = System.currentTimeMillis() - startTime;
          
          if (throwable != null) {
            recordFailure();
          } else {
            recordSuccess(duration);
          }
          
          updateMetrics();
        });
  }
  
  /**
   * Executes a supplier synchronously with circuit breaker protection.
   */
  public <T> T execute(Supplier<T> supplier) {
    if (!canExecute()) {
      throw new CircuitBreakerException("Circuit breaker is OPEN for: " + name);
    }
    
    long startTime = System.currentTimeMillis();
    
    try {
      T result = supplier.get();
      long duration = System.currentTimeMillis() - startTime;
      recordSuccess(duration);
      updateMetrics();
      return result;
    } catch (Exception e) {
      recordFailure();
      updateMetrics();
      throw e;
    }
  }
  
  /**
   * Checks if execution is allowed based on current state.
   */
  private boolean canExecute() {
    State currentState = state.get();
    
    switch (currentState) {
      case CLOSED:
        return true;
        
      case OPEN:
        if (shouldAttemptReset()) {
          transitionToHalfOpen();
          return true;
        }
        return false;
        
      case HALF_OPEN:
        // Allow limited number of calls in half-open state
        return totalCalls.get() < config.getMinimumNumberOfCalls();
        
      default:
        return false;
    }
  }
  
  /**
   * Records a successful execution.
   */
  private void recordSuccess(long duration) {
    successCount.incrementAndGet();
    totalCalls.incrementAndGet();
    lastSuccessTime.set(System.currentTimeMillis());
    
    if (duration > config.getSlowCallDurationThreshold()) {
      slowCallCount.incrementAndGet();
    }
    
    // If we're in half-open state and getting successes, consider closing
    if (state.get() == State.HALF_OPEN && successCount.get() >= config.getMinimumNumberOfCalls()) {
      transitionToClosed();
    }
  }
  
  /**
   * Records a failed execution.
   */
  private void recordFailure() {
    failureCount.incrementAndGet();
    totalCalls.incrementAndGet();
    lastFailureTime.set(System.currentTimeMillis());
    
    // Check if we should open the circuit
    if (shouldOpenCircuit()) {
      transitionToOpen();
    }
  }
  
  /**
   * Checks if circuit should be opened based on failure rate.
   */
  private boolean shouldOpenCircuit() {
    int total = totalCalls.get();
    if (total < config.getMinimumNumberOfCalls()) {
      return false;
    }
    
    double failureRate = (double) failureCount.get() / total * 100;
    double slowCallRate = (double) slowCallCount.get() / total * 100;
    
    return failureRate >= config.getFailureThreshold() || 
           slowCallRate >= config.getSlowCallRateThreshold();
  }
  
  /**
   * Checks if we should attempt to reset from OPEN to HALF_OPEN.
   */
  private boolean shouldAttemptReset() {
    return System.currentTimeMillis() - lastFailureTime.get() >= config.getRetryInterval().toMillis();
  }
  
  /**
   * Transitions circuit breaker to CLOSED state.
   */
  private void transitionToClosed() {
    State previousState = state.getAndSet(State.CLOSED);
    if (previousState != State.CLOSED) {
      log.info("Circuit breaker '{}' transitioned to CLOSED", name);
      resetCounters();
      notifyStateChange(previousState, State.CLOSED, "Sufficient successful calls received");
    }
  }
  
  /**
   * Transitions circuit breaker to OPEN state.
   */
  private void transitionToOpen() {
    State previousState = state.getAndSet(State.OPEN);
    if (previousState != State.OPEN) {
      log.warn("Circuit breaker '{}' transitioned to OPEN", name);
      scheduleRetryAttempt();
      notifyStateChange(previousState, State.OPEN, "Failure threshold exceeded");
    }
  }
  
  /**
   * Transitions circuit breaker to HALF_OPEN state.
   */
  private void transitionToHalfOpen() {
    State previousState = state.getAndSet(State.HALF_OPEN);
    if (previousState != State.HALF_OPEN) {
      log.info("Circuit breaker '{}' transitioned to HALF_OPEN", name);
      resetCounters();
      notifyStateChange(previousState, State.HALF_OPEN, "Testing service recovery");
    }
  }
  
  /**
   * Schedules a retry attempt after the circuit is opened.
   */
  private void scheduleRetryAttempt() {
    scheduler.schedule(() -> {
      if (state.get() == State.OPEN) {
        transitionToHalfOpen();
      }
    }, config.getRetryInterval().toMillis(), TimeUnit.MILLISECONDS);
  }
  
  /**
   * Resets all counters.
   */
  private void resetCounters() {
    failureCount.set(0);
    successCount.set(0);
    slowCallCount.set(0);
    totalCalls.set(0);
  }
  
  /**
   * Resets the metrics window for sliding window behavior.
   */
  private void resetMetricsWindow() {
    long now = System.currentTimeMillis();
    if (now - windowStart.get() >= config.getTimeout().toMillis()) {
      resetCounters();
      windowStart.set(now);
    }
  }
  
  /**
   * Updates and notifies about metrics.
   */
  private void updateMetrics() {
    if (metricsListener != null) {
      try {
        CircuitBreakerMetrics metrics = getMetrics();
        metricsListener.onMetricsUpdate(name, metrics);
      } catch (Exception e) {
        log.warn("Error notifying metrics listener for circuit breaker {}: {}", name, e.getMessage());
      }
    }
  }
  
  /**
   * Notifies about state changes.
   */
  private void notifyStateChange(State from, State to, String reason) {
    if (stateChangeListener != null) {
      try {
        stateChangeListener.onStateChange(name, from, to, reason);
      } catch (Exception e) {
        log.warn("Error notifying state change listener for circuit breaker {}: {}", name, e.getMessage());
      }
    }
  }
  
  /**
   * Gets current circuit breaker state.
   */
  public State getState() {
    return state.get();
  }
  
  /**
   * Gets current metrics.
   */
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
    
    CircuitBreakerMetrics.CallMetrics callMetrics = new CircuitBreakerMetrics.CallMetrics(
        total, failures, successes, slowCalls, failureRate, slowCallRate);
    CircuitBreakerMetrics.TimestampInfo timestampInfo = new CircuitBreakerMetrics.TimestampInfo(
        lastFailure, lastSuccess);

    return new CircuitBreakerMetrics(callMetrics, state.get(), timestampInfo);
  }
  
  /**
   * Forces the circuit breaker to open.
   */
  public void forceOpen() {
    log.warn("Forcing circuit breaker '{}' to OPEN state", name);
    transitionToOpen();
  }
  
  /**
   * Forces the circuit breaker to close.
   */
  public void forceClose() {
    log.info("Forcing circuit breaker '{}' to CLOSED state", name);
    transitionToClosed();
  }
  
  /**
   * Resets the circuit breaker to initial state.
   */
  public void reset() {
    log.info("Resetting circuit breaker '{}'", name);
    resetCounters();
    state.set(State.CLOSED);
    lastFailureTime.set(0);
    lastSuccessTime.set(0);
    windowStart.set(System.currentTimeMillis());
  }
  
  /**
   * Sets state change listener.
   */
  public void setStateChangeListener(StateChangeListener listener) {
    this.stateChangeListener = listener;
  }
  
  /**
   * Sets metrics listener.
   */
  public void setMetricsListener(MetricsListener listener) {
    this.metricsListener = listener;
  }
  
  /**
   * Gets circuit breaker name.
   */
  public String getName() {
    return name;
  }
  
  /**
   * Gets circuit breaker configuration.
   */
  public CircuitBreakerConfig getConfig() {
    return config;
  }
  
  /**
   * Shuts down the circuit breaker.
   */
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
  
  /**
   * Gets a detailed status report.
   */
  public String getStatusReport() {
    CircuitBreakerMetrics metrics = getMetrics();
    StringBuilder report = new StringBuilder();
    
    report.append("=== Circuit Breaker Status: ").append(name).append(" ===\n");
    report.append("State: ").append(state.get()).append("\n");
    report.append("Total Calls: ").append(metrics.getTotalCalls()).append("\n");
    report.append("Success Count: ").append(metrics.getSuccessCount()).append("\n");
    report.append("Failure Count: ").append(metrics.getFailureCount()).append("\n");
    report.append("Slow Call Count: ").append(metrics.getSlowCallCount()).append("\n");
    report.append("Failure Rate: ").append(String.format("%.2f%%", metrics.getFailureRate())).append("\n");
    report.append("Slow Call Rate: ").append(String.format("%.2f%%", metrics.getSlowCallRate())).append("\n");
    
    if (metrics.getLastFailureTime() != null) {
      report.append("Last Failure: ").append(metrics.getLastFailureTime()).append("\n");
    }
    if (metrics.getLastSuccessTime() != null) {
      report.append("Last Success: ").append(metrics.getLastSuccessTime()).append("\n");
    }
    
    report.append("Configuration:\n");
    report.append("  Failure Threshold: ").append(config.getFailureThreshold()).append("%\n");
    report.append("  Timeout: ").append(config.getTimeout()).append("\n");
    report.append("  Retry Interval: ").append(config.getRetryInterval()).append("\n");
    report.append("  Slow Call Duration Threshold: ").append(config.getSlowCallDurationThreshold()).append("ms\n");
    report.append("  Slow Call Rate Threshold: ").append(config.getSlowCallRateThreshold()).append("%\n");
    report.append("  Minimum Number of Calls: ").append(config.getMinimumNumberOfCalls()).append("\n");
    
    return report.toString();
  }
}
