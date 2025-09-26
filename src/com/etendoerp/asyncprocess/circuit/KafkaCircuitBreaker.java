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
     *
     * The default configuration is as follows:
     * <ul>
     *   <li>Failure threshold: 5</li>
     *   <li>Timeout: 60 seconds</li>
     *   <li>Retry interval: 30 seconds</li>
     *   <li>Slow call duration threshold: 5000 milliseconds</li>
     *   <li>Slow call rate threshold: 50%</li>
     *   <li>Minimum number of calls: 10</li>
     * </ul>
     *
     * @return the default configuration for the circuit breaker
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

    /**
     * Gets the failure threshold as a percentage value.
     *
     * This value determines how many failures in a row are required to
     * transition the circuit breaker to the OPEN state.
     *
     * @return the failure threshold as a percentage value
     */
    public int getFailureThreshold() {
      return failureThreshold;
    }

    /**
     * Returns the timeout duration after which the circuit breaker will attempt to transition to the HALF_OPEN state.
     *
     * This value determines how long the circuit breaker will wait before attempting to reset after a failure.
     *
     * @return the timeout duration after which the circuit breaker will attempt to transition to the HALF_OPEN state
     */
    public Duration getTimeout() {
      return timeout;
    }

    /**
     * Returns the retry interval after which the circuit breaker will attempt to transition to the HALF_OPEN state.
     *
     * This value determines how often the circuit breaker will attempt to reset after a failure.
     *
     * @return the retry interval after which the circuit breaker will attempt to transition to the HALF_OPEN state
     */
    public Duration getRetryInterval() {
      return retryInterval;
    }

    /**
     * Gets the slow call duration threshold in milliseconds.
     *
     * This value determines what calls are considered slow. Calls that take longer than this value will be counted towards the slow call count.
     *
     * @return the slow call duration threshold in milliseconds
     */
    public int getSlowCallDurationThreshold() {
      return slowCallDurationThreshold;
    }

    /**
     * Returns the slow call rate threshold as a percentage.
     *
     * This value determines the minimum percentage of slow calls that will trigger the circuit breaker to open.
     *
     * @return slow call rate threshold percentage
     */
    public int getSlowCallRateThreshold() {
      return slowCallRateThreshold;
    }

    /**
     * Returns the minimum number of calls required to evaluate thresholds.
     *
     * This value determines the minimum number of calls that must be made before the circuit breaker will consider opening.
     *
     * @return minimum number of calls in the sliding window
     */
    public int getMinimumNumberOfCalls() {
      return minimumNumberOfCalls;
    }
  }

  public static class CircuitBreakerException extends RuntimeException {
    /**
     * Creates a circuit breaker exception with a message.
     *
     * @param message descriptive message for the exception
     */
    public CircuitBreakerException(String message) {
      super(message);
    }

    /**
     * Creates a circuit breaker exception with a message and underlying cause.
     *
     * @param message descriptive message for the exception
     * @param cause original throwable that caused this exception
     */
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
    /**
     * Invoked when the circuit breaker state changes.
     *
     * @param circuitBreakerName name of the circuit breaker that changed
     * @param from previous state
     * @param to new state
     * @param reason reason for the state change
     */
    void onStateChange(String circuitBreakerName, State from, State to, String reason);
  }

  @FunctionalInterface
  public interface MetricsListener {
    /**
     * Invoked to deliver an updated set of metrics for the circuit breaker.
     *
     * @param circuitBreakerName name of the circuit breaker emitting metrics
     * @param metrics current metrics snapshot
     */
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
     * Helper class to group call counts and rates.
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

      /**
       * Returns the total number of calls counted in the window.
       *
       * @return total number of calls
       */
      public int getTotalCalls() {
        return totalCalls;
      }

      /**
       * Returns the number of failures recorded in the window.
       *
       * @return failure count
       */
      public int getFailureCount() {
        return failureCount;
      }

      /**
       * Returns the number of successful calls recorded in the window.
       *
       * @return success count
       */
      public int getSuccessCount() {
        return successCount;
      }

      /**
       * Returns the number of slow calls recorded in the window.
       *
       * @return slow call count
       */
      public int getSlowCallCount() {
        return slowCallCount;
      }

      /**
       * Returns the failure rate calculated for the window (percentage).
       *
       * @return failure rate as a percentage
       */
      public double getFailureRate() {
        return failureRate;
      }

      /**
       * Returns the slow call rate calculated for the window (percentage).
       *
       * @return slow call rate as a percentage
       */
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

      /**
       * Returns the timestamp of the last recorded failure, or null if none.
       *
       * @return LocalDateTime of last failure or null
       */
      public LocalDateTime getLastFailureTime() {
        return lastFailureTime;
      }

      /**
       * Returns the timestamp of the last recorded success, or null if none.
       *
       * @return LocalDateTime of last success or null
       */
      public LocalDateTime getLastSuccessTime() {
        return lastSuccessTime;
      }
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

  /**
   * Creates a circuit breaker with a specific configuration.
   *
   * @param name identifier name for the circuit
   * @param config configuration instance
   */
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
   * Executes an asynchronous supplier protected by the circuit breaker.
   * If the circuit is OPEN, returns a failed CompletableFuture with CircuitBreakerException.
   *
   * @param <T> result type
   * @param supplier supplier that returns a CompletableFuture with the operation
   * @return CompletableFuture with the result or a failure if circuit is OPEN
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
   * Executes a synchronous supplier protected by the circuit breaker.
   * If the circuit is OPEN, throws CircuitBreakerException.
   *
   * @param <T> result type
   * @param supplier supplier that performs the operation
   * @return the operation result
   * @throws CircuitBreakerException if circuit is OPEN
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
   * Determines if calls are permitted depending on the current state.
   * CLOSED: allow calls; OPEN: block calls unless retry interval passed; HALF_OPEN: allow limited calls.
   *
   * @return true if execution is permitted, false otherwise
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
   * Records a successful execution and updates counters and timestamps.
   * If in HALF_OPEN and enough successes accumulate, transition to CLOSED.
   *
   * @param duration call duration in milliseconds
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
   * Records a failed execution, updates counters and timestamps, and checks if circuit should open.
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
   * Determines whether the circuit should be opened based on failure and slow call rates.
   *
   * @return true if circuit should open, false otherwise
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
   * Checks if retry interval has passed since last failure to attempt reset.
   *
   * @return true if reset attempt is allowed
   */
  private boolean shouldAttemptReset() {
    return System.currentTimeMillis() - lastFailureTime.get() >= config.getRetryInterval().toMillis();
  }

  /**
   * Transition to CLOSED state and reset counters.
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
   * Transition to OPEN state, schedule retry and notify listeners.
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
   * Transition to HALF_OPEN state and reset counters for testing.
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
   * Schedule a retry attempt after opening the circuit.
   */
  private void scheduleRetryAttempt() {
    scheduler.schedule(() -> {
      if (state.get() == State.OPEN) {
        transitionToHalfOpen();
      }
    }, config.getRetryInterval().toMillis(), TimeUnit.MILLISECONDS);
  }

  /**
   * Reset internal counters used for decision making.
   */
  private void resetCounters() {
    failureCount.set(0);
    successCount.set(0);
    slowCallCount.set(0);
    totalCalls.set(0);
  }

  /**
   * Reset the sliding window metrics when timeout elapses.
   */
  private void resetMetricsWindow() {
    long now = System.currentTimeMillis();
    if (now - windowStart.get() >= config.getTimeout().toMillis()) {
      resetCounters();
      windowStart.set(now);
    }
  }

  /**
   * Build and notify metrics to registered listener if present. Exceptions from listener are logged.
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
   * Notify registered state change listener. Exceptions from listener are logged.
   *
   * @param from previous state
   * @param to new state
   * @param reason reason for change
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
   * Returns the current state of the circuit breaker.
   *
   * @return current state
   */
  public State getState() {
    return state.get();
  }

  /**
   * Returns a snapshot of current metrics.
   *
   * @return current CircuitBreakerMetrics
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
   * Force the circuit to OPEN state.
   */
  public void forceOpen() {
    log.warn("Forcing circuit breaker '{}' to OPEN state", name);
    transitionToOpen();
  }

  /**
   * Force the circuit to CLOSED state.
   */
  public void forceClose() {
    log.info("Forcing circuit breaker '{}' to CLOSED state", name);
    transitionToClosed();
  }

  /**
   * Reset the circuit to initial state (CLOSED) and clear counters/timestamps.
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
   * Register a listener for state change notifications.
   *
   * @param listener StateChangeListener implementation
   */
  public void setStateChangeListener(StateChangeListener listener) {
    this.stateChangeListener = listener;
  }

  /**
   * Register a listener for metrics updates.
   *
   * @param listener MetricsListener implementation
   */
  public void setMetricsListener(MetricsListener listener) {
    this.metricsListener = listener;
  }

  /**
   * Returns the circuit breaker name.
   *
   * @return circuit name
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the configuration used by this circuit breaker.
   *
   * @return CircuitBreakerConfig instance
   */
  public CircuitBreakerConfig getConfig() {
    return config;
  }

  /**
   * Shutdown internal scheduler and release resources.
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
   * Returns a human readable status report including metrics and configuration.
   *
   * @return status report string
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
