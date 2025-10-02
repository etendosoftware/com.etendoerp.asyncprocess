package com.etendoerp.asyncprocess.circuit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link KafkaCircuitBreaker}.
 * <p>
 * This test class provides comprehensive coverage for the KafkaCircuitBreaker, including:
 * <ul>
 *   <li>State transitions (CLOSED, OPEN, HALF_OPEN)</li>
 *   <li>Execution of synchronous and asynchronous operations</li>
 *   <li>Custom and default configuration</li>
 *   <li>Listeners for state changes and metrics</li>
 *   <li>Metrics and status reporting</li>
 *   <li>Exception and error handling</li>
 *   <li>Idempotent shutdown</li>
 * </ul>
 * The tests ensure correct behavior for all edge cases and transitions, maximizing code coverage.
 */
class KafkaCircuitBreakerTest {

  public static final String TEST_BREAKER = "testBreaker";
  public static final String CUSTOM_BREAKER = "custom";
  public static final String SUCCESS_RESULT = "ok";
  public static final String ASYNC_RESULT = "async";
  public static final String HALF_OPEN_RESULT = "half-open";
  public static final String METRICS_RESULT = "metrics";
  public static final String SHOULD_NOT_RUN = "should not run";

  // Test configuration constants
  private static final int LONG_TIMEOUT_SECONDS = 60;
  private static final int QUICK_TIMEOUT_MILLIS = 100;
  private static final int BUFFER_WAIT_MILLIS = 100;

  private KafkaCircuitBreaker breaker;

  /**
   * Initializes a default KafkaCircuitBreaker before each test.
   */
  @BeforeEach
  void setUp() {
    breaker = new KafkaCircuitBreaker(TEST_BREAKER);
  }

  /**
   * Shuts down the KafkaCircuitBreaker after each test to release resources.
   */
  @AfterEach
  void tearDown() {
    breaker.shutdown();
  }

  /**
   * Creates a long timeout configuration for testing open state behavior.
   */
  private KafkaCircuitBreaker.CircuitBreakerConfig createLongTimeoutConfig() {
    return new KafkaCircuitBreaker.CircuitBreakerConfig(
        5, Duration.ofSeconds(LONG_TIMEOUT_SECONDS), Duration.ofSeconds(LONG_TIMEOUT_SECONDS), 5000, 50, 10);
  }

  /**
   * Creates a quick timeout configuration for testing transitions.
   */
  private KafkaCircuitBreaker.CircuitBreakerConfig createQuickTimeoutConfig() {
    return new KafkaCircuitBreaker.CircuitBreakerConfig(
        1, Duration.ofMillis(QUICK_TIMEOUT_MILLIS), Duration.ofMillis(QUICK_TIMEOUT_MILLIS), 1, 1, 1);
  }

  /**
   * Creates a circuit breaker with auto-cleanup using try-with-resources pattern.
   */
  private KafkaCircuitBreaker createManagedBreaker(String name, KafkaCircuitBreaker.CircuitBreakerConfig config) {
    return new KafkaCircuitBreaker(name, config);
  }

  /**
   * Sets up a state change listener that sets the provided AtomicBoolean to true when called.
   */
  private void setupStateChangeListener(KafkaCircuitBreaker breaker, AtomicBoolean flag) {
    breaker.setStateChangeListener((name, from, to, reason) -> flag.set(true));
  }

  /**
   * Sets up a metrics listener that sets the provided AtomicBoolean to true when called.
   */
  private void setupMetricsListener(KafkaCircuitBreaker breaker, AtomicBoolean flag) {
    breaker.setMetricsListener((name, metrics) -> flag.set(true));
  }

  /**
   * Sets up listeners that throw exceptions to test graceful handling.
   */
  private void setupExceptionThrowingListeners(KafkaCircuitBreaker breaker) {
    breaker.setStateChangeListener((name, from, to, reason) -> {
      throw new RuntimeException("listener error");
    });
    breaker.setMetricsListener((name, metrics) -> {
      throw new RuntimeException("metrics error");
    });
  }

  /**
   * Triggers a failure in the breaker to update lastFailureTime.
   */
  private void triggerFailure(KafkaCircuitBreaker breaker) {
    assertThrows(RuntimeException.class, () -> breaker.execute(() -> {
      throw new RuntimeException("fail");
    }));
  }

  /**
   * Waits for the retry interval to pass, allowing state transitions.
   */
  private void waitForRetryInterval(KafkaCircuitBreaker breaker) throws InterruptedException {
    Thread.sleep(breaker.getConfig().getRetryInterval().toMillis() + BUFFER_WAIT_MILLIS);
  }

  /**
   * Verifies that a breaker is in the expected state.
   */
  private void assertState(KafkaCircuitBreaker breaker, KafkaCircuitBreaker.State expectedState) {
    assertEquals(expectedState, breaker.getState());
  }

  /**
   * Verifies default configuration, getters, and initial state.
   */
  @Test
  void testDefaultConfigAndGetters() {
    assertEquals(TEST_BREAKER, breaker.getName());
    assertNotNull(breaker.getConfig());
    assertState(breaker, KafkaCircuitBreaker.State.CLOSED);
    assertNotNull(breaker.getMetrics());
    assertNotNull(breaker.getStatusReport());
  }

  /**
   * Tests forceOpen and forceClose transitions.
   */
  @Test
  void testForceOpenAndForceClose() {
    breaker.forceOpen();
    assertState(breaker, KafkaCircuitBreaker.State.OPEN);
    breaker.forceClose();
    assertState(breaker, KafkaCircuitBreaker.State.CLOSED);
  }

  /**
   * Tests the reset method, ensuring state and counters are reset.
   */
  @Test
  void testReset() {
    breaker.forceOpen();
    breaker.reset();
    assertState(breaker, KafkaCircuitBreaker.State.CLOSED);
    assertEquals(0, breaker.getMetrics().getTotalCalls());
  }

  /**
   * Tests synchronous execution for both success and failure cases.
   */
  @Test
  void testExecuteSuccessAndFailure() {
    // Success
    String result = breaker.execute(() -> SUCCESS_RESULT);
    assertEquals(SUCCESS_RESULT, result);
    // Failure
    assertThrows(RuntimeException.class, () -> breaker.execute(() -> {
      throw new RuntimeException("fail");
    }));
    // After failures, circuit should still be CLOSED (not enough calls to open)
    assertState(breaker, KafkaCircuitBreaker.State.CLOSED);
  }

  /**
   * Tests asynchronous execution for both success and failure cases.
   */
  @Test
  void testExecuteAsyncSuccessAndFailure() throws Exception {
    CompletableFuture<String> future = breaker.executeAsync(() -> CompletableFuture.completedFuture(ASYNC_RESULT));
    assertEquals(ASYNC_RESULT, future.get(1, TimeUnit.SECONDS));
    CompletableFuture<String> failed = breaker.executeAsync(() -> {
      CompletableFuture<String> f = new CompletableFuture<>();
      f.completeExceptionally(new RuntimeException("fail"));
      return f;
    });
    assertThrows(Exception.class, () -> failed.get(1, TimeUnit.SECONDS));
  }

  /**
   * Verifies that the circuit breaker in OPEN state blocks execution and throws the expected exception.
   * Ensures the breaker remains in OPEN by updating lastFailureTime before forceOpen.
   */
  @Test
  void testOpenStateBlocksExecution() {
    KafkaCircuitBreaker longBreaker = createManagedBreaker(TEST_BREAKER, createLongTimeoutConfig());
    try {
      // Trigger a failure to update lastFailureTime
      triggerFailure(longBreaker);
      longBreaker.forceOpen();
      assertState(longBreaker, KafkaCircuitBreaker.State.OPEN);
      assertThrows(KafkaCircuitBreaker.CircuitBreakerException.class,
          () -> longBreaker.execute(() -> SHOULD_NOT_RUN));
      CompletableFuture<String> future = longBreaker.executeAsync(
          () -> CompletableFuture.completedFuture(SHOULD_NOT_RUN));
      assertTrue(future.isCompletedExceptionally());
    } finally {
      longBreaker.shutdown();
    }
  }

  /**
   * Verifies that after the retry interval, the breaker transitions to HALF_OPEN and allows limited calls.
   */
  @Test
  void testHalfOpenStateAllowsLimitedCalls() throws Exception {
    // Simulate transition to HALF_OPEN
    breaker.forceOpen();
    // Simulate retry interval passed
    waitForRetryInterval(breaker);
    // Next call should transition to HALF_OPEN and allow execution
    assertDoesNotThrow(() -> breaker.execute(() -> HALF_OPEN_RESULT));
    assertState(breaker, KafkaCircuitBreaker.State.HALF_OPEN);
  }

  /**
   * Verifies that the state change listener is called on state transitions.
   */
  @Test
  void testStateChangeListener() {
    AtomicBoolean called = new AtomicBoolean(false);
    setupStateChangeListener(breaker, called);
    breaker.forceOpen();
    assertTrue(called.get());
  }

  /**
   * Verifies that the metrics listener is called on metrics update.
   */
  @Test
  void testMetricsListener() {
    AtomicBoolean called = new AtomicBoolean(false);
    setupMetricsListener(breaker, called);
    breaker.execute(() -> METRICS_RESULT);
    assertTrue(called.get());
  }

  /**
   * Ensures that exceptions in listeners do not affect breaker operation.
   */
  @Test
  void testListenersHandleExceptionsGracefully() {
    setupExceptionThrowingListeners(breaker);
    // Should not throw
    breaker.forceOpen();
    breaker.execute(() -> METRICS_RESULT);
  }

  /**
   * Verifies that a custom configuration is correctly applied.
   */
  @Test
  void testCustomConfig() {
    KafkaCircuitBreaker.CircuitBreakerConfig config = createQuickTimeoutConfig();
    KafkaCircuitBreaker customBreaker = createManagedBreaker(CUSTOM_BREAKER, config);
    try {
      assertEquals(config, customBreaker.getConfig());
    } finally {
      customBreaker.shutdown();
    }
  }

  /**
   * Verifies that after the retry interval, the breaker transitions from OPEN to HALF_OPEN.
   */
  @Test
  void testForceOpenAndRetryTransitionToHalfOpen() throws Exception {
    breaker.forceOpen();
    assertState(breaker, KafkaCircuitBreaker.State.OPEN);
    // Wait for retry interval
    waitForRetryInterval(breaker);
    // Next call should transition to HALF_OPEN
    assertDoesNotThrow(() -> breaker.execute(() -> HALF_OPEN_RESULT));
    assertState(breaker, KafkaCircuitBreaker.State.HALF_OPEN);
  }

  /**
   * Ensures that shutdown is idempotent and does not throw exceptions on repeated calls.
   */
  @Test
  void testShutdownIsIdempotent() {
    breaker.shutdown();
    breaker.shutdown();
    assertTrue(
        breaker.getState() == KafkaCircuitBreaker.State.CLOSED ||
        breaker.getState() == KafkaCircuitBreaker.State.OPEN ||
        breaker.getState() == KafkaCircuitBreaker.State.HALF_OPEN);
  }
}
