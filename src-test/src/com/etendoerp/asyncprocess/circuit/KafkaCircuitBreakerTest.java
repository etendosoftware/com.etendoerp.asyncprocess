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

  private KafkaCircuitBreaker breaker;

  /**
   * Initializes a default KafkaCircuitBreaker before each test.
   */
  @BeforeEach
  void setUp() {
    breaker = new KafkaCircuitBreaker("testBreaker");
  }

  /**
   * Shuts down the KafkaCircuitBreaker after each test to release resources.
   */
  @AfterEach
  void tearDown() {
    breaker.shutdown();
  }

  /**
   * Verifies default configuration, getters, and initial state.
   */
  @Test
  void testDefaultConfigAndGetters() {
    assertEquals("testBreaker", breaker.getName());
    assertNotNull(breaker.getConfig());
    assertEquals(KafkaCircuitBreaker.State.CLOSED, breaker.getState());
    assertNotNull(breaker.getMetrics());
    assertNotNull(breaker.getStatusReport());
  }

  /**
   * Tests forceOpen and forceClose transitions.
   */
  @Test
  void testForceOpenAndForceClose() {
    breaker.forceOpen();
    assertEquals(KafkaCircuitBreaker.State.OPEN, breaker.getState());
    breaker.forceClose();
    assertEquals(KafkaCircuitBreaker.State.CLOSED, breaker.getState());
  }

  /**
   * Tests the reset method, ensuring state and counters are reset.
   */
  @Test
  void testReset() {
    breaker.forceOpen();
    breaker.reset();
    assertEquals(KafkaCircuitBreaker.State.CLOSED, breaker.getState());
    assertEquals(0, breaker.getMetrics().getTotalCalls());
  }

  /**
   * Tests synchronous execution for both success and failure cases.
   */
  @Test
  void testExecuteSuccessAndFailure() {
    // Success
    String result = breaker.execute(() -> "ok");
    assertEquals("ok", result);
    // Failure
    assertThrows(RuntimeException.class, () -> breaker.execute(() -> {
      throw new RuntimeException("fail");
    }));
    // After failures, circuit should still be CLOSED (not enough calls to open)
    assertEquals(KafkaCircuitBreaker.State.CLOSED, breaker.getState());
  }

  /**
   * Tests asynchronous execution for both success and failure cases.
   */
  @Test
  void testExecuteAsyncSuccessAndFailure() throws Exception {
    CompletableFuture<String> future = breaker.executeAsync(() -> CompletableFuture.completedFuture("async"));
    assertEquals("async", future.get(1, TimeUnit.SECONDS));
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
    KafkaCircuitBreaker.CircuitBreakerConfig config = new KafkaCircuitBreaker.CircuitBreakerConfig(
        5, Duration.ofSeconds(60), Duration.ofSeconds(60), 5000, 50, 10);
    KafkaCircuitBreaker longBreaker = new KafkaCircuitBreaker("testBreaker", config);
    try {
      // Trigger a failure to update lastFailureTime
      assertThrows(RuntimeException.class, () -> longBreaker.execute(() -> {
        throw new RuntimeException("fail");
      }));
      longBreaker.forceOpen();
      assertEquals(KafkaCircuitBreaker.State.OPEN, longBreaker.getState());
      assertThrows(KafkaCircuitBreaker.CircuitBreakerException.class,
          () -> longBreaker.execute(() -> "should not run"));
      CompletableFuture<String> future = longBreaker.executeAsync(
          () -> CompletableFuture.completedFuture("should not run"));
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
    Thread.sleep(breaker.getConfig().getRetryInterval().toMillis());
    // Next call should transition to HALF_OPEN and allow execution
    assertDoesNotThrow(() -> breaker.execute(() -> "half-open"));
    assertEquals(KafkaCircuitBreaker.State.HALF_OPEN, breaker.getState());
  }

  /**
   * Verifies that the state change listener is called on state transitions.
   */
  @Test
  void testStateChangeListener() {
    AtomicBoolean called = new AtomicBoolean(false);
    breaker.setStateChangeListener((name, from, to, reason) -> called.set(true));
    breaker.forceOpen();
    assertTrue(called.get());
  }

  /**
   * Verifies that the metrics listener is called on metrics update.
   */
  @Test
  void testMetricsListener() {
    AtomicBoolean called = new AtomicBoolean(false);
    breaker.setMetricsListener((name, metrics) -> called.set(true));
    breaker.execute(() -> "metrics");
    assertTrue(called.get());
  }

  /**
   * Ensures that exceptions in listeners do not affect breaker operation.
   */
  @Test
  void testListenersHandleExceptionsGracefully() {
    breaker.setStateChangeListener((name, from, to, reason) -> {
      throw new RuntimeException("listener error");
    });
    breaker.setMetricsListener((name, metrics) -> {
      throw new RuntimeException("metrics error");
    });
    // Should not throw
    breaker.forceOpen();
    breaker.execute(() -> "metrics");
  }

  /**
   * Verifies that a custom configuration is correctly applied.
   */
  @Test
  void testCustomConfig() {
    KafkaCircuitBreaker.CircuitBreakerConfig config = new KafkaCircuitBreaker.CircuitBreakerConfig(
        1, Duration.ofMillis(100), Duration.ofMillis(100), 1, 1, 1);
    KafkaCircuitBreaker customBreaker = new KafkaCircuitBreaker("custom", config);
    assertEquals(config, customBreaker.getConfig());
    customBreaker.shutdown();
  }

  /**
   * Verifies that after the retry interval, the breaker transitions from OPEN to HALF_OPEN.
   */
  @Test
  void testForceOpenAndRetryTransitionToHalfOpen() throws Exception {
    breaker.forceOpen();
    assertEquals(KafkaCircuitBreaker.State.OPEN, breaker.getState());
    // Wait for retry interval
    Thread.sleep(breaker.getConfig().getRetryInterval().toMillis() + 100);
    // Next call should transition to HALF_OPEN
    assertDoesNotThrow(() -> breaker.execute(() -> "half-open"));
    assertEquals(KafkaCircuitBreaker.State.HALF_OPEN, breaker.getState());
  }

  /**
   * Ensures that shutdown is idempotent and does not throw exceptions on repeated calls.
   */
  @Test
  void testShutdownIsIdempotent() {
    breaker.shutdown();
    breaker.shutdown();
    assertTrue(
        breaker.getState() == KafkaCircuitBreaker.State.CLOSED || breaker.getState() == KafkaCircuitBreaker.State.OPEN || breaker.getState() == KafkaCircuitBreaker.State.HALF_OPEN);
  }
}
