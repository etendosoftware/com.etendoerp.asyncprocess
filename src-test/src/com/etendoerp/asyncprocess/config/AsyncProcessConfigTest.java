package com.etendoerp.asyncprocess.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@link AsyncProcessConfig} class.
 * <p>
 * This test class verifies the default values and the correct behavior of setters and getters
 * for the AsyncProcessConfig configuration class.
 */
class AsyncProcessConfigTest {

  /**
   * Tests that the default values of AsyncProcessConfig are as expected.
   */
  @Test
  void testDefaultValues() {
    AsyncProcessConfig config = new AsyncProcessConfig();
    assertEquals(3, config.getMaxRetries());
    assertEquals(1000, config.getRetryDelayMs());
    assertEquals(5, config.getPrefetchCount());
    assertEquals(5, config.getParallelThreads());
  }

  /**
   * Tests the setters and getters of AsyncProcessConfig to ensure values are set and retrieved correctly.
   */
  @Test
  void testSettersAndGetters() {
    AsyncProcessConfig config = new AsyncProcessConfig();
    config.setMaxRetries(10);
    config.setRetryDelayMs(5000);
    config.setPrefetchCount(20);
    config.setParallelThreads(8);

    assertEquals(10, config.getMaxRetries());
    assertEquals(5000, config.getRetryDelayMs());
    assertEquals(20, config.getPrefetchCount());
    assertEquals(8, config.getParallelThreads());
  }
}
