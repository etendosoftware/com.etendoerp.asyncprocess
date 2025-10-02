package com.etendoerp.asyncprocess.retry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@link SimpleRetryPolicy} class.
 * <p>
 * This test class verifies the retry logic and delay configuration of the SimpleRetryPolicy implementation.
 * It ensures that the retry limit and delay are respected and that the constructor stores values correctly.
 */
class SimpleRetryPolicyTest {

  /**
   * Tests that shouldRetry returns true for attempts within the retry limit and false otherwise.
   */
  @Test
  void testShouldRetryWithinLimit() {
    SimpleRetryPolicy policy = new SimpleRetryPolicy(3, 1000);
    assertTrue(policy.shouldRetry(0));
    assertTrue(policy.shouldRetry(1));
    assertTrue(policy.shouldRetry(2));
    assertFalse(policy.shouldRetry(3));
    assertFalse(policy.shouldRetry(4));
  }

  /**
   * Tests that getRetryDelay always returns the configured delay value, regardless of the attempt number.
   */
  @Test
  void testGetRetryDelayAlwaysReturnsConfiguredValue() {
    SimpleRetryPolicy policy = new SimpleRetryPolicy(5, 2000);
    for (int i = 0; i < 10; i++) {
      assertEquals(2000, policy.getRetryDelay(i));
    }
  }

  /**
   * Tests that the constructor correctly stores the maximum retries and delay values.
   */
  @Test
  void testConstructorStoresValues() {
    SimpleRetryPolicy policy = new SimpleRetryPolicy(7, 1500);
    assertTrue(policy.shouldRetry(6));
    assertFalse(policy.shouldRetry(7));
    assertEquals(1500, policy.getRetryDelay(0));
  }
}
