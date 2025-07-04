package com.etendoerp.asyncprocess.retry;

/**
 * Simple implementation of a retry policy with a maximum number
 * of attempts and a fixed wait time between retries.
 */
public class SimpleRetryPolicy implements RetryPolicy {
  private final int maxRetries;
  private final long retryDelayMs;

  /**
   * Constructs a SimpleRetryPolicy with the specified maximum number of retries
   * and the delay between retries.
   *
   * @param maxRetries the maximum number of retry attempts
   * @param retryDelayMs the delay in milliseconds between retries
   */
  public SimpleRetryPolicy(int maxRetries, long retryDelayMs) {
    this.maxRetries = maxRetries;
    this.retryDelayMs = retryDelayMs;
  }

  @Override
  public boolean shouldRetry(int attemptNumber) {
    return attemptNumber < maxRetries;
  }

  @Override
  public long getRetryDelay(int attemptNumber) {
    return retryDelayMs;
  }
}
