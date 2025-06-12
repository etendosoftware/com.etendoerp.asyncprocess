package com.etendoerp.asyncprocess.retry;

/**
 * Implementación simple de política de reintentos con un número máximo
 * de intentos y un tiempo de espera fijo entre reintentos
 */
public class SimpleRetryPolicy implements RetryPolicy {
  private final int maxRetries;
  private final long retryDelayMs;

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
