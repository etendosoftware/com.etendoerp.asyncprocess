package com.etendoerp.asyncprocess.config;

/**
 * Class that encapsulates advanced configuration for asynchronous processes
 */
public class AsyncProcessConfig {
  private int maxRetries = 3;
  private long retryDelayMs = 1000;
  private int prefetchCount = 5;
  private int parallelThreads = 5;

  public int getMaxRetries() {
    return maxRetries;
  }

  public void setMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
  }

  public long getRetryDelayMs() {
    return retryDelayMs;
  }

  public void setRetryDelayMs(long retryDelayMs) {
    this.retryDelayMs = retryDelayMs;
  }

  public int getPrefetchCount() {
    return prefetchCount;
  }

  public void setPrefetchCount(int prefetchCount) {
    this.prefetchCount = prefetchCount;
  }

  public int getParallelThreads() {
    return parallelThreads;
  }

  public void setParallelThreads(int parallelThreads) {
    this.parallelThreads = parallelThreads;
  }
}
