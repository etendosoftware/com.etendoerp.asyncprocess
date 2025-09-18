package com.etendoerp.asyncprocess.recovery;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.etendoerp.asyncprocess.startup.ReceiverRecordConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.etendoerp.asyncprocess.config.AsyncProcessConfig;
import com.etendoerp.asyncprocess.health.KafkaHealthChecker;
import com.etendoerp.asyncprocess.model.AsyncProcessExecution;
import com.etendoerp.asyncprocess.model.AsyncProcessState;
import com.etendoerp.asyncprocess.retry.RetryPolicy;
import com.smf.jobs.Action;

import org.openbravo.base.exception.OBException;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;

/**
 * Auto-recovery manager for Kafka consumers.
 * Monitors consumer health and automatically recreates failed consumers.
 */
public class ConsumerRecoveryManager {
  private static final Logger log = LogManager.getLogger();
  private static final int DEFAULT_MAX_RECOVERY_ATTEMPTS = 5;
  private static final long DEFAULT_RECOVERY_DELAY_MS = 10000; // 10 seconds
  private static final long DEFAULT_RECOVERY_BACKOFF_MULTIPLIER = 2;
  
  private final KafkaHealthChecker healthChecker;
  private final ScheduledExecutorService recoveryScheduler;
  private final Map<String, ConsumerInfo> activeConsumers = new ConcurrentHashMap<>();
  private final Map<String, AtomicInteger> recoveryAttempts = new ConcurrentHashMap<>();
  private final AtomicBoolean isRecoveryEnabled = new AtomicBoolean(true);
  
  private final int maxRecoveryAttempts;
  private final long baseRecoveryDelayMs;
  private final long recoveryBackoffMultiplier;
  
  // Consumer recreation function
  private ConsumerRecreationFunction consumerRecreationFunction;
  
  @FunctionalInterface
  public interface ConsumerRecreationFunction {
    Flux<ReceiverRecord<String, AsyncProcessExecution>> recreateConsumer(ConsumerInfo consumerInfo);
  }
  
  public static class ConsumerInfo {
    private final String consumerId;
    private final String groupId;
    private final String topic;
    private final boolean isRegExp;
    private final AsyncProcessConfig config;
    private final String jobLineId;
    private final Supplier<Action> actionFactory;
    private final String nextTopic;
    private final String errorTopic;
    private final AsyncProcessState targetStatus;
    private final KafkaSender<String, AsyncProcessExecution> kafkaSender;
    private final String clientId;
    private final String orgId;
    private final RetryPolicy retryPolicy;
    private final ScheduledExecutorService scheduler;
    private final String kafkaHost;
    
    private Disposable subscription;
    private boolean isActive = true;
    
    private ConsumerInfo(Builder builder) {
      this.consumerId = builder.consumerId;
      this.groupId = builder.groupId;
      this.topic = builder.topic;
      this.isRegExp = builder.isRegExp;
      this.config = builder.config;
      this.jobLineId = builder.jobLineId;
      this.actionFactory = builder.actionFactory;
      this.nextTopic = builder.nextTopic;
      this.errorTopic = builder.errorTopic;
      this.targetStatus = builder.targetStatus;
      this.kafkaSender = builder.kafkaSender;
      this.clientId = builder.clientId;
      this.orgId = builder.orgId;
      this.retryPolicy = builder.retryPolicy;
      this.scheduler = builder.scheduler;
      this.kafkaHost = builder.kafkaHost;
    }

    public static class Builder {
      private String consumerId;
      private String groupId;
      private String topic;
      private boolean isRegExp;
      private AsyncProcessConfig config;
      private String jobLineId;
      private Supplier<Action> actionFactory;
      private String nextTopic;
      private String errorTopic;
      private AsyncProcessState targetStatus;
      private KafkaSender<String, AsyncProcessExecution> kafkaSender;
      private String clientId;
      private String orgId;
      private RetryPolicy retryPolicy;
      private ScheduledExecutorService scheduler;
      private String kafkaHost;

      public Builder consumerId(String consumerId) {
        this.consumerId = consumerId;
        return this;
      }

      public Builder groupId(String groupId) {
        this.groupId = groupId;
        return this;
      }

      public Builder topic(String topic) {
        this.topic = topic;
        return this;
      }

      public Builder isRegExp(boolean isRegExp) {
        this.isRegExp = isRegExp;
        return this;
      }

      public Builder config(AsyncProcessConfig config) {
        this.config = config;
        return this;
      }

      public Builder jobLineId(String jobLineId) {
        this.jobLineId = jobLineId;
        return this;
      }

      public Builder actionFactory(Supplier<Action> actionFactory) {
        this.actionFactory = actionFactory;
        return this;
      }

      public Builder nextTopic(String nextTopic) {
        this.nextTopic = nextTopic;
        return this;
      }

      public Builder errorTopic(String errorTopic) {
        this.errorTopic = errorTopic;
        return this;
      }

      public Builder targetStatus(AsyncProcessState targetStatus) {
        this.targetStatus = targetStatus;
        return this;
      }

      public Builder kafkaSender(KafkaSender<String, AsyncProcessExecution> kafkaSender) {
        this.kafkaSender = kafkaSender;
        return this;
      }

      public Builder clientId(String clientId) {
        this.clientId = clientId;
        return this;
      }

      public Builder orgId(String orgId) {
        this.orgId = orgId;
        return this;
      }

      public Builder retryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
        return this;
      }

      public Builder scheduler(ScheduledExecutorService scheduler) {
        this.scheduler = scheduler;
        return this;
      }

      public Builder kafkaHost(String kafkaHost) {
        this.kafkaHost = kafkaHost;
        return this;
      }

      public ConsumerInfo build() {
        // Validate required fields
        if (consumerId == null || consumerId.trim().isEmpty()) {
          throw new IllegalArgumentException("consumerId is required");
        }
        if (groupId == null || groupId.trim().isEmpty()) {
          throw new IllegalArgumentException("groupId is required");
        }
        if (topic == null || topic.trim().isEmpty()) {
          throw new IllegalArgumentException("topic is required");
        }
        if (jobLineId == null || jobLineId.trim().isEmpty()) {
          throw new IllegalArgumentException("jobLineId is required");
        }
        if (actionFactory == null) {
          throw new IllegalArgumentException("actionFactory is required");
        }
        if (kafkaSender == null) {
          throw new IllegalArgumentException("kafkaSender is required");
        }

        return new ConsumerInfo(this);
      }
    }
    
    // Getters
    public String getConsumerId() { return consumerId; }
    public String getGroupId() { return groupId; }
    public String getTopic() { return topic; }
    public boolean isRegExp() { return isRegExp; }
    public AsyncProcessConfig getConfig() { return config; }
    public String getJobLineId() { return jobLineId; }
    public Supplier<Action> getActionFactory() { return actionFactory; }
    public String getNextTopic() { return nextTopic; }
    public String getErrorTopic() { return errorTopic; }
    public AsyncProcessState getTargetStatus() { return targetStatus; }
    public KafkaSender<String, AsyncProcessExecution> getKafkaSender() { return kafkaSender; }
    public String getClientId() { return clientId; }
    public String getOrgId() { return orgId; }
    public RetryPolicy getRetryPolicy() { return retryPolicy; }
    public ScheduledExecutorService getScheduler() { return scheduler; }
    public String getKafkaHost() { return kafkaHost; }
    
    public Disposable getSubscription() { return subscription; }
    public void setSubscription(Disposable subscription) { this.subscription = subscription; }
    
    public boolean isActive() { return isActive; }
    public void setActive(boolean active) { this.isActive = active; }
  }
  
  public ConsumerRecoveryManager(KafkaHealthChecker healthChecker) {
    this(healthChecker, DEFAULT_MAX_RECOVERY_ATTEMPTS, DEFAULT_RECOVERY_DELAY_MS, DEFAULT_RECOVERY_BACKOFF_MULTIPLIER);
  }
  
  public ConsumerRecoveryManager(KafkaHealthChecker healthChecker, int maxRecoveryAttempts, 
                                long baseRecoveryDelayMs, long recoveryBackoffMultiplier) {
    this.healthChecker = healthChecker;
    this.maxRecoveryAttempts = maxRecoveryAttempts;
    this.baseRecoveryDelayMs = baseRecoveryDelayMs;
    this.recoveryBackoffMultiplier = recoveryBackoffMultiplier;
    this.recoveryScheduler = Executors.newScheduledThreadPool(2);
    
    setupHealthListeners();
  }
  
  /**
   * Sets up listeners for health check events.
   */
  private void setupHealthListeners() {
    healthChecker.setConsumerHealthListener(new KafkaHealthChecker.ConsumerHealthListener() {
      @Override
      public void onConsumerUnhealthy(String consumerGroupId, String reason) {
        if (isRecoveryEnabled.get()) {
          log.warn("Consumer group {} is unhealthy: {}. Scheduling recovery...", consumerGroupId, reason);
          scheduleConsumerRecovery(consumerGroupId, reason);
        }
      }
      
      @Override
      public void onConsumerHealthy(String consumerGroupId) {
        // Reset recovery attempts when consumer becomes healthy
        recoveryAttempts.remove(consumerGroupId);
        log.info("Consumer group {} is healthy again", consumerGroupId);
      }
    });
    
    healthChecker.setOnKafkaHealthRestored(() -> {
      if (isRecoveryEnabled.get()) {
        log.info("Kafka health restored. Checking for consumers to recover...");
        recoverAllInactiveConsumers();
      }
    });
    
    healthChecker.setOnKafkaHealthLost(() ->
        log.warn("Kafka health lost. Consumer recovery will be delayed until Kafka is restored."));
  }
  
  /**
   * Registers a consumer for monitoring and potential recovery.
   */
  public void registerConsumer(ConsumerInfo consumerInfo) {
    activeConsumers.put(consumerInfo.getConsumerId(), consumerInfo);
    healthChecker.registerConsumerGroup(consumerInfo.getGroupId());
    log.info("Registered consumer {} (group: {}) for recovery monitoring", 
             consumerInfo.getConsumerId(), consumerInfo.getGroupId());
  }
  
  /**
   * Unregisters a consumer from monitoring.
   */
  public void unregisterConsumer(String consumerId) {
    ConsumerInfo consumerInfo = activeConsumers.remove(consumerId);
    if (consumerInfo != null) {
      consumerInfo.setActive(false);
      if (consumerInfo.getSubscription() != null && !consumerInfo.getSubscription().isDisposed()) {
        consumerInfo.getSubscription().dispose();
      }
      log.info("Unregistered consumer {} from recovery monitoring", consumerId);
    }
  }
  
  /**
   * Sets the function to recreate consumers.
   */
  public void setConsumerRecreationFunction(ConsumerRecreationFunction function) {
    this.consumerRecreationFunction = function;
  }
  
  /**
   * Schedules recovery for a specific consumer group.
   */
  private void scheduleConsumerRecovery(String consumerGroupId, String reason) {
    if (!healthChecker.isKafkaHealthy()) {
      log.info("Kafka is not healthy. Delaying recovery for consumer group {}", consumerGroupId);
      return;
    }
    
    AtomicInteger attempts = recoveryAttempts.computeIfAbsent(consumerGroupId, k -> new AtomicInteger(0));
    int currentAttempt = attempts.incrementAndGet();
    
    if (currentAttempt > maxRecoveryAttempts) {
      log.error("Max recovery attempts ({}) reached for consumer group {}. Giving up.", 
                maxRecoveryAttempts, consumerGroupId);
      return;
    }
    
    long delay = calculateRecoveryDelay(currentAttempt);
    
    log.info("Scheduling recovery attempt {} for consumer group {} in {} ms", 
             currentAttempt, consumerGroupId, delay);
    
    recoveryScheduler.schedule(() -> {
      try {
        recoverConsumerGroup(consumerGroupId, reason);
      } catch (Exception e) {
        log.error("Error during recovery of consumer group {}: {}", consumerGroupId, e.getMessage(), e);
      }
    }, delay, TimeUnit.MILLISECONDS);
  }
  
  /**
   * Calculates recovery delay with exponential backoff.
   */
  private long calculateRecoveryDelay(int attempt) {
    return baseRecoveryDelayMs * (long) Math.pow(recoveryBackoffMultiplier, attempt - 1f);
  }
  
  /**
   * Recovers consumers for a specific consumer group.
   */
  private void recoverConsumerGroup(String consumerGroupId, String reason) {
    log.info("Attempting to recover consumer group: {}", consumerGroupId);
    
    // Find all consumers for this group
    List<ConsumerInfo> groupConsumers = activeConsumers.values().stream()
        .filter(consumer -> consumerGroupId.equals(consumer.getGroupId()))
        .collect(Collectors.toList());
    
    if (groupConsumers.isEmpty()) {
      log.warn("No consumers found for group {}", consumerGroupId);
      return;
    }
    
    for (ConsumerInfo consumerInfo : groupConsumers) {
      try {
        recoverConsumer(consumerInfo, reason);
      } catch (Exception e) {
        log.error("Failed to recover consumer {}: {}", consumerInfo.getConsumerId(), e.getMessage(), e);
      }
    }
  }
  
  /**
   * Recovers a specific consumer.
   */
  private void recoverConsumer(ConsumerInfo consumerInfo, String reason) {
    log.info("Recovering consumer {} due to: {}", consumerInfo.getConsumerId(), reason);
    
    // Dispose old subscription if exists
    if (consumerInfo.getSubscription() != null && !consumerInfo.getSubscription().isDisposed()) {
      try {
        consumerInfo.getSubscription().dispose();
        log.debug("Disposed old subscription for consumer {}", consumerInfo.getConsumerId());
      } catch (Exception e) {
        log.warn("Error disposing old subscription for consumer {}: {}", 
                 consumerInfo.getConsumerId(), e.getMessage());
      }
    }
    
    if (consumerRecreationFunction == null) {
      log.error("Consumer recreation function not set. Cannot recover consumer {}", 
                consumerInfo.getConsumerId());
      return;
    }
    
    try {
      // Recreate the consumer
      Flux<ReceiverRecord<String, AsyncProcessExecution>> receiver = 
          consumerRecreationFunction.recreateConsumer(consumerInfo);
      
      // Subscribe with new consumer
      Disposable newSubscription = receiver.subscribe(
          new ReceiverRecordConsumer(
              new ReceiverRecordConsumer.ConsumerConfig.Builder()
                  .jobId(consumerInfo.getJobLineId())
                  .actionFactory(consumerInfo.getActionFactory())
                  .nextTopic(consumerInfo.getNextTopic())
                  .errorTopic(consumerInfo.getErrorTopic())
                  .targetStatus(consumerInfo.getTargetStatus())
                  .kafkaSender(consumerInfo.getKafkaSender())
                  .clientId(consumerInfo.getClientId())
                  .orgId(consumerInfo.getOrgId())
                  .retryPolicy(consumerInfo.getRetryPolicy())
                  .scheduler(consumerInfo.getScheduler())
                  .build()
          ),
          error -> {
            log.error("Error in recovered consumer {}: {}", consumerInfo.getConsumerId(), error.getMessage(), error);
            // Schedule another recovery if needed
            if (isRecoveryEnabled.get()) {
              scheduleConsumerRecovery(consumerInfo.getGroupId(), "Consumer error: " + error.getMessage());
            }
          },
          () -> log.info("Consumer {} completed", consumerInfo.getConsumerId())
      );
      
      consumerInfo.setSubscription(newSubscription);
      consumerInfo.setActive(true);
      
      log.info("Successfully recovered consumer {}", consumerInfo.getConsumerId());
      
    } catch (Exception e) {
      log.error("Failed to recover consumer {}: {}", consumerInfo.getConsumerId(), e.getMessage(), e);
      throw e;
    }
  }
  
  /**
   * Attempts to recover all inactive consumers.
   */
  private void recoverAllInactiveConsumers() {
    log.info("Checking for inactive consumers to recover...");
    
    CompletableFuture.runAsync(() ->
      activeConsumers.values().stream()
          .filter(consumer -> !consumer.isActive() || 
                            (consumer.getSubscription() != null && consumer.getSubscription().isDisposed()))
          .forEach(consumer -> {
            try {
              recoverConsumer(consumer, "Auto-recovery on Kafka health restoration");
            } catch (Exception e) {
              log.error("Failed to auto-recover consumer {}: {}", 
                       consumer.getConsumerId(), e.getMessage(), e);
            }
          }), recoveryScheduler);
  }
  
  /**
   * Enables or disables auto-recovery.
   */
  public void setRecoveryEnabled(boolean enabled) {
    boolean wasEnabled = isRecoveryEnabled.getAndSet(enabled);
    if (enabled && !wasEnabled) {
      log.info("Consumer auto-recovery enabled");
      // Trigger immediate check if recovery was re-enabled
      if (healthChecker.isKafkaHealthy()) {
        recoverAllInactiveConsumers();
      }
    } else if (!enabled && wasEnabled) {
      log.info("Consumer auto-recovery disabled");
    }
  }
  
  /**
   * Checks if auto-recovery is enabled.
   */
  public boolean isRecoveryEnabled() {
    return isRecoveryEnabled.get();
  }
  
  /**
   * Gets recovery status for all consumer groups.
   */
  public Map<String, Object> getRecoveryStatus() {
    Map<String, Object> status = new HashMap<>();
    status.put("recoveryEnabled", isRecoveryEnabled.get());
    status.put("maxRecoveryAttempts", maxRecoveryAttempts);
    status.put("baseRecoveryDelayMs", baseRecoveryDelayMs);
    
    Map<String, Object> consumers = new HashMap<>();
    activeConsumers.forEach((id, info) -> {
      Map<String, Object> consumerStatus = new HashMap<>();
      consumerStatus.put("groupId", info.getGroupId());
      consumerStatus.put("topic", info.getTopic());
      consumerStatus.put("active", info.isActive());
      consumerStatus.put("subscriptionActive", 
                        info.getSubscription() != null && !info.getSubscription().isDisposed());
      
      AtomicInteger attempts = recoveryAttempts.get(info.getGroupId());
      consumerStatus.put("recoveryAttempts", attempts != null ? attempts.get() : 0);
      
      consumers.put(id, consumerStatus);
    });
    status.put("consumers", consumers);
    
    return status;
  }
  
  /**
   * Forces recovery of a specific consumer.
   */
  public void forceRecoverConsumer(String consumerId) {
    ConsumerInfo consumerInfo = activeConsumers.get(consumerId);
    if (consumerInfo == null) {
      throw new IllegalArgumentException("Consumer not found: " + consumerId);
    }
    
    log.info("Forcing recovery of consumer: {}", consumerId);
    try {
      recoverConsumer(consumerInfo, "Manual recovery request");
    } catch (Exception e) {
      log.error("Failed to force recover consumer {}: {}", consumerId, e.getMessage(), e);
      throw new OBException("Recovery failed", e);
    }
  }
  
  /**
   * Shuts down the recovery manager.
   */
  public void shutdown() {
    log.info("Shutting down consumer recovery manager");
    isRecoveryEnabled.set(false);
    
    // Dispose all active subscriptions
    activeConsumers.values().forEach(consumer -> {
      if (consumer.getSubscription() != null && !consumer.getSubscription().isDisposed()) {
        consumer.getSubscription().dispose();
      }
    });
    
    recoveryScheduler.shutdown();
    try {
      if (!recoveryScheduler.awaitTermination(10, TimeUnit.SECONDS)) {
        recoveryScheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      recoveryScheduler.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
