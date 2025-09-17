package com.etendoerp.asyncprocess.health;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.etendoerp.asyncprocess.model.AsyncProcessExecution;
import com.etendoerp.asyncprocess.serdes.AsyncProcessExecutionDeserializer;

/**
 * Health checker for Kafka connectivity and consumer status.
 * Monitors the health of Kafka connection and detects disconnected consumers.
 */
public class KafkaHealthChecker {
  private static final Logger log = LogManager.getLogger();
  private static final long DEFAULT_CHECK_INTERVAL_SECONDS = 30;
  private static final long DEFAULT_TIMEOUT_MS = 5000;
  
  private final String kafkaHost;
  private final ScheduledExecutorService scheduler;
  private final AtomicBoolean isKafkaHealthy = new AtomicBoolean(true);
  private final AtomicLong lastSuccessfulCheck = new AtomicLong(System.currentTimeMillis());
  private final Map<String, ConsumerHealthStatus> consumerHealthStatus = new HashMap<>();
  private final long checkIntervalSeconds;
  private final long timeoutMs;
  
  // Listeners for health status changes
  private Runnable onKafkaHealthRestored;
  private Runnable onKafkaHealthLost;
  private ConsumerHealthListener consumerHealthListener;
  
  public interface ConsumerHealthListener {
    void onConsumerUnhealthy(String consumerGroupId, String reason);
    void onConsumerHealthy(String consumerGroupId);
  }
  
  public static class ConsumerHealthStatus {
    private final String groupId;
    private final AtomicBoolean healthy = new AtomicBoolean(true);
    private LocalDateTime lastSeen = LocalDateTime.now();
    private String lastError;
    
    public ConsumerHealthStatus(String groupId) {
      this.groupId = groupId;
    }
    
    public boolean isHealthy() {
      return healthy.get();
    }
    
    public void setHealthy(boolean healthy) {
      this.healthy.set(healthy);
      if (healthy) {
        this.lastSeen = LocalDateTime.now();
        this.lastError = null;
      }
    }
    
    public LocalDateTime getLastSeen() {
      return lastSeen;
    }
    
    public String getLastError() {
      return lastError;
    }
    
    public void setLastError(String lastError) {
      this.lastError = lastError;
    }
    
    public String getGroupId() {
      return groupId;
    }
  }
  
  public KafkaHealthChecker(String kafkaHost) {
    this(kafkaHost, DEFAULT_CHECK_INTERVAL_SECONDS, DEFAULT_TIMEOUT_MS);
  }
  
  public KafkaHealthChecker(String kafkaHost, long checkIntervalSeconds, long timeoutMs) {
    this.kafkaHost = kafkaHost;
    this.checkIntervalSeconds = checkIntervalSeconds;
    this.timeoutMs = timeoutMs;
    this.scheduler = Executors.newScheduledThreadPool(2);
  }
  
  /**
   * Starts the health checker with periodic monitoring.
   */
  public void start() {
    log.info("Starting Kafka health checker with interval {} seconds", checkIntervalSeconds);
    
    // Schedule periodic health checks
    scheduler.scheduleWithFixedDelay(
        this::performHealthCheck,
        0,
        checkIntervalSeconds,
        TimeUnit.SECONDS
    );
    
    // Schedule periodic consumer group monitoring
    scheduler.scheduleWithFixedDelay(
        this::checkConsumerGroups,
        10, // Initial delay
        checkIntervalSeconds * 2, // Less frequent than general health check
        TimeUnit.SECONDS
    );
  }
  
  /**
   * Stops the health checker.
   */
  public void stop() {
    log.info("Stopping Kafka health checker");
    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      scheduler.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
  
  /**
   * Performs a health check on Kafka connectivity.
   */
  private void performHealthCheck() {
    try {
      CompletableFuture<Boolean> healthCheck = CompletableFuture.supplyAsync(() -> {
        try {
          // Test basic Kafka connectivity
          Properties props = new Properties();
          props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost);
          props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
          props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AsyncProcessExecutionDeserializer.class.getName());
          props.put(ConsumerConfig.GROUP_ID_CONFIG, "health-check-" + System.currentTimeMillis());
          props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
          props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
          props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, (int) timeoutMs);
          props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) timeoutMs);
          
          try (KafkaConsumer<String, AsyncProcessExecution> consumer = new KafkaConsumer<>(props)) {
            // Try to get metadata (this will fail if Kafka is not reachable)
            consumer.listTopics(Duration.ofMillis(timeoutMs));
            return true;
          }
        } catch (Exception e) {
          log.debug("Kafka health check failed: {}", e.getMessage());
          return false;
        }
      });
      
      boolean healthy = healthCheck.get(timeoutMs + 1000, TimeUnit.MILLISECONDS);
      updateKafkaHealth(healthy);
      
    } catch (Exception e) {
      log.warn("Health check interrupted or failed: {}", e.getMessage());
      updateKafkaHealth(false);
    }
  }
  
  /**
   * Updates Kafka health status and triggers listeners if status changed.
   */
  private void updateKafkaHealth(boolean healthy) {
    boolean wasHealthy = isKafkaHealthy.getAndSet(healthy);
    
    if (healthy) {
      lastSuccessfulCheck.set(System.currentTimeMillis());
      if (!wasHealthy) {
        log.info("Kafka connectivity restored");
        if (onKafkaHealthRestored != null) {
          try {
            onKafkaHealthRestored.run();
          } catch (Exception e) {
            log.error("Error in health restored callback", e);
          }
        }
      }
    } else {
      if (wasHealthy) {
        log.warn("Kafka connectivity lost");
        if (onKafkaHealthLost != null) {
          try {
            onKafkaHealthLost.run();
          } catch (Exception e) {
            log.error("Error in health lost callback", e);
          }
        }
      }
    }
  }
  
  /**
   * Checks the health of consumer groups.
   */
  private void checkConsumerGroups() {
    try {
      Properties props = new Properties();
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost);
      
      try (AdminClient adminClient = AdminClient.create(props)) {
        ListConsumerGroupsResult listResult = adminClient.listConsumerGroups();
        
        // Get all consumer groups
        listResult.all().get(timeoutMs, TimeUnit.MILLISECONDS).forEach(groupListing -> {
          String groupId = groupListing.groupId();
          
          // Only monitor our async process groups
          if (groupId.startsWith("etendo-ap-group")) {
            checkConsumerGroupHealth(adminClient, groupId);
          }
        });
        
      }
    } catch (Exception e) {
      log.warn("Failed to check consumer groups: {}", e.getMessage());
    }
  }
  
  /**
   * Checks the health of a specific consumer group.
   */
  private void checkConsumerGroupHealth(AdminClient adminClient, String groupId) {
    try {
      ConsumerGroupDescription description = adminClient.describeConsumerGroups(Collections.singleton(groupId))
          .describedGroups()
          .get(groupId)
          .get(timeoutMs, TimeUnit.MILLISECONDS);
      
      ConsumerHealthStatus status = consumerHealthStatus.computeIfAbsent(
          groupId, k -> new ConsumerHealthStatus(groupId));
      
      boolean wasHealthy = status.isHealthy();
      boolean isHealthy = !description.members().isEmpty() && 
                         description.state().name().equals("STABLE");
      
      status.setHealthy(isHealthy);
      
      if (!isHealthy) {
        String reason = description.members().isEmpty() ? 
            "No active members" : 
            "Group state: " + description.state().name();
        status.setLastError(reason);
        
        if (wasHealthy && consumerHealthListener != null) {
          log.warn("Consumer group {} became unhealthy: {}", groupId, reason);
          consumerHealthListener.onConsumerUnhealthy(groupId, reason);
        }
      } else if (!wasHealthy && consumerHealthListener != null) {
        log.info("Consumer group {} is now healthy", groupId);
        consumerHealthListener.onConsumerHealthy(groupId);
      }
      
    } catch (Exception e) {
      log.debug("Failed to check consumer group {}: {}", groupId, e.getMessage());
      ConsumerHealthStatus status = consumerHealthStatus.get(groupId);
      if (status != null) {
        status.setHealthy(false);
        status.setLastError("Check failed: " + e.getMessage());
      }
    }
  }
  
  /**
   * Registers a consumer group for monitoring.
   */
  public void registerConsumerGroup(String groupId) {
    consumerHealthStatus.put(groupId, new ConsumerHealthStatus(groupId));
    log.debug("Registered consumer group for monitoring: {}", groupId);
  }
  
  /**
   * Gets the current Kafka health status.
   */
  public boolean isKafkaHealthy() {
    return isKafkaHealthy.get();
  }
  
  /**
   * Gets the time of last successful health check.
   */
  public long getLastSuccessfulCheck() {
    return lastSuccessfulCheck.get();
  }
  
  /**
   * Gets the health status of all monitored consumer groups.
   */
  public Map<String, ConsumerHealthStatus> getConsumerHealthStatus() {
    return new HashMap<>(consumerHealthStatus);
  }
  
  /**
   * Sets callback for when Kafka health is restored.
   */
  public void setOnKafkaHealthRestored(Runnable callback) {
    this.onKafkaHealthRestored = callback;
  }
  
  /**
   * Sets callback for when Kafka health is lost.
   */
  public void setOnKafkaHealthLost(Runnable callback) {
    this.onKafkaHealthLost = callback;
  }
  
  /**
   * Sets listener for consumer health status changes.
   */
  public void setConsumerHealthListener(ConsumerHealthListener listener) {
    this.consumerHealthListener = listener;
  }
  
  /**
   * Checks if a specific consumer group is healthy.
   */
  public boolean isConsumerGroupHealthy(String groupId) {
    ConsumerHealthStatus status = consumerHealthStatus.get(groupId);
    return status != null && status.isHealthy();
  }
  
  /**
   * Gets detailed health report.
   */
  public String getHealthReport() {
    StringBuilder report = new StringBuilder();
    report.append("=== Kafka Health Report ===\n");
    report.append("Kafka Healthy: ").append(isKafkaHealthy.get()).append("\n");
    report.append("Last Successful Check: ").append(new java.util.Date(lastSuccessfulCheck.get())).append("\n");
    report.append("Consumer Groups:\n");
    
    consumerHealthStatus.forEach((groupId, status) -> {
      report.append("  - ").append(groupId).append(": ");
      report.append(status.isHealthy() ? "HEALTHY" : "UNHEALTHY");
      if (!status.isHealthy() && status.getLastError() != null) {
        report.append(" (").append(status.getLastError()).append(")");
      }
      report.append(" [Last seen: ").append(status.getLastSeen()).append("]\n");
    });
    
    return report.toString();
  }
}