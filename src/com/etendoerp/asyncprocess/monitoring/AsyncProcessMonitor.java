package com.etendoerp.asyncprocess.monitoring;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Comprehensive monitoring and alerting system for async process operations.
 * Collects metrics, detects anomalies, and triggers alerts.
 */
public class AsyncProcessMonitor {
  private static final Logger log = LogManager.getLogger();
  private static final long DEFAULT_METRICS_COLLECTION_INTERVAL_MS = 10000; // 10 seconds
  private static final long DEFAULT_ALERT_CHECK_INTERVAL_MS = 30000; // 30 seconds
  public static final String NUMBER_FORMAT = "%.2f%%";

  // Metrics storage
  private final Map<String, JobMetrics> jobMetrics = new ConcurrentHashMap<>();
  private final Map<String, ConsumerMetrics> consumerMetrics = new ConcurrentHashMap<>();
  private final KafkaMetrics kafkaMetrics = new KafkaMetrics();
  private final SystemMetrics systemMetrics = new SystemMetrics();
  
  // Alert management
  private final List<AlertRule> alertRules = new ArrayList<>();
  private final Map<String, AlertState> alertStates = new ConcurrentHashMap<>();
  
  private final ScheduledExecutorService monitoringScheduler;
  private final long metricsCollectionIntervalMs;
  private final long alertCheckIntervalMs;
  
  // Listeners
  private final List<MetricsListener> metricsListeners = new ArrayList<>();
  private final List<AlertListener> alertListeners = new ArrayList<>();
  
  public interface MetricsListener {
    void onMetricsUpdate(MetricsSnapshot snapshot);
  }
  
  public interface AlertListener {
    void onAlert(Alert alert);
    void onAlertResolved(Alert alert);
  }
  
  public static class JobMetrics {
    private final String jobId;
    private final String jobName;
    private final LongAdder messagesProcessed = new LongAdder();
    private final LongAdder messagesSucceeded = new LongAdder();
    private final LongAdder messagesFailed = new LongAdder();
    private final LongAdder messagesRetried = new LongAdder();
    private final LongAdder processingTimeMs = new LongAdder();
    private final AtomicLong lastProcessedTime = new AtomicLong(0);
    private final AtomicLong averageProcessingTime = new AtomicLong(0);
    
    public JobMetrics(String jobId, String jobName) {
      this.jobId = jobId;
      this.jobName = jobName;
    }
    
    public void recordProcessedMessage(long processingTime, boolean success, boolean isRetry) {
      messagesProcessed.increment();
      if (success) {
        messagesSucceeded.increment();
      } else {
        messagesFailed.increment();
      }
      if (isRetry) {
        messagesRetried.increment();
      }
      
      processingTimeMs.add(processingTime);
      lastProcessedTime.set(System.currentTimeMillis());
      
      // Calculate moving average
      long total = messagesProcessed.sum();
      if (total > 0) {
        averageProcessingTime.set(this.processingTimeMs.sum() / total);
      }
    }
    
    // Getters
    public String getJobId() { return jobId; }
    public String getJobName() { return jobName; }
    public long getMessagesProcessed() { return messagesProcessed.sum(); }
    public long getMessagesSucceeded() { return messagesSucceeded.sum(); }
    public long getMessagesFailed() { return messagesFailed.sum(); }
    public long getMessagesRetried() { return messagesRetried.sum(); }
    public long getTotalProcessingTime() { return processingTimeMs.sum(); }
    public long getLastProcessedTime() { return lastProcessedTime.get(); }
    public long getAverageProcessingTime() { return averageProcessingTime.get(); }
    
    public double getSuccessRate() {
      long total = messagesProcessed.sum();
      return total > 0 ? (double) messagesSucceeded.sum() / total * 100 : 100.0;
    }
    
    public double getFailureRate() {
      long total = messagesProcessed.sum();
      return total > 0 ? (double) messagesFailed.sum() / total * 100 : 0.0;
    }
    
    public double getRetryRate() {
      long total = messagesProcessed.sum();
      return total > 0 ? (double) messagesRetried.sum() / total * 100 : 0.0;
    }
  }
  
  public static class ConsumerMetrics {
    private final String consumerId;
    private final String groupId;
    private final String topic;
    private final LongAdder messagesConsumed = new LongAdder();
    private final LongAdder connectionLost = new LongAdder();
    private final LongAdder reconnections = new LongAdder();
    private final AtomicLong lastActivity = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong lagTotal = new AtomicLong(0);
    
    public ConsumerMetrics(String consumerId, String groupId, String topic) {
      this.consumerId = consumerId;
      this.groupId = groupId;
      this.topic = topic;
    }
    
    public void recordMessage() {
      messagesConsumed.increment();
      lastActivity.set(System.currentTimeMillis());
    }
    
    public void recordConnectionLost() {
      connectionLost.increment();
    }
    
    public void recordReconnection() {
      reconnections.increment();
      lastActivity.set(System.currentTimeMillis());
    }
    
    public void updateLag(long lag) {
      lagTotal.set(lag);
    }
    
    // Getters
    public String getConsumerId() { return consumerId; }
    public String getGroupId() { return groupId; }
    public String getTopic() { return topic; }
    public long getMessagesConsumed() { return messagesConsumed.sum(); }
    public long getConnectionLost() { return connectionLost.sum(); }
    public long getReconnections() { return reconnections.sum(); }
    public long getLastActivity() { return lastActivity.get(); }
    public long getLag() { return lagTotal.get(); }
    
    public boolean isIdle(long idleThresholdMs) {
      return System.currentTimeMillis() - lastActivity.get() > idleThresholdMs;
    }
  }
  
  public static class KafkaMetrics {
    private final LongAdder connectionAttempts = new LongAdder();
    private final LongAdder connectionFailures = new LongAdder();
    private final LongAdder messagesSent = new LongAdder();
    private final LongAdder sendFailures = new LongAdder();
    private final AtomicLong lastConnectionTime = new AtomicLong(0);
    private final AtomicLong avgSendTime = new AtomicLong(0);
    
    public void recordConnectionAttempt(boolean success) {
      connectionAttempts.increment();
      if (success) {
        lastConnectionTime.set(System.currentTimeMillis());
      } else {
        connectionFailures.increment();
      }
    }
    
    public void recordMessageSent(long sendTime, boolean success) {
      messagesSent.increment();
      if (!success) {
        sendFailures.increment();
      }
      
      // Update average send time
      long total = messagesSent.sum();
      if (total > 0) {
        avgSendTime.updateAndGet(current -> (current * (total - 1) + sendTime) / total);
      }
    }
    
    // Getters
    public long getConnectionAttempts() { return connectionAttempts.sum(); }
    public long getConnectionFailures() { return connectionFailures.sum(); }
    public long getMessagesSent() { return messagesSent.sum(); }
    public long getSendFailures() { return sendFailures.sum(); }
    public long getLastConnectionTime() { return lastConnectionTime.get(); }
    public long getAverageSendTime() { return avgSendTime.get(); }
    
    public double getConnectionSuccessRate() {
      long total = connectionAttempts.sum();
      return total > 0 ? (double) (total - connectionFailures.sum()) / total * 100 : 100.0;
    }
    
    public double getSendSuccessRate() {
      long total = messagesSent.sum();
      return total > 0 ? (double) (total - sendFailures.sum()) / total * 100 : 100.0;
    }
  }
  
  public static class SystemMetrics {
    private final AtomicLong heapUsedBytes = new AtomicLong(0);
    private final AtomicLong heapMaxBytes = new AtomicLong(0);
    private final AtomicLong activeThreads = new AtomicLong(0);
    private final AtomicLong cpuUsagePercent = new AtomicLong(0);
    
    public void updateMemoryUsage(long used, long max) {
      heapUsedBytes.set(used);
      heapMaxBytes.set(max);
    }
    
    public void updateThreadCount(long count) {
      activeThreads.set(count);
    }
    
    public void updateCpuUsage(long usage) {
      cpuUsagePercent.set(usage);
    }
    
    // Getters
    public long getHeapUsedBytes() { return heapUsedBytes.get(); }
    public long getHeapMaxBytes() { return heapMaxBytes.get(); }
    public long getActiveThreads() { return activeThreads.get(); }
    public long getCpuUsagePercent() { return cpuUsagePercent.get(); }
    
    public double getHeapUsagePercent() {
      long max = heapMaxBytes.get();
      return max > 0 ? (double) heapUsedBytes.get() / max * 100 : 0.0;
    }
  }
  
  public static class MetricsSnapshot {
    private final LocalDateTime timestamp;
    private final Map<String, JobMetrics> jobMetrics;
    private final Map<String, ConsumerMetrics> consumerMetrics;
    private final KafkaMetrics kafkaMetrics;
    private final SystemMetrics systemMetrics;
    
    public MetricsSnapshot(LocalDateTime timestamp, Map<String, JobMetrics> jobMetrics,
                          Map<String, ConsumerMetrics> consumerMetrics,
                          KafkaMetrics kafkaMetrics, SystemMetrics systemMetrics) {
      this.timestamp = timestamp;
      this.jobMetrics = new HashMap<>(jobMetrics);
      this.consumerMetrics = new HashMap<>(consumerMetrics);
      this.kafkaMetrics = kafkaMetrics;
      this.systemMetrics = systemMetrics;
    }
    
    // Getters
    public LocalDateTime getTimestamp() { return timestamp; }
    public Map<String, JobMetrics> getJobMetrics() { return jobMetrics; }
    public Map<String, ConsumerMetrics> getConsumerMetrics() { return consumerMetrics; }
    public KafkaMetrics getKafkaMetrics() { return kafkaMetrics; }
    public SystemMetrics getSystemMetrics() { return systemMetrics; }
  }
  
  public enum AlertSeverity {
    INFO, WARNING, CRITICAL
  }
  
  public enum AlertType {
    HIGH_FAILURE_RATE,
    HIGH_PROCESSING_TIME,
    CONSUMER_LAG,
    CONSUMER_IDLE,
    KAFKA_CONNECTION_ISSUE,
    HIGH_MEMORY_USAGE,
    HIGH_CPU_USAGE,
    CIRCUIT_BREAKER_OPEN
  }
  
  public static class Alert {
    private final String id;
    private final AlertType type;
    private final AlertSeverity severity;
    private final String message;
    private final String source;
    private final LocalDateTime timestamp;
    private final Map<String, Object> metadata;
    
    public Alert(String id, AlertType type, AlertSeverity severity, String message, 
                String source, LocalDateTime timestamp, Map<String, Object> metadata) {
      this.id = id;
      this.type = type;
      this.severity = severity;
      this.message = message;
      this.source = source;
      this.timestamp = timestamp;
      this.metadata = metadata != null ? new HashMap<>(metadata) : new HashMap<>();
    }
    
    // Getters
    public String getId() { return id; }
    public AlertType getType() { return type; }
    public AlertSeverity getSeverity() { return severity; }
    public String getMessage() { return message; }
    public String getSource() { return source; }
    public LocalDateTime getTimestamp() { return timestamp; }
    public Map<String, Object> getMetadata() { return metadata; }
  }
  
  public static class AlertRule {
    private final String name;
    private final AlertType type;
    private final AlertSeverity severity;
    private final MetricsPredicate condition;
    private final String messageTemplate;
    
    @FunctionalInterface
    public interface MetricsPredicate {
      boolean test(MetricsSnapshot snapshot);
    }
    
    public AlertRule(String name, AlertType type, AlertSeverity severity, 
                    MetricsPredicate condition, String messageTemplate) {
      this.name = name;
      this.type = type;
      this.severity = severity;
      this.condition = condition;
      this.messageTemplate = messageTemplate;
    }
    
    // Getters
    public String getName() { return name; }
    public AlertType getType() { return type; }
    public AlertSeverity getSeverity() { return severity; }
    public MetricsPredicate getCondition() { return condition; }
    public String getMessageTemplate() { return messageTemplate; }
  }
  
  public static class AlertState {
    private final String ruleId;
    private boolean isActive;
    private LocalDateTime lastTriggered;
    private LocalDateTime lastResolved;
    private int triggerCount;
    
    public AlertState(String ruleId) {
      this.ruleId = ruleId;
      this.isActive = false;
      this.triggerCount = 0;
    }
    
    public void trigger() {
      if (!isActive) {
        isActive = true;
        lastTriggered = LocalDateTime.now();
      }
      triggerCount++;
    }
    
    public void resolve() {
      if (isActive) {
        isActive = false;
        lastResolved = LocalDateTime.now();
      }
    }
    
    // Getters
    public String getRuleId() { return ruleId; }
    public boolean isActive() { return isActive; }
    public LocalDateTime getLastTriggered() { return lastTriggered; }
    public LocalDateTime getLastResolved() { return lastResolved; }
    public int getTriggerCount() { return triggerCount; }
  }
  
  public AsyncProcessMonitor() {
    this(DEFAULT_METRICS_COLLECTION_INTERVAL_MS, DEFAULT_ALERT_CHECK_INTERVAL_MS);
  }
  
  public AsyncProcessMonitor(long metricsCollectionIntervalMs, long alertCheckIntervalMs) {
    this.metricsCollectionIntervalMs = metricsCollectionIntervalMs;
    this.alertCheckIntervalMs = alertCheckIntervalMs;
    this.monitoringScheduler = Executors.newScheduledThreadPool(3);
    
    setupDefaultAlertRules();
  }
  
  /**
   * Starts monitoring and alerting.
   */
  public void start() {
    log.info("Starting async process monitoring");
    
    // Schedule metrics collection
    monitoringScheduler.scheduleWithFixedDelay(
        this::collectSystemMetrics,
        0,
        metricsCollectionIntervalMs,
        TimeUnit.MILLISECONDS
    );
    
    // Schedule metrics broadcasting
    monitoringScheduler.scheduleWithFixedDelay(
        this::broadcastMetrics,
        metricsCollectionIntervalMs / 2,
        metricsCollectionIntervalMs,
        TimeUnit.MILLISECONDS
    );
    
    // Schedule alert checking
    monitoringScheduler.scheduleWithFixedDelay(
        this::checkAlerts,
        alertCheckIntervalMs,
        alertCheckIntervalMs,
        TimeUnit.MILLISECONDS
    );
  }
  
  /**
   * Stops monitoring.
   */
  public void stop() {
    log.info("Stopping async process monitoring");
    monitoringScheduler.shutdown();
    try {
      if (!monitoringScheduler.awaitTermination(10, TimeUnit.SECONDS)) {
        monitoringScheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      monitoringScheduler.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
  
  /**
   * Records metrics for job execution.
   */
  public void recordJobExecution(String jobId, String jobName, long processingTime, 
                                boolean success, boolean isRetry) {
    JobMetrics metrics = jobMetrics.computeIfAbsent(jobId, k -> new JobMetrics(jobId, jobName));
    metrics.recordProcessedMessage(processingTime, success, isRetry);
  }
  
  /**
   * Records consumer activity.
   */
  public void recordConsumerActivity(String consumerId, String groupId, String topic) {
    ConsumerMetrics metrics = consumerMetrics.computeIfAbsent(consumerId, 
        k -> new ConsumerMetrics(consumerId, groupId, topic));
    metrics.recordMessage();
  }
  
  /**
   * Records consumer connection issues.
   */
  public void recordConsumerConnectionLost(String consumerId) {
    ConsumerMetrics metrics = consumerMetrics.get(consumerId);
    if (metrics != null) {
      metrics.recordConnectionLost();
    }
  }
  
  /**
   * Records consumer reconnection.
   */
  public void recordConsumerReconnection(String consumerId) {
    ConsumerMetrics metrics = consumerMetrics.get(consumerId);
    if (metrics != null) {
      metrics.recordReconnection();
    }
  }
  
  /**
   * Updates consumer lag.
   */
  public void updateConsumerLag(String consumerId, long lag) {
    ConsumerMetrics metrics = consumerMetrics.get(consumerId);
    if (metrics != null) {
      metrics.updateLag(lag);
    }
  }
  
  /**
   * Records Kafka connection attempt.
   */
  public void recordKafkaConnection(boolean success) {
    kafkaMetrics.recordConnectionAttempt(success);
  }
  
  /**
   * Records message send to Kafka.
   */
  public void recordKafkaMessageSent(long sendTime, boolean success) {
    kafkaMetrics.recordMessageSent(sendTime, success);
  }
  
  /**
   * Collects system metrics.
   */
  private void collectSystemMetrics() {
    try {
      // Memory metrics
      Runtime runtime = Runtime.getRuntime();
      long totalMemory = runtime.totalMemory();
      long freeMemory = runtime.freeMemory();
      long usedMemory = totalMemory - freeMemory;
      long maxMemory = runtime.maxMemory();
      
      systemMetrics.updateMemoryUsage(usedMemory, maxMemory);
      
      // Thread metrics
      systemMetrics.updateThreadCount(Thread.activeCount());
      
      // CPU metrics (simplified - in production you might want to use JMX)
      systemMetrics.updateCpuUsage(0); // Placeholder
      
    } catch (Exception e) {
      log.warn("Error collecting system metrics: {}", e.getMessage());
    }
  }
  
  /**
   * Broadcasts current metrics to listeners.
   */
  private void broadcastMetrics() {
    try {
      MetricsSnapshot snapshot = new MetricsSnapshot(
          LocalDateTime.now(),
          jobMetrics,
          consumerMetrics,
          kafkaMetrics,
          systemMetrics
      );
      
      for (MetricsListener listener : metricsListeners) {
        try {
          listener.onMetricsUpdate(snapshot);
        } catch (Exception e) {
          log.warn("Error notifying metrics listener: {}", e.getMessage());
        }
      }
    } catch (Exception e) {
      log.warn("Error broadcasting metrics: {}", e.getMessage());
    }
  }
  
  /**
   * Checks alert rules and triggers alerts.
   */
  private void checkAlerts() {
    try {
      MetricsSnapshot snapshot = new MetricsSnapshot(
          LocalDateTime.now(),
          jobMetrics,
          consumerMetrics,
          kafkaMetrics,
          systemMetrics
      );
      
      for (AlertRule rule : alertRules) {
        try {
          checkAlertRule(rule, snapshot);
        } catch (Exception e) {
          log.warn("Error checking alert rule {}: {}", rule.getName(), e.getMessage());
        }
      }
    } catch (Exception e) {
      log.warn("Error checking alerts: {}", e.getMessage());
    }
  }
  
  /**
   * Checks a specific alert rule.
   */
  private void checkAlertRule(AlertRule rule, MetricsSnapshot snapshot) {
    AlertState state = alertStates.computeIfAbsent(rule.getName(), AlertState::new);
    
    boolean conditionMet = rule.getCondition().test(snapshot);
    
    if (conditionMet && !state.isActive()) {
      // Trigger alert
      state.trigger();
      
      Alert alert = new Alert(
          rule.getName() + "-" + System.currentTimeMillis(),
          rule.getType(),
          rule.getSeverity(),
          rule.getMessageTemplate(),
          "AsyncProcessMonitor",
          LocalDateTime.now(),
          null
      );
      
      log.warn("Alert triggered: {} - {}", rule.getName(), rule.getMessageTemplate());
      
      for (AlertListener listener : alertListeners) {
        try {
          listener.onAlert(alert);
        } catch (Exception e) {
          log.warn("Error notifying alert listener: {}", e.getMessage());
        }
      }
      
    } else if (!conditionMet && state.isActive()) {
      // Resolve alert
      state.resolve();
      
      Alert alert = new Alert(
          rule.getName() + "-resolved-" + System.currentTimeMillis(),
          rule.getType(),
          AlertSeverity.INFO,
          "Alert resolved: " + rule.getMessageTemplate(),
          "AsyncProcessMonitor",
          LocalDateTime.now(),
          null
      );
      
      log.info("Alert resolved: {}", rule.getName());
      
      for (AlertListener listener : alertListeners) {
        try {
          listener.onAlertResolved(alert);
        } catch (Exception e) {
          log.warn("Error notifying alert resolution listener: {}", e.getMessage());
        }
      }
    }
  }
  
  /**
   * Sets up default alert rules.
   */
  private void setupDefaultAlertRules() {
    // High failure rate alert
    addAlertRule(new AlertRule(
        "high-job-failure-rate",
        AlertType.HIGH_FAILURE_RATE,
        AlertSeverity.WARNING,
        snapshot -> snapshot.getJobMetrics().values().stream()
            .anyMatch(job -> job.getFailureRate() > 10 && job.getMessagesProcessed() > 10),
        "High failure rate detected in job processing"
    ));
    
    // High processing time alert
    addAlertRule(new AlertRule(
        "high-processing-time",
        AlertType.HIGH_PROCESSING_TIME,
        AlertSeverity.WARNING,
        snapshot -> snapshot.getJobMetrics().values().stream()
            .anyMatch(job -> job.getAverageProcessingTime() > 30000), // 30 seconds
        "High average processing time detected"
    ));
    
    // Consumer lag alert
    addAlertRule(new AlertRule(
        "high-consumer-lag",
        AlertType.CONSUMER_LAG,
        AlertSeverity.WARNING,
        snapshot -> snapshot.getConsumerMetrics().values().stream()
            .anyMatch(consumer -> consumer.getLag() > 1000),
        "High consumer lag detected"
    ));
    
    // Idle consumer alert
    addAlertRule(new AlertRule(
        "idle-consumer",
        AlertType.CONSUMER_IDLE,
        AlertSeverity.WARNING,
        snapshot -> snapshot.getConsumerMetrics().values().stream()
            .anyMatch(consumer -> consumer.isIdle(300000)), // 5 minutes
        "Consumer has been idle for extended period"
    ));
    
    // Kafka connection issues
    addAlertRule(new AlertRule(
        "kafka-connection-issues",
        AlertType.KAFKA_CONNECTION_ISSUE,
        AlertSeverity.CRITICAL,
        snapshot -> snapshot.getKafkaMetrics().getConnectionSuccessRate() < 90,
        "Kafka connection success rate is low"
    ));
    
    // High memory usage
    addAlertRule(new AlertRule(
        "high-memory-usage",
        AlertType.HIGH_MEMORY_USAGE,
        AlertSeverity.WARNING,
        snapshot -> snapshot.getSystemMetrics().getHeapUsagePercent() > 80,
        "High memory usage detected"
    ));
  }
  
  /**
   * Adds an alert rule.
   */
  public void addAlertRule(AlertRule rule) {
    alertRules.add(rule);
    log.info("Added alert rule: {}", rule.getName());
  }
  
  /**
   * Removes an alert rule.
   */
  public void removeAlertRule(String ruleName) {
    alertRules.removeIf(rule -> rule.getName().equals(ruleName));
    alertStates.remove(ruleName);
    log.info("Removed alert rule: {}", ruleName);
  }
  
  /**
   * Adds a metrics listener.
   */
  public void addMetricsListener(MetricsListener listener) {
    metricsListeners.add(listener);
  }
  
  /**
   * Removes a metrics listener.
   */
  public void removeMetricsListener(MetricsListener listener) {
    metricsListeners.remove(listener);
  }
  
  /**
   * Adds an alert listener.
   */
  public void addAlertListener(AlertListener listener) {
    alertListeners.add(listener);
  }
  
  /**
   * Removes an alert listener.
   */
  public void removeAlertListener(AlertListener listener) {
    alertListeners.remove(listener);
  }
  
  /**
   * Gets current metrics snapshot.
   */
  public MetricsSnapshot getCurrentMetrics() {
    return new MetricsSnapshot(
        LocalDateTime.now(),
        jobMetrics,
        consumerMetrics,
        kafkaMetrics,
        systemMetrics
    );
  }
  
  /**
   * Gets alert states.
   */
  public Map<String, AlertState> getAlertStates() {
    return new HashMap<>(alertStates);
  }
  
  /**
   * Gets monitoring status report.
   */
  public String getStatusReport() {
    StringBuilder report = new StringBuilder();
    report.append("=== Async Process Monitoring Status ===\n");
    
    // Job metrics summary
    report.append("Jobs (").append(jobMetrics.size()).append("):\n");
    jobMetrics.forEach((id, metrics) -> {
      report.append("  ").append(metrics.getJobName()).append(" (").append(id).append(")\n");
      report.append("    Processed: ").append(metrics.getMessagesProcessed()).append("\n");
      report.append("    Success Rate: ").append(String.format(NUMBER_FORMAT, metrics.getSuccessRate())).append("\n");
      report.append("    Avg Processing Time: ").append(metrics.getAverageProcessingTime()).append("ms\n");
    });
    
    // Consumer metrics summary
    report.append("Consumers (").append(consumerMetrics.size()).append("):\n");
    consumerMetrics.forEach((id, metrics) -> {
      report.append("  ").append(id).append(" (").append(metrics.getTopic()).append(")\n");
      report.append("    Messages: ").append(metrics.getMessagesConsumed()).append("\n");
      report.append("    Lag: ").append(metrics.getLag()).append("\n");
      report.append("    Reconnections: ").append(metrics.getReconnections()).append("\n");
    });
    
    // Kafka metrics
    report.append("Kafka:\n");
    report.append("  Connection Success Rate: ").append(String.format(NUMBER_FORMAT, kafkaMetrics.getConnectionSuccessRate())).append("\n");
    report.append("  Send Success Rate: ").append(String.format(NUMBER_FORMAT, kafkaMetrics.getSendSuccessRate())).append("\n");
    report.append("  Messages Sent: ").append(kafkaMetrics.getMessagesSent()).append("\n");
    
    // System metrics
    report.append("System:\n");
    report.append("  Heap Usage: ").append(String.format(NUMBER_FORMAT, systemMetrics.getHeapUsagePercent())).append("\n");
    report.append("  Active Threads: ").append(systemMetrics.getActiveThreads()).append("\n");
    
    // Active alerts
    long activeAlerts = alertStates.values().stream().mapToLong(state -> state.isActive() ? 1 : 0).sum();
    report.append("Active Alerts: ").append(activeAlerts).append("\n");
    
    return report.toString();
  }
}
