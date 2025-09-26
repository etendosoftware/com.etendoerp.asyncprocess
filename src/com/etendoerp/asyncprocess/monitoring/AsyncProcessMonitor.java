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
 * Central monitoring and alerting component for the asynchronous processing subsystem.
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>Collect runtime metrics for jobs, consumers, Kafka and the JVM.</li>
 *   <li>Produce periodic metric snapshots and broadcast them to registered listeners.</li>
 *   <li>Evaluate alerting rules against collected metrics and notify alert listeners
 *       when alerts are triggered or resolved.</li>
 *   <li>Expose programmatic API to record low-level events (job execution, consumer
 *       activity, Kafka interactions) that are aggregated into metrics.</li>
 * </ul>
 * </p>
 *
 * <p>Design notes:
 * <ul>
 *   <li>Internal state is stored in concurrent collections so that recording operations
 *       can be performed safely from multiple threads.</li>
 *   <li>Alert rules are represented by {@link AlertRule} instances that contain a
 *       {@link AlertRule.MetricsPredicate} to evaluate conditions against a
 *       {@link MetricsSnapshot}.</li>
 *   <li>Alerts are managed using {@link AlertState} to avoid repeated notifications
 *       for the same ongoing condition; state transitions trigger notifications.</li>
 * </ul>
 * </p>
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
  
  /**
   * Listener interface for receiving periodic metric snapshots.
   * Implementers will receive {@link MetricsSnapshot} instances when metrics are
   * broadcast by the monitor.
   */
  public interface MetricsListener {
    /**
     * Called when a new metrics snapshot is available.
     *
     * @param snapshot immutable snapshot that contains metrics at a point in time
     */
    void onMetricsUpdate(MetricsSnapshot snapshot);
  }
  
  /**
   * Listener interface for alert lifecycle notifications. Implementers will be
   * informed when an alert is triggered and when it is resolved.
   */
  public interface AlertListener {
    /**
     * Called when an alert is triggered.
     *
     * @param alert alert object with details about the triggered condition
     */
    void onAlert(Alert alert);

    /**
     * Called when an active alert is resolved.
     *
     * @param alert alert object describing the resolved alert
     */
    void onAlertResolved(Alert alert);
  }
  
  /**
   * Container for per-job aggregated metrics.
   * Instances are updated by the monitor when job execution events are recorded.
   */
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
    
    /**
     * Create job metrics holder.
     *
     * @param jobId stable identifier for the job
     * @param jobName human-readable job name
     */
    public JobMetrics(String jobId, String jobName) {
      this.jobId = jobId;
      this.jobName = jobName;
    }
    
    /**
     * Record a processed message for this job.
     *
     * @param processingTime elapsed time in milliseconds to process the message
     * @param success whether the processing succeeded
     * @param isRetry whether the message was retried
     */
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
    
    /**
     * Returns the success rate in percentage (0-100). If no messages have been processed
     * a default of 100.0 is returned (interpreted as no failures recorded yet).
     */
    public double getSuccessRate() {
      long total = messagesProcessed.sum();
      return total > 0 ? (double) messagesSucceeded.sum() / total * 100 : 100.0;
    }
    
    /**
     * Returns the failure rate in percentage (0-100).
     */
    public double getFailureRate() {
      long total = messagesProcessed.sum();
      return total > 0 ? (double) messagesFailed.sum() / total * 100 : 0.0;
    }
    
    /**
     * Returns the retry rate in percentage (0-100).
     */
    public double getRetryRate() {
      long total = messagesProcessed.sum();
      return total > 0 ? (double) messagesRetried.sum() / total * 100 : 0.0;
    }
  }
  
  /**
   * Metrics related to a single consumer instance.
   */
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
    
    /**
     * Mark that the consumer processed one message.
     */
    public void recordMessage() {
      messagesConsumed.increment();
      lastActivity.set(System.currentTimeMillis());
    }
    
    /**
     * Increment the count of connection lost events for this consumer.
     */
    public void recordConnectionLost() {
      connectionLost.increment();
    }
    
    /**
     * Record a reconnection event and update last activity time.
     */
    public void recordReconnection() {
      reconnections.increment();
      lastActivity.set(System.currentTimeMillis());
    }
    
    /**
     * Update observed lag for this consumer (topic partitions total lag).
     *
     * @param lag total lag value to store
     */
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
    
    /**
     * Returns true if the consumer has been idle longer than the provided threshold.
     *
     * @param idleThresholdMs threshold in milliseconds
     */
    public boolean isIdle(long idleThresholdMs) {
      return System.currentTimeMillis() - lastActivity.get() > idleThresholdMs;
    }
  }
  
  /**
   * Aggregated Kafka client metrics.
   */
  public static class KafkaMetrics {
    private final LongAdder connectionAttempts = new LongAdder();
    private final LongAdder connectionFailures = new LongAdder();
    private final LongAdder messagesSent = new LongAdder();
    private final LongAdder sendFailures = new LongAdder();
    private final AtomicLong lastConnectionTime = new AtomicLong(0);
    private final AtomicLong avgSendTime = new AtomicLong(0);
    
    /**
     * Record a connection attempt to Kafka.
     *
     * @param success true if the attempt succeeded
     */
    public void recordConnectionAttempt(boolean success) {
      connectionAttempts.increment();
      if (success) {
        lastConnectionTime.set(System.currentTimeMillis());
      } else {
        connectionFailures.increment();
      }
    }
    
    /**
     * Record a message send operation and update average send time.
     *
     * @param sendTime elapsed milliseconds for send operation
     * @param success whether the send succeeded
     */
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
    
    /**
     * Returns the percentage of successful connection attempts (0-100).
     */
    public double getConnectionSuccessRate() {
      long total = connectionAttempts.sum();
      return total > 0 ? (double) (total - connectionFailures.sum()) / total * 100 : 100.0;
    }
    
    /**
     * Returns the percentage of successful sends (0-100).
     */
    public double getSendSuccessRate() {
      long total = messagesSent.sum();
      return total > 0 ? (double) (total - sendFailures.sum()) / total * 100 : 100.0;
    }
  }
  
  /**
   * System-level metrics (JVM memory, threads, CPU usage).
   */
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
    
    /**
     * Returns heap usage percentage (0-100). Returns 0 if max heap size is not available.
     */
    public double getHeapUsagePercent() {
      long max = heapMaxBytes.get();
      return max > 0 ? (double) heapUsedBytes.get() / max * 100 : 0.0;
    }
  }
  
  /**
   * Immutable snapshot of metrics at a specific point in time. This object is sent to
   * metrics listeners and used to evaluate alert rules. The snapshot contains copies
   * of the current metric maps to avoid concurrent modification issues.
   */
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
  
  /**
   * Severity levels for alerts.
   */
  public enum AlertSeverity {
    INFO, WARNING, CRITICAL
  }
  
  /**
   * Types of alerts that the monitor can emit. These represent common problem categories
   * within the async processing subsystem.
   */
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
  
  /**
   * Alert model carrying metadata about a detected condition.
   */
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
  
  /**
   * Definition of an alert rule. Each rule contains a predicate that evaluates a
   * {@link MetricsSnapshot} and a message template used for notifications.
   */
  public static class AlertRule {
    private final String name;
    private final AlertType type;
    private final AlertSeverity severity;
    private final MetricsPredicate condition;
    private final String messageTemplate;
    
    /**
     * Functional predicate used by rules to evaluate a snapshot.
     */
    @FunctionalInterface
    public interface MetricsPredicate {
      /**
       * Evaluate the predicate against the provided snapshot.
       *
       * @param snapshot metrics snapshot to test
       * @return true when the condition is met
       */
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
  
  /**
   * Internal state holder for a rule that tracks whether it is currently active
   * and timestamps when it was triggered or resolved.
   */
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
    
    /**
     * Mark the alert as triggered. If it was inactive the trigger time is recorded.
     */
    public void trigger() {
      if (!isActive) {
        isActive = true;
        lastTriggered = LocalDateTime.now();
      }
      triggerCount++;
    }
    
    /**
     * Resolve the alert and record the resolution timestamp.
     */
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
  
  /**
   * Create a monitor using default collection and alert intervals.
   */
  public AsyncProcessMonitor() {
    this(DEFAULT_METRICS_COLLECTION_INTERVAL_MS, DEFAULT_ALERT_CHECK_INTERVAL_MS);
  }
  
  /**
   * Create a monitor with custom intervals. Useful for testing or tuning in different environments.
   *
   * @param metricsCollectionIntervalMs interval in milliseconds for metrics collection
   * @param alertCheckIntervalMs interval in milliseconds for evaluating alert rules
   */
  public AsyncProcessMonitor(long metricsCollectionIntervalMs, long alertCheckIntervalMs) {
    this.metricsCollectionIntervalMs = metricsCollectionIntervalMs;
    this.alertCheckIntervalMs = alertCheckIntervalMs;
    this.monitoringScheduler = Executors.newScheduledThreadPool(3);
    
    setupDefaultAlertRules();
  }
  
  /**
   * Start monitoring and scheduling of internal tasks (metrics collection, broadcasting and alert checks).
   * This method is idempotent in the sense that scheduling additional invocations will create more tasks
   * if called multiple times; callers should ensure start/stop lifecycle is managed.
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
   * Stop the monitor and shutdown internal scheduler. Blocks briefly while awaiting termination.
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
   * Record an execution event for a job. This updates per-job aggregated metrics.
   *
   * @param jobId identifier of the job
   * @param jobName human readable name of the job
   * @param processingTime elapsed processing time in milliseconds
   * @param success true if execution succeeded
   * @param isRetry true if this execution was a retry attempt
   */
  public void recordJobExecution(String jobId, String jobName, long processingTime, 
                                  boolean success, boolean isRetry) {
    JobMetrics metrics = jobMetrics.computeIfAbsent(jobId, k -> new JobMetrics(jobId, jobName));
    metrics.recordProcessedMessage(processingTime, success, isRetry);
  }
  
  /**
   * Record that a consumer processed a message. Creates metrics for the consumer if not present.
   *
   * @param consumerId identifier for the consumer instance
   * @param groupId consumer group id
   * @param topic topic the consumer reads from
   */
  public void recordConsumerActivity(String consumerId, String groupId, String topic) {
    ConsumerMetrics metrics = consumerMetrics.computeIfAbsent(consumerId, 
        k -> new ConsumerMetrics(consumerId, groupId, topic));
    metrics.recordMessage();
  }
  
  /**
   * Record that a consumer experienced a lost connection event.
   *
   * @param consumerId consumer identifier
   */
  public void recordConsumerConnectionLost(String consumerId) {
    ConsumerMetrics metrics = consumerMetrics.get(consumerId);
    if (metrics != null) {
      metrics.recordConnectionLost();
    }
  }
  
  /**
   * Record that a consumer reconnected successfully.
   *
   * @param consumerId consumer identifier
   */
  public void recordConsumerReconnection(String consumerId) {
    ConsumerMetrics metrics = consumerMetrics.get(consumerId);
    if (metrics != null) {
      metrics.recordReconnection();
    }
  }
  
  /**
   * Update the observed lag value for a consumer.
   *
   * @param consumerId consumer identifier
   * @param lag total lag measured for this consumer
   */
  public void updateConsumerLag(String consumerId, long lag) {
    ConsumerMetrics metrics = consumerMetrics.get(consumerId);
    if (metrics != null) {
      metrics.updateLag(lag);
    }
  }
  
  /**
   * Record an attempt to connect to Kafka. This influences the Kafka metrics and
   * may be used by alert rules detecting Kafka availability issues.
   *
   * @param success true if the connection attempt succeeded
   */
  public void recordKafkaConnection(boolean success) {
    kafkaMetrics.recordConnectionAttempt(success);
  }
  
  /**
   * Record a message send operation to Kafka.
   *
   * @param sendTime elapsed time in milliseconds for the send operation
   * @param success true if the send succeeded
   */
  public void recordKafkaMessageSent(long sendTime, boolean success) {
    kafkaMetrics.recordMessageSent(sendTime, success);
  }
  
  /**
   * Collect basic system metrics (memory, threads and CPU placeholder).
   * This method is invoked periodically by the monitor's scheduler.
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
   * Build a metrics snapshot and notify registered metrics listeners.
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
   * Evaluate all configured alert rules against a fresh metrics snapshot and
   * trigger or resolve alerts accordingly.
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
   * Evaluate a single alert rule and notify listeners when the rule transitions
   * between active and resolved states.
   *
   * @param rule alert rule to evaluate
   * @param snapshot metrics snapshot used for evaluation
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
   * Initialize default alert rules that cover common failure and performance scenarios.
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
   * Add a custom alert rule to the monitor.
   *
   * @param rule rule to add; rules are evaluated during alert checks in insertion order
   */
  public void addAlertRule(AlertRule rule) {
    alertRules.add(rule);
    log.info("Added alert rule: {}", rule.getName());
  }
  
  /**
   * Remove an alert rule by name. This also clears any associated alert state.
   *
   * @param ruleName identifier of the rule to remove
   */
  public void removeAlertRule(String ruleName) {
    alertRules.removeIf(rule -> rule.getName().equals(ruleName));
    alertStates.remove(ruleName);
    log.info("Removed alert rule: {}", ruleName);
  }
  
  /**
   * Register a metrics listener that will receive periodic {@link MetricsSnapshot}s.
   *
   * @param listener implementation to register
   */
  public void addMetricsListener(MetricsListener listener) {
    metricsListeners.add(listener);
  }
  
  /**
   * Remove a previously registered metrics listener.
   *
   * @param listener listener to remove
   */
  public void removeMetricsListener(MetricsListener listener) {
    metricsListeners.remove(listener);
  }
  
  /**
   * Register an alert listener that will be notified when alerts trigger or resolve.
   *
   * @param listener implementation to register
   */
  public void addAlertListener(AlertListener listener) {
    alertListeners.add(listener);
  }
  
  /**
   * Remove a previously registered alert listener.
   *
   * @param listener listener to remove
   */
  public void removeAlertListener(AlertListener listener) {
    alertListeners.remove(listener);
  }
  
  /**
   * Returns a snapshot of current metrics suitable for diagnostics or external reporting.
   *
   * @return current {@link MetricsSnapshot}
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
   * Returns a copy of the current alert states keyed by rule name. The returned map is
   * a shallow copy and safe to inspect by callers.
   *
   * @return map from rule name to {@link AlertState}
   */
  public Map<String, AlertState> getAlertStates() {
    return new HashMap<>(alertStates);
  }
  
  /**
   * Build a textual, human readable monitoring status report. This is intended for
   * quick diagnostics (logs, admin pages) and not for machine parsing.
   *
   * @return multi-line status summary string
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
