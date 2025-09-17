package com.etendoerp.asyncprocess.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.criterion.Restrictions;
import org.openbravo.base.session.OBPropertiesProvider;
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.service.OBDal;

import com.etendoerp.asyncprocess.health.KafkaHealthChecker;
import com.etendoerp.asyncprocess.model.AsyncProcessState;
import com.etendoerp.asyncprocess.recovery.ConsumerRecoveryManager;
import com.etendoerp.asyncprocess.retry.RetryPolicy;
import com.etendoerp.asyncprocess.retry.SimpleRetryPolicy;
import com.etendoerp.asyncprocess.startup.AsyncProcessStartup;
import com.smf.jobs.Action;
import com.smf.jobs.model.Job;
import com.smf.jobs.model.JobLine;

import reactor.core.Disposable;

/**
 * Manager for dynamic reconfiguration of async process jobs and topics.
 * Allows reloading configuration without restarting Tomcat.
 */
public class AsyncProcessReconfigurationManager {
  private static final Logger log = LogManager.getLogger();
  private static final long DEFAULT_CONFIG_CHECK_INTERVAL_MS = 60000; // 1 minute
  
  private final AsyncProcessStartup startupManager;
  private final KafkaHealthChecker healthChecker;
  private final ConsumerRecoveryManager recoveryManager;
  private final ScheduledExecutorService configScheduler;
  
  // Track active configurations
  private final Map<String, JobConfiguration> activeJobs = new ConcurrentHashMap<>();
  private final Map<String, Disposable> activeSubscriptions = new ConcurrentHashMap<>();
  private final AtomicBoolean isMonitoringEnabled = new AtomicBoolean(true);
  private final AtomicBoolean isReconfigurationEnabled = new AtomicBoolean(true);
  
  // Configuration state tracking
  private long lastConfigurationHash = 0;
  private long configCheckIntervalMs;
  
  public static class JobConfiguration {
    private final Job job;
    private final List<JobLineConfiguration> jobLines;
    private final long configurationHash;
    
    public JobConfiguration(Job job, List<JobLineConfiguration> jobLines, long configurationHash) {
      this.job = job;
      this.jobLines = jobLines;
      this.configurationHash = configurationHash;
    }
    
    public Job getJob() { return job; }
    public List<JobLineConfiguration> getJobLines() { return jobLines; }
    public long getConfigurationHash() { return configurationHash; }
  }
  
  public static class JobLineConfiguration {
    private final JobLine jobLine;
    private final AsyncProcessConfig config;
    private final String topic;
    private final String nextTopic;
    private final String errorTopic;
    private final String groupId;
    private final AsyncProcessState targetStatus;
    private final Supplier<Action> actionFactory;
    private final RetryPolicy retryPolicy;
    
    public JobLineConfiguration(JobLine jobLine, AsyncProcessConfig config, String topic, 
                               String nextTopic, String errorTopic, String groupId,
                               AsyncProcessState targetStatus, Supplier<Action> actionFactory,
                               RetryPolicy retryPolicy) {
      this.jobLine = jobLine;
      this.config = config;
      this.topic = topic;
      this.nextTopic = nextTopic;
      this.errorTopic = errorTopic;
      this.groupId = groupId;
      this.targetStatus = targetStatus;
      this.actionFactory = actionFactory;
      this.retryPolicy = retryPolicy;
    }
    
    // Getters
    public JobLine getJobLine() { return jobLine; }
    public AsyncProcessConfig getConfig() { return config; }
    public String getTopic() { return topic; }
    public String getNextTopic() { return nextTopic; }
    public String getErrorTopic() { return errorTopic; }
    public String getGroupId() { return groupId; }
    public AsyncProcessState getTargetStatus() { return targetStatus; }
    public Supplier<Action> getActionFactory() { return actionFactory; }
    public RetryPolicy getRetryPolicy() { return retryPolicy; }
  }
  
  // Listeners for configuration changes
  public interface ConfigurationChangeListener {
    void onJobAdded(JobConfiguration jobConfig);
    void onJobRemoved(String jobId);
    void onJobModified(JobConfiguration oldConfig, JobConfiguration newConfig);
  }
  
  private final List<ConfigurationChangeListener> changeListeners = new ArrayList<>();
  
  public AsyncProcessReconfigurationManager(AsyncProcessStartup startupManager, 
                                          KafkaHealthChecker healthChecker,
                                          ConsumerRecoveryManager recoveryManager) {
    this(startupManager, healthChecker, recoveryManager, DEFAULT_CONFIG_CHECK_INTERVAL_MS);
  }
  
  public AsyncProcessReconfigurationManager(AsyncProcessStartup startupManager, 
                                          KafkaHealthChecker healthChecker,
                                          ConsumerRecoveryManager recoveryManager,
                                          long configCheckIntervalMs) {
    this.startupManager = startupManager;
    this.healthChecker = healthChecker;
    this.recoveryManager = recoveryManager;
    this.configCheckIntervalMs = configCheckIntervalMs;
    this.configScheduler = Executors.newScheduledThreadPool(2);
  }
  
  /**
   * Starts the configuration monitoring.
   */
  public void startMonitoring() {
    if (!isMonitoringEnabled.get()) {
      log.info("Configuration monitoring is disabled");
      return;
    }
    
    log.info("Starting async process configuration monitoring with interval {} ms", configCheckIntervalMs);
    
    // Initial configuration load
    loadCurrentConfiguration();
    
    // Schedule periodic configuration checks
    configScheduler.scheduleWithFixedDelay(
        this::checkForConfigurationChanges,
        configCheckIntervalMs,
        configCheckIntervalMs,
        TimeUnit.MILLISECONDS
    );
  }
  
  /**
   * Stops the configuration monitoring.
   */
  public void stopMonitoring() {
    log.info("Stopping configuration monitoring");
    isMonitoringEnabled.set(false);
    configScheduler.shutdown();
    try {
      if (!configScheduler.awaitTermination(10, TimeUnit.SECONDS)) {
        configScheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      configScheduler.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
  
  /**
   * Loads the current configuration from the database.
   */
  private void loadCurrentConfiguration() {
    try {
      OBContext.setOBContext("100", "0", "0", "0");
      
      var critJob = OBDal.getInstance().createCriteria(Job.class);
      critJob.add(Restrictions.eq(Job.PROPERTY_ETAPISASYNC, true));
      List<Job> jobs = critJob.list();
      
      Map<String, JobConfiguration> newJobConfigs = new HashMap<>();
      
      for (Job job : jobs) {
        JobConfiguration jobConfig = createJobConfiguration(job);
        newJobConfigs.put(job.getId(), jobConfig);
      }
      
      // Calculate configuration hash
      long newHash = calculateConfigurationHash(newJobConfigs);
      
      synchronized (activeJobs) {
        activeJobs.clear();
        activeJobs.putAll(newJobConfigs);
        lastConfigurationHash = newHash;
      }
      
      log.info("Loaded configuration for {} async jobs", activeJobs.size());
      
    } catch (Exception e) {
      log.error("Error loading current configuration", e);
    }
  }
  
  /**
   * Creates job configuration from a job entity.
   */
  private JobConfiguration createJobConfiguration(Job job) {
    try {
      var jobLines = job.getJOBSJobLineList();
      jobLines.sort((o1, o2) -> (int) (o1.getLineNo() - o2.getLineNo()));
      
      List<JobLineConfiguration> jobLineConfigs = new ArrayList<>();
      
      for (JobLine jobLine : jobLines) {
        JobLineConfiguration lineConfig = createJobLineConfiguration(job, jobLine, jobLines);
        jobLineConfigs.add(lineConfig);
      }
      
      long configHash = calculateJobConfigurationHash(job, jobLineConfigs);
      return new JobConfiguration(job, jobLineConfigs, configHash);
      
    } catch (Exception e) {
      log.error("Error creating job configuration for job {}", job.getId(), e);
      throw new RuntimeException("Failed to create job configuration", e);
    }
  }
  
  /**
   * Creates job line configuration.
   */
  private JobLineConfiguration createJobLineConfiguration(Job job, JobLine jobLine, List<JobLine> allJobLines) {
    try {
      // Use methods from AsyncProcessStartup to maintain consistency
      AsyncProcessConfig config = getJobLineConfig(jobLine);
      String topic = calculateCurrentTopic(jobLine, allJobLines);
      String nextTopic = calculateNextTopic(jobLine, allJobLines);
      String errorTopic = calculateErrorTopic(job);
      String groupId = getGroupId(jobLine);
      AsyncProcessState targetStatus = convertState(jobLine.getEtapTargetstatus());
      Supplier<Action> actionFactory = createActionFactory(jobLine.getAction());
      RetryPolicy retryPolicy = new SimpleRetryPolicy(config.getMaxRetries(), config.getRetryDelayMs());
      
      return new JobLineConfiguration(jobLine, config, topic, nextTopic, errorTopic,
                                    groupId, targetStatus, actionFactory, retryPolicy);
      
    } catch (Exception e) {
      log.error("Error creating job line configuration for job line {}", jobLine.getId(), e);
      throw new RuntimeException("Failed to create job line configuration", e);
    }
  }
  
  /**
   * Checks for configuration changes and applies them if found.
   */
  private void checkForConfigurationChanges() {
    if (!isMonitoringEnabled.get() || !isReconfigurationEnabled.get()) {
      return;
    }
    
    try {
      // Check if Kafka is healthy before attempting reconfiguration
      if (!healthChecker.isKafkaHealthy()) {
        log.debug("Kafka is not healthy, skipping configuration check");
        return;
      }
      
      Map<String, JobConfiguration> currentConfigs = loadJobConfigurations();
      long newHash = calculateConfigurationHash(currentConfigs);
      
      if (newHash != lastConfigurationHash) {
        log.info("Configuration changes detected. Applying changes...");
        applyConfigurationChanges(currentConfigs);
        lastConfigurationHash = newHash;
      }
      
    } catch (Exception e) {
      log.error("Error checking for configuration changes", e);
    }
  }
  
  /**
   * Loads job configurations from database.
   */
  private Map<String, JobConfiguration> loadJobConfigurations() {
    try {
      OBContext.setOBContext("100", "0", "0", "0");
      
      var critJob = OBDal.getInstance().createCriteria(Job.class);
      critJob.add(Restrictions.eq(Job.PROPERTY_ETAPISASYNC, true));
      List<Job> jobs = critJob.list();
      
      Map<String, JobConfiguration> jobConfigs = new HashMap<>();
      
      for (Job job : jobs) {
        if (isAsyncJobsEnabled()) {
          JobConfiguration jobConfig = createJobConfiguration(job);
          jobConfigs.put(job.getId(), jobConfig);
        }
      }
      
      return jobConfigs;
      
    } catch (Exception e) {
      log.error("Error loading job configurations", e);
      return new HashMap<>();
    }
  }
  
  /**
   * Applies configuration changes.
   */
  private void applyConfigurationChanges(Map<String, JobConfiguration> newConfigs) {
    CompletableFuture.runAsync(() -> {
      try {
        synchronized (activeJobs) {
          // Find added, removed, and modified jobs
          List<JobConfiguration> addedJobs = new ArrayList<>();
          List<String> removedJobIds = new ArrayList<>();
          List<JobConfiguration[]> modifiedJobs = new ArrayList<>(); // [old, new]
          
          // Check for added and modified jobs
          for (Map.Entry<String, JobConfiguration> entry : newConfigs.entrySet()) {
            String jobId = entry.getKey();
            JobConfiguration newConfig = entry.getValue();
            JobConfiguration oldConfig = activeJobs.get(jobId);
            
            if (oldConfig == null) {
              addedJobs.add(newConfig);
            } else if (oldConfig.getConfigurationHash() != newConfig.getConfigurationHash()) {
              modifiedJobs.add(new JobConfiguration[]{oldConfig, newConfig});
            }
          }
          
          // Check for removed jobs
          for (String jobId : activeJobs.keySet()) {
            if (!newConfigs.containsKey(jobId)) {
              removedJobIds.add(jobId);
            }
          }
          
          // Apply changes
          log.info("Configuration changes: {} added, {} modified, {} removed", 
                   addedJobs.size(), modifiedJobs.size(), removedJobIds.size());
          
          // Remove old jobs
          for (String jobId : removedJobIds) {
            removeJob(jobId);
            notifyConfigurationChange(listener -> listener.onJobRemoved(jobId));
          }
          
          // Modify existing jobs
          for (JobConfiguration[] configs : modifiedJobs) {
            modifyJob(configs[0], configs[1]);
            notifyConfigurationChange(listener -> listener.onJobModified(configs[0], configs[1]));
          }
          
          // Add new jobs
          for (JobConfiguration jobConfig : addedJobs) {
            addJob(jobConfig);
            notifyConfigurationChange(listener -> listener.onJobAdded(jobConfig));
          }
          
          // Update active jobs map
          activeJobs.clear();
          activeJobs.putAll(newConfigs);
        }
        
      } catch (Exception e) {
        log.error("Error applying configuration changes", e);
      }
    }, configScheduler);
  }
  
  /**
   * Adds a new job configuration.
   */
  private void addJob(JobConfiguration jobConfig) {
    log.info("Adding new job: {} ({})", jobConfig.getJob().getName(), jobConfig.getJob().getId());
    
    try {
      // Create consumers for this job using the startup manager logic
      createConsumersForJob(jobConfig);
      
    } catch (Exception e) {
      log.error("Error adding job {}: {}", jobConfig.getJob().getId(), e.getMessage(), e);
    }
  }
  
  /**
   * Removes a job configuration.
   */
  private void removeJob(String jobId) {
    log.info("Removing job: {}", jobId);
    
    try {
      // Stop all consumers for this job
      activeSubscriptions.entrySet().removeIf(entry -> {
        if (entry.getKey().startsWith(jobId + "-")) {
          try {
            entry.getValue().dispose();
            return true;
          } catch (Exception e) {
            log.warn("Error disposing subscription {}: {}", entry.getKey(), e.getMessage());
            return true;
          }
        }
        return false;
      });
      
      // Unregister consumers from recovery manager
      recoveryManager.getRecoveryStatus().forEach((consumerId, status) -> {
        if (consumerId.startsWith(jobId + "-")) {
          recoveryManager.unregisterConsumer(consumerId);
        }
      });
      
    } catch (Exception e) {
      log.error("Error removing job {}: {}", jobId, e.getMessage(), e);
    }
  }
  
  /**
   * Modifies an existing job configuration.
   */
  private void modifyJob(JobConfiguration oldConfig, JobConfiguration newConfig) {
    log.info("Modifying job: {} ({})", newConfig.getJob().getName(), newConfig.getJob().getId());
    
    try {
      // Remove old configuration
      removeJob(oldConfig.getJob().getId());
      
      // Add new configuration  
      addJob(newConfig);
      
    } catch (Exception e) {
      log.error("Error modifying job {}: {}", newConfig.getJob().getId(), e.getMessage(), e);
    }
  }
  
  /**
   * Creates consumers for a job (simplified version of startup logic).
   */
  private void createConsumersForJob(JobConfiguration jobConfig) {
    // This is a simplified version - in a real implementation, you'd want to
    // integrate more closely with AsyncProcessStartup or extract the consumer
    // creation logic to a shared utility
    log.info("Creating consumers for job {} - this is a placeholder implementation", 
             jobConfig.getJob().getId());
    
    // TODO: Implement actual consumer creation logic
    // This would involve:
    // 1. Creating topics if they don't exist
    // 2. Creating receivers
    // 3. Subscribing to topics
    // 4. Registering with recovery manager
  }
  
  // Helper methods (copied/adapted from AsyncProcessStartup)
  
  private AsyncProcessConfig getJobLineConfig(JobLine jobLine) {
    // Implementation from AsyncProcessStartup.getJobLineConfig()
    AsyncProcessConfig config = new AsyncProcessConfig();
    // ... (copy implementation from original class)
    return config;
  }
  
  private String calculateCurrentTopic(JobLine jobLine, List<JobLine> jobLines) {
    // Implementation from AsyncProcessStartup.calculateCurrentTopic()
    return ""; // Placeholder
  }
  
  private String calculateNextTopic(JobLine jobLine, List<JobLine> jobLines) {
    // Implementation from AsyncProcessStartup.calculateNextTopic()
    return ""; // Placeholder
  }
  
  private String calculateErrorTopic(Job job) {
    // Implementation from AsyncProcessStartup.calculateErrorTopic()
    return ""; // Placeholder
  }
  
  private String getGroupId(JobLine jobLine) {
    // Implementation from AsyncProcessStartup.getGroupId()
    return ""; // Placeholder
  }
  
  private AsyncProcessState convertState(String status) {
    // Implementation from AsyncProcessStartup.convertState()
    return AsyncProcessState.STARTED; // Placeholder
  }
  
  private Supplier<Action> createActionFactory(org.openbravo.client.application.Process action) {
    // Implementation from AsyncProcessStartup.createActionFactory()
    return null; // Placeholder
  }
  
  private boolean isAsyncJobsEnabled() {
    var obProps = OBPropertiesProvider.getInstance().getOpenbravoProperties();
    return obProps.containsKey("kafka.enable") && 
           StringUtils.equalsIgnoreCase(obProps.getProperty("kafka.enable", "false"), "true");
  }
  
  /**
   * Calculates hash for configuration change detection.
   */
  private long calculateConfigurationHash(Map<String, JobConfiguration> configs) {
    return configs.values().stream()
        .mapToLong(JobConfiguration::getConfigurationHash)
        .sum();
  }
  
  /**
   * Calculates hash for a job configuration.
   */
  private long calculateJobConfigurationHash(Job job, List<JobLineConfiguration> jobLines) {
    // Simple hash based on job and job line properties
    StringBuilder sb = new StringBuilder();
    sb.append(job.getId())
      .append(job.getName())
      .append(job.getEtapInitialTopic())
      .append(job.getEtapErrortopic())
      .append(job.isEtapIsregularexp())
      .append(job.isEtapConsumerPerPartition());
    
    for (JobLineConfiguration line : jobLines) {
      sb.append(line.getJobLine().getId())
        .append(line.getJobLine().getLineNo())
        .append(line.getJobLine().getEtapTargettopic())
        .append(line.getJobLine().getEtapTargetstatus())
        .append(line.getJobLine().isEtapConsumerPerPartition());
    }
    
    return sb.toString().hashCode();
  }
  
  /**
   * Adds a configuration change listener.
   */
  public void addConfigurationChangeListener(ConfigurationChangeListener listener) {
    changeListeners.add(listener);
  }
  
  /**
   * Removes a configuration change listener.
   */
  public void removeConfigurationChangeListener(ConfigurationChangeListener listener) {
    changeListeners.remove(listener);
  }
  
  /**
   * Notifies all listeners about configuration changes.
   */
  private void notifyConfigurationChange(java.util.function.Consumer<ConfigurationChangeListener> action) {
    for (ConfigurationChangeListener listener : changeListeners) {
      try {
        action.accept(listener);
      } catch (Exception e) {
        log.error("Error notifying configuration change listener", e);
      }
    }
  }
  
  /**
   * Forces a configuration reload.
   */
  public void forceConfigurationReload() {
    log.info("Forcing configuration reload");
    checkForConfigurationChanges();
  }
  
  /**
   * Enables or disables automatic reconfiguration.
   */
  public void setReconfigurationEnabled(boolean enabled) {
    boolean wasEnabled = isReconfigurationEnabled.getAndSet(enabled);
    if (enabled && !wasEnabled) {
      log.info("Automatic reconfiguration enabled");
      // Trigger immediate check
      configScheduler.execute(this::checkForConfigurationChanges);
    } else if (!enabled && wasEnabled) {
      log.info("Automatic reconfiguration disabled");
    }
  }
  
  /**
   * Gets current configuration status.
   */
  public Map<String, Object> getConfigurationStatus() {
    Map<String, Object> status = new HashMap<>();
    status.put("monitoringEnabled", isMonitoringEnabled.get());
    status.put("reconfigurationEnabled", isReconfigurationEnabled.get());
    status.put("configCheckIntervalMs", configCheckIntervalMs);
    status.put("lastConfigurationHash", lastConfigurationHash);
    status.put("activeJobsCount", activeJobs.size());
    status.put("activeSubscriptionsCount", activeSubscriptions.size());
    
    List<Map<String, Object>> jobs = new ArrayList<>();
    activeJobs.forEach((id, config) -> {
      Map<String, Object> jobInfo = new HashMap<>();
      jobInfo.put("id", id);
      jobInfo.put("name", config.getJob().getName());
      jobInfo.put("jobLinesCount", config.getJobLines().size());
      jobInfo.put("configurationHash", config.getConfigurationHash());
      jobs.add(jobInfo);
    });
    status.put("activeJobs", jobs);
    
    return status;
  }
}