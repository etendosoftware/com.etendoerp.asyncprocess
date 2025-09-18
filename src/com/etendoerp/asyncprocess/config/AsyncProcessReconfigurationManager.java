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

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.criterion.Restrictions;
import org.openbravo.base.session.OBPropertiesProvider;
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.service.OBDal;

import com.etendoerp.asyncprocess.health.KafkaHealthChecker;
import com.etendoerp.asyncprocess.recovery.ConsumerRecoveryManager;
import com.smf.jobs.model.Job;

/**
 * Manager for dynamic reconfiguration of async process jobs and topics.
 * Allows reloading configuration without restarting Tomcat.
 * This is a simplified version that focuses on configuration monitoring.
 */
public class AsyncProcessReconfigurationManager {
  private static final Logger log = LogManager.getLogger();
  private static final long DEFAULT_CONFIG_CHECK_INTERVAL_MS = 60000; // 1 minute

  private final KafkaHealthChecker healthChecker;
  private final ConsumerRecoveryManager recoveryManager;
  private final ScheduledExecutorService configScheduler;
  
  // Track active configurations
  private final Map<String, Job> activeJobs = new ConcurrentHashMap<>();
  private final AtomicBoolean isMonitoringEnabled = new AtomicBoolean(true);
  private final AtomicBoolean isReconfigurationEnabled = new AtomicBoolean(true);
  
  // Configuration state tracking
  private long lastConfigurationHash = 0;
  private final long configCheckIntervalMs;

  // Configuration change listeners
  public interface ConfigurationChangeListener {
    void onJobAdded(Job jobConfig);
    void onJobRemoved(String jobId);
    void onJobModified(Job oldConfig, Job newConfig);
  }

  private final List<ConfigurationChangeListener> changeListeners = new ArrayList<>();

  public AsyncProcessReconfigurationManager(KafkaHealthChecker healthChecker,
                                          ConsumerRecoveryManager recoveryManager) {
    this(healthChecker, recoveryManager, DEFAULT_CONFIG_CHECK_INTERVAL_MS);
  }
  
  public AsyncProcessReconfigurationManager(KafkaHealthChecker healthChecker,
                                          ConsumerRecoveryManager recoveryManager,
                                          long configCheckIntervalMs) {
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
      
      Map<String, Job> newJobConfigs = new HashMap<>();

      for (Job job : jobs) {
        newJobConfigs.put(job.getId(), job);
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
      
      Map<String, Job> currentConfigs = loadJobConfigurations();
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
  private Map<String, Job> loadJobConfigurations() {
    try {
      OBContext.setOBContext("100", "0", "0", "0");
      
      var critJob = OBDal.getInstance().createCriteria(Job.class);
      critJob.add(Restrictions.eq(Job.PROPERTY_ETAPISASYNC, true));
      List<Job> jobs = critJob.list();
      
      Map<String, Job> jobConfigs = new HashMap<>();

      for (Job job : jobs) {
        if (isAsyncJobsEnabled()) {
          jobConfigs.put(job.getId(), job);
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
  private void applyConfigurationChanges(Map<String, Job> newConfigs) {
    CompletableFuture.runAsync(() -> {
      try {
        synchronized (activeJobs) {
          // Find added, removed, and modified jobs
          List<Job> addedJobs = new ArrayList<>();
          List<String> removedJobIds = new ArrayList<>();
          List<Job[]> modifiedJobs = new ArrayList<>(); // [old, new]

          // Check for added and modified jobs
          for (Map.Entry<String, Job> entry : newConfigs.entrySet()) {
            String jobId = entry.getKey();
            Job newConfig = entry.getValue();
            Job oldConfig = activeJobs.get(jobId);

            if (oldConfig == null) {
              addedJobs.add(newConfig);
            } else if (oldConfig.getUpdated().getTime() != newConfig.getUpdated().getTime()) {
              modifiedJobs.add(new Job[]{oldConfig, newConfig});
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
          for (Job[] configs : modifiedJobs) {
            modifyJob(configs[0], configs[1]);
            notifyConfigurationChange(listener -> listener.onJobModified(configs[0], configs[1]));
          }
          
          // Add new jobs
          for (Job jobConfig : addedJobs) {
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
  private void addJob(Job jobConfig) {
    log.info("Adding new job: {} ({})", jobConfig.getName(), jobConfig.getId());

    try {
      // For now, just log the configuration change
      // In a full implementation, this would trigger job processor to create consumers
      log.info("Job configuration added - restart required for full activation");

    } catch (Exception e) {
      log.error("Error adding job {}: {}", jobConfig.getId(), e.getMessage(), e);
    }
  }
  
  /**
   * Removes a job configuration.
   */
  private void removeJob(String jobId) {
    log.info("Removing job: {}", jobId);
    
    try {
      // Unregister consumers from recovery manager
      if (recoveryManager != null) {
        recoveryManager.getRecoveryStatus().forEach((consumerId, status) -> {
          if (consumerId.startsWith(jobId + "-")) {
            recoveryManager.unregisterConsumer(consumerId);
          }
        });
      }

    } catch (Exception e) {
      log.error("Error removing job {}: {}", jobId, e.getMessage(), e);
    }
  }
  
  /**
   * Modifies an existing job configuration.
   */
  private void modifyJob(Job oldConfig, Job newConfig) {
    log.info("Modifying job: {} ({})", newConfig.getName(), newConfig.getId());

    try {
      // Remove old configuration
      removeJob(oldConfig.getId());

      // Add new configuration
      addJob(newConfig);
      
    } catch (Exception e) {
      log.error("Error modifying job {}: {}", newConfig.getId(), e.getMessage(), e);
    }
  }

  // Utility methods that delegate to existing classes

  /**
   * Calculates a hash for the entire configuration set.
   */
  private long calculateConfigurationHash(Map<String, Job> configs) {
    long hash = 0;
    for (Job config : configs.values()) {
      hash += config.getId().hashCode();
      hash += config.getName().hashCode();
      hash += config.getUpdated().getTime();
    }
    return hash;
  }

  /**
   * Checks if async jobs are enabled in the system configuration.
   */
  private boolean isAsyncJobsEnabled() {
    var obProps = OBPropertiesProvider.getInstance().getOpenbravoProperties();
    return StringUtils.equalsIgnoreCase(obProps.getProperty("kafka.enable", "false"), "true");
  }

  /**
   * Notifies configuration change listeners.
   */
  private void notifyConfigurationChange(java.util.function.Consumer<ConfigurationChangeListener> action) {
    for (ConfigurationChangeListener listener : changeListeners) {
      try {
        action.accept(listener);
      } catch (Exception e) {
        log.warn("Error notifying configuration change listener: {}", e.getMessage());
      }
    }
  }

  // Public API methods

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
   * Forces a configuration reload.
   */
  public void forceConfigurationReload() {
    log.info("Forcing configuration reload");
    checkForConfigurationChanges();
  }

  /**
   * Gets the current configuration status.
   */
  public Map<String, Object> getConfigurationStatus() {
    Map<String, Object> status = new HashMap<>();
    status.put("monitoringEnabled", isMonitoringEnabled.get());
    status.put("reconfigurationEnabled", isReconfigurationEnabled.get());
    status.put("activeJobs", activeJobs.size());
    status.put("lastConfigurationHash", lastConfigurationHash);
    status.put("configCheckIntervalMs", configCheckIntervalMs);
    return status;
  }

  /**
   * Enables or disables configuration monitoring.
   */
  public void setMonitoringEnabled(boolean enabled) {
    isMonitoringEnabled.set(enabled);
    log.info("Configuration monitoring {}", enabled ? "enabled" : "disabled");
  }

  /**
   * Enables or disables reconfiguration.
   */
  public void setReconfigurationEnabled(boolean enabled) {
    isReconfigurationEnabled.set(enabled);
    log.info("Configuration reconfiguration {}", enabled ? "enabled" : "disabled");
  }

  /**
   * Gets the list of active job IDs.
   */
  public List<String> getActiveJobIds() {
    return new ArrayList<>(activeJobs.keySet());
  }

  /**
   * Gets a specific job configuration.
   */
  public Job getJobConfiguration(String jobId) {
    return activeJobs.get(jobId);
  }
}
