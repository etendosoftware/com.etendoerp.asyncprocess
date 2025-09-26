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
 * Manager for dynamic reconfiguration of asynchronous process jobs and topics.
 *
 * <p>This class monitors job configurations stored in the database, detects changes
 * (added, removed or modified jobs) and applies the required actions to the running
 * system. It is intended to allow dynamic reconfiguration without restarting the
 * application server. Monitoring and reconfiguration can be enabled/disabled at runtime.</p>
 *
 * <p>The manager performs periodic checks using an internal scheduler and delegates
 * actual consumer/producer lifecycle operations to other components (for example
 * ConsumerRecoveryManager). It also exposes a listener API for other parts of the
 * system to react when configuration changes occur.</p>
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

  /**
   * Listener interface for configuration change events.
   * Implementations will receive callbacks when jobs are added, removed or modified.
   */
  public interface ConfigurationChangeListener {
    /**
     * Called when a new job configuration is detected and added.
     *
     * @param jobConfig the new job configuration
     */
    void onJobAdded(Job jobConfig);

    /**
     * Called when an existing job configuration is removed.
     *
     * @param jobId id of the removed job
     */
    void onJobRemoved(String jobId);

    /**
     * Called when a job configuration changes.
     *
     * @param oldConfig previous job configuration
     * @param newConfig new job configuration
     */
    void onJobModified(Job oldConfig, Job newConfig);
  }

  private final List<ConfigurationChangeListener> changeListeners = new ArrayList<>();

  /**
   * Creates a new AsyncProcessReconfigurationManager with default check interval.
   *
   * @param healthChecker health checker used to ensure Kafka availability before reconfiguration
   * @param recoveryManager recovery manager to handle consumer registration/unregistration
   */
  public AsyncProcessReconfigurationManager(KafkaHealthChecker healthChecker,
                                          ConsumerRecoveryManager recoveryManager) {
    this(healthChecker, recoveryManager, DEFAULT_CONFIG_CHECK_INTERVAL_MS);
  }

  /**
   * Creates a new AsyncProcessReconfigurationManager with a custom check interval.
   *
   * @param healthChecker health checker used to ensure Kafka availability before reconfiguration
   * @param recoveryManager recovery manager to handle consumer registration/unregistration
   * @param configCheckIntervalMs interval in milliseconds between configuration checks
   */
  public AsyncProcessReconfigurationManager(KafkaHealthChecker healthChecker,
                                          ConsumerRecoveryManager recoveryManager,
                                          long configCheckIntervalMs) {
    this.healthChecker = healthChecker;
    this.recoveryManager = recoveryManager;
    this.configCheckIntervalMs = configCheckIntervalMs;
    this.configScheduler = Executors.newScheduledThreadPool(2);
  }

  /**
   * Starts the background configuration monitoring task.
   *
   * <p>The method performs an initial load of the current configuration and then schedules
   * periodic checks using the configured interval. If monitoring is disabled, the method
   * will return immediately.</p>
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
   * Stops the background configuration monitoring task and attempts an orderly shutdown
   * of the internal scheduler.
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
   * Loads the current configuration from the database into the manager's internal state.
   *
   * <p>This method sets the current OBContext for DAL calls and reads all jobs marked
   * as async. It updates the internal activeJobs map and computes the configuration hash
   * used to detect future changes.</p>
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
   * Performs a single configuration check and applies detected changes.
   *
   * <p>The method first verifies that monitoring and reconfiguration are enabled and
   * that Kafka is healthy. It then loads the latest job configurations and compares
   * a computed hash to the last known hash to decide whether to apply changes.</p>
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
   * Loads job configurations from the database and returns them as a map keyed by job id.
   *
   * @return map of job id to Job configuration; empty map on error
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
   * Applies configuration changes by calculating differences between the provided
   * newConfigs map and the current activeJobs. The actual apply is executed
   * asynchronously using the internal scheduler.
   *
   * @param newConfigs new job configurations to apply
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
   * Adds a new job configuration to the running system.
   *
   * <p>In this simplified implementation the method logs the action. In a full
   * implementation this would create consumers/producers and register them with
   * the recovery manager.</p>
   *
   * @param jobConfig job configuration to add
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
   * Removes a job configuration from the running system.
   *
   * <p>The method will attempt to unregister any consumer instances related to the job
   * using the ConsumerRecoveryManager.</p>
   *
   * @param jobId identifier of the job to remove
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
   * Modifies an existing job configuration by replacing the old configuration.
   *
   * <p>The simplified implementation removes the old job and adds the new one.</p>
   *
   * @param oldConfig previous configuration
   * @param newConfig new configuration
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
   * Calculates a simple hash for the provided configuration set.
   *
   * <p>The algorithm sums identifiers, names and last-updated timestamps to
   * provide a quick change-detection mechanism. It is not cryptographically secure.</p>
   *
   * @param configs map of job id to Job
   * @return computed hash value
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
   * Checks whether asynchronous jobs are enabled in the system properties.
   *
   * @return true if async jobs are enabled; false otherwise
   */
  private boolean isAsyncJobsEnabled() {
    var obProps = OBPropertiesProvider.getInstance().getOpenbravoProperties();
    return StringUtils.equalsIgnoreCase(obProps.getProperty("kafka.enable", "false"), "true");
  }

  /**
   * Invokes the provided action on all registered configuration change listeners.
   * Listener exceptions are caught and logged to avoid interrupting notification of others.
   *
   * @param action consumer that receives a ConfigurationChangeListener
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
   * Registers a configuration change listener.
   *
   * @param listener listener implementation to register
   */
  public void addConfigurationChangeListener(ConfigurationChangeListener listener) {
    changeListeners.add(listener);
  }

  /**
   * Removes a previously registered configuration change listener.
   *
   * @param listener listener implementation to remove
   */
  public void removeConfigurationChangeListener(ConfigurationChangeListener listener) {
    changeListeners.remove(listener);
  }

  /**
   * Forces an immediate configuration reload and applies changes if detected.
   */
  public void forceConfigurationReload() {
    log.info("Forcing configuration reload");
    checkForConfigurationChanges();
  }

  /**
   * Returns a snapshot of the manager's current status useful for diagnostics.
   *
   * @return map containing monitoring state, active jobs count, last hash and interval
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
   * Enables or disables configuration monitoring at runtime.
   *
   * @param enabled true to enable monitoring, false to disable
   */
  public void setMonitoringEnabled(boolean enabled) {
    isMonitoringEnabled.set(enabled);
    log.info("Configuration monitoring {}", enabled ? "enabled" : "disabled");
  }

  /**
   * Enables or disables the application of configuration changes at runtime.
   *
   * @param enabled true to enable reconfiguration, false to disable
   */
  public void setReconfigurationEnabled(boolean enabled) {
    isReconfigurationEnabled.set(enabled);
    log.info("Configuration reconfiguration {}", enabled ? "enabled" : "disabled");
  }

  /**
   * Returns a list with the ids of currently active jobs.
   *
   * @return list of active job identifiers
   */
  public List<String> getActiveJobIds() {
    return new ArrayList<>(activeJobs.keySet());
  }

  /**
   * Returns the Job configuration for the given job id if present.
   *
   * @param jobId identifier of the job
   * @return Job configuration or null if not found
   */
  public Job getJobConfiguration(String jobId) {
    return activeJobs.get(jobId);
  }
}
