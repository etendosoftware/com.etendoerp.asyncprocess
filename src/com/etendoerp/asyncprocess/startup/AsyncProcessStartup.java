package com.etendoerp.asyncprocess.startup;

import com.etendoerp.asyncprocess.circuit.KafkaCircuitBreaker;
import com.etendoerp.asyncprocess.config.AsyncProcessReconfigurationManager;
import com.etendoerp.asyncprocess.health.KafkaHealthChecker;
import com.etendoerp.asyncprocess.monitoring.AsyncProcessMonitor;
import com.etendoerp.asyncprocess.recovery.ConsumerRecoveryManager;
import com.etendoerp.reactor.EtendoReactorSetup;
import com.smf.jobs.model.Job;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.criterion.Restrictions;
import org.openbravo.base.exception.OBException;
import org.openbravo.base.session.OBPropertiesProvider;
import org.openbravo.client.kernel.ComponentProvider;
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.service.OBDal;
import reactor.core.Disposable;

import javax.enterprise.context.ApplicationScoped;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;


/**
 * Enhanced startup process for async process workflows with advanced configuration,
 * health monitoring, auto-recovery, and circuit breaker protection.
 * This class orchestrates the initialization of core components and delegates
 * job processing to the JobProcessor.
 */
@ApplicationScoped
@ComponentProvider.Qualifier(AsyncProcessStartup.ASYNC_PROCESS_STARTUP)
public class AsyncProcessStartup implements EtendoReactorSetup {
  public static final String ASYNC_PROCESS_STARTUP = "asyncProcessStartup";
  // Configuration properties keys
  public static final String KAFKA_TOPIC_PARTITIONS = "kafka.topic.partitions";
  public static final String KAFKA_URL = "kafka.url";
  // Default Kafka connection values
  public static final String DEFAULT_KAFKA_URL = "localhost:29092";
  private static final Logger log = LogManager.getLogger();
  private static final String DEFAULT_KAFKA_URL_TOMCAT_IN_DOCKER = "kafka:9092";

  private final Map<String, Disposable> activeSubscriptions = new HashMap<>();

  // Core components for resilience and monitoring
  private KafkaHealthChecker healthChecker;
  private ConsumerRecoveryManager recoveryManager;
  private AsyncProcessReconfigurationManager reconfigurationManager;
  private KafkaCircuitBreaker circuitBreaker;
  private AsyncProcessMonitor processMonitor;
  private JobProcessor jobProcessor;
  private KafkaClientManager kafkaClientManager;
  private volatile boolean isInitialized = false;

  private static String getKafkaHost(Properties obProps) {
    if (obProps.containsKey(KAFKA_URL)) {
      return obProps.getProperty(KAFKA_URL);
    }
    if (propInTrue(obProps, "docker_com.etendoerp.tomcat")) {
      return DEFAULT_KAFKA_URL_TOMCAT_IN_DOCKER;
    }
    return DEFAULT_KAFKA_URL;
  }

  private static boolean propInTrue(Properties obProps, String propKey) {
    return obProps.containsKey(propKey) && StringUtils.equalsIgnoreCase(obProps.getProperty(propKey, "false"), "true");
  }

  @Override
  public void init() {
    log.info("Etendo Reactor Startup with Enhanced Resilience and Monitoring");

    try {
      var obProps = OBPropertiesProvider.getInstance().getOpenbravoProperties();
      String kafkaHost = getKafkaHost(obProps);

      this.kafkaClientManager = new KafkaClientManager(kafkaHost);

      if (!shouldStart()) {
        return;
      }
      initializeHealthChecker(kafkaHost);
      initializeCircuitBreaker();
      initializeMonitoring();
      initializeRecoveryManager();
      initializeReconfigurationManager();

      this.jobProcessor = new JobProcessor(
          this.processMonitor,
          this.recoveryManager,
          this.healthChecker,
          this.activeSubscriptions,
          this.kafkaClientManager
      );

      performInitialSetup();

      isInitialized = true;
      log.info("Enhanced async process startup completed successfully");

    } catch (Exception e) {
      log.error("Critical error during startup initialization", e);
      handleStartupFailure(e);
    }
  }

  boolean shouldStart() {
    try {
      OBContext.setAdminMode();
      var critJob = OBDal.getInstance().createCriteria(Job.class);
      critJob.add(Restrictions.eq(Job.PROPERTY_ETAPISASYNC, true));
      List<Job> list = critJob.list();
      if (list.isEmpty()) {
        log.info("No async process found, reactor will not connect to any topic until restart.");
        return false;
      }
      if (!isAsyncJobsEnabled()) {
        log.warn("There are async jobs defined, but Kafka is disabled.");
        log.warn("To enable async jobs, set the property 'kafka.enable' to true in gradle.properties.");
        log.warn(
            "The recommended steps are editing the gradle.properties, and then running './gradlew setup smartbuild' to update and deploy the Openbravo.properties file.");
        return false;
      }
      return true;
    } finally {
      OBContext.restorePreviousMode();
    }
  }

  void initializeHealthChecker(String kafkaHost) {
    try {
      healthChecker = new KafkaHealthChecker(kafkaHost, 30, 5000);
      healthChecker.setOnKafkaHealthRestored(() -> {
        log.info("Kafka health restored - attempting to restart failed consumers");
        if (isInitialized && recoveryManager != null) {
          recoveryManager.setRecoveryEnabled(true);
        }
      });
      healthChecker.setOnKafkaHealthLost(() -> {
        log.warn("Kafka health lost - disabling new consumer creation");
        if (circuitBreaker != null) {
          circuitBreaker.forceOpen();
        }
      });
      healthChecker.start();
      log.info("Health checker initialized and started");
    } catch (Exception e) {
      log.error("Failed to initialize health checker", e);
      throw new OBException("Health checker initialization failed", e);
    }
  }

  void initializeCircuitBreaker() {
    try {
      KafkaCircuitBreaker.CircuitBreakerConfig config = new KafkaCircuitBreaker.CircuitBreakerConfig(
          5, java.time.Duration.ofMinutes(2), java.time.Duration.ofSeconds(30),
          10000, 50, 10
      );
      circuitBreaker = new KafkaCircuitBreaker("async-process-kafka", config);
      circuitBreaker.setStateChangeListener((name, from, to, reason) -> {
        log.info("Circuit breaker '{}' state changed from {} to {}: {}", name, from, to, reason);
        if (processMonitor != null) {
          processMonitor.recordKafkaConnection(to == KafkaCircuitBreaker.State.CLOSED);
        }
      });
      log.info("Circuit breaker initialized");
    } catch (Exception e) {
      log.error("Failed to initialize circuit breaker", e);
      throw new OBException("Circuit breaker initialization failed", e);
    }
  }

  void initializeMonitoring() {
    try {
      processMonitor = new AsyncProcessMonitor(10000, 30000); // 10s metrics, 30s alerts
      processMonitor.addAlertListener(new AsyncProcessMonitor.AlertListener() {
        @Override
        public void onAlert(AsyncProcessMonitor.Alert alert) {
          log.warn("ALERT [{}]: {} - {}", alert.getSeverity(), alert.getType(), alert.getMessage());
        }

        @Override
        public void onAlertResolved(AsyncProcessMonitor.Alert alert) {
          log.info("ALERT RESOLVED [{}]: {}", alert.getType(), alert.getMessage());
        }
      });
      processMonitor.start();
      log.info("Process monitoring initialized and started");
    } catch (Exception e) {
      log.error("Failed to initialize process monitoring", e);
    }
  }

  void initializeRecoveryManager() {
    try {
      recoveryManager = new ConsumerRecoveryManager(healthChecker, 5, 10000, 2);
      recoveryManager.setConsumerRecreationFunction(consumerInfo -> {
        try {
          return executeWithClassLoaderContext(() ->
              kafkaClientManager.createReceiver(
                  consumerInfo.getTopic(),
                  consumerInfo.isRegExp(),
                  consumerInfo.getConfig(),
                  consumerInfo.getGroupId()
              )
          );
        } catch (Exception e) {
          log.error("Failed to recreate consumer {}: {}", consumerInfo.getConsumerId(), e.getMessage());
          throw new OBException("Consumer recreation failed", e);
        }
      });
      log.info("Consumer recovery manager initialized");
    } catch (Exception e) {
      log.error("Failed to initialize recovery manager", e);
      throw new OBException("Recovery manager initialization failed", e);
    }
  }

  void initializeReconfigurationManager() {
    try {
      reconfigurationManager = new AsyncProcessReconfigurationManager(healthChecker, recoveryManager);
      reconfigurationManager.addConfigurationChangeListener(
          new AsyncProcessReconfigurationManager.ConfigurationChangeListener() {
            @Override
            public void onJobAdded(Job jobConfig) {
              log.info("Configuration: Job added - {}", jobConfig.getName());
            }

            @Override
            public void onJobRemoved(String jobId) {
              log.info("Configuration: Job removed - {}", jobId);
            }

            @Override
            public void onJobModified(Job oldConfig, Job newConfig) {
              log.info("Configuration: Job modified - {}", newConfig.getName());
            }
          });
      reconfigurationManager.startMonitoring();
      log.info("Reconfiguration manager initialized and started");
    } catch (Exception e) {
      log.error("Failed to initialize reconfiguration manager", e);
    }
  }

  /**
   * Performs the initial setup by creating Kafka clients and delegating to the JobProcessor.
   */
  void performInitialSetup() {
    try {
      CompletableFuture<Void> setupFuture = circuitBreaker.executeAsync(() ->
          CompletableFuture.runAsync(() -> executeWithClassLoaderContext(this::executeKafkaSetup)));

      waitForSetupCompletion(setupFuture);
    } catch (Exception e) {
      log.error("Critical error during initial setup", e);
      throw new OBException("Initial setup failed", e);
    }
  }

  /**
   * Waits for the setup future to complete and handles any exceptions that occur.
   */
  private void waitForSetupCompletion(CompletableFuture<Void> setupFuture) {
    try {
      setupFuture.get(30, TimeUnit.SECONDS);
      log.info("Initial setup completed successfully");
    } catch (InterruptedException e) {
      log.error("Initial setup was interrupted", e);
      Thread.currentThread().interrupt(); // Re-interrupt the thread
      handleSetupFailure(e);
      throw new OBException("Initial setup was interrupted", e);
    } catch (Exception e) {
      log.error("Initial setup failed within timeout", e);
      handleSetupFailure(e);
      throw new OBException("Initial setup failed", e);
    }
  }

  /**
   * Executes the Kafka setup operations including topic creation and job processing.
   */
  private Void executeKafkaSetup() {
    try {
      if (!isAsyncJobsEnabled()) {
        log.warn("There are async jobs defined, but Kafka is disabled.");
        return null;
      }
      try (AdminClient adminClient = kafkaClientManager.createAdminClient()) {
        kafkaClientManager.createKafkaConnectTopics(adminClient);
      }
      jobProcessor.processAllJobs();
      return null;
    } catch (Exception e) {
      log.error("Kafka operation failed during setup", e);
      throw new OBException("Kafka setup failed", e);
    }
  }

  private void handleSetupFailure(Throwable throwable) {
    log.error("Initial setup failed: {}", throwable.getMessage(), throwable);
    if (processMonitor != null) {
      processMonitor.recordJobExecution("SYSTEM_SETUP", "Initial Setup Failed", 0, false, false);
    }
    if (recoveryManager != null && recoveryManager.isRecoveryEnabled()) {
      log.info("Attempting recovery after setup failure...");
    }
  }

  private void handleStartupFailure(Exception e) {
    log.error("Startup failure - attempting graceful degradation", e);
    try {
      if (healthChecker == null) {
        var obProps = OBPropertiesProvider.getInstance().getOpenbravoProperties();
        String kafkaHost = getKafkaHost(obProps);
        initializeHealthChecker(kafkaHost);
      }
    } catch (Exception healthError) {
      log.error("Failed to start health checker during failure recovery", healthError);
    }
  }

  private boolean isAsyncJobsEnabled() {
    var obProps = OBPropertiesProvider.getInstance().getOpenbravoProperties();
    return propInTrue(obProps, "kafka.enable");
  }

  private <T> T executeWithClassLoaderContext(Supplier<T> operation) {
    final ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
    final ClassLoader applicationClassLoader = this.getClass().getClassLoader();
    try {
      if (currentClassLoader == null || currentClassLoader != applicationClassLoader) {
        Thread.currentThread().setContextClassLoader(applicationClassLoader);
      }
      return operation.get();
    } finally {
      Thread.currentThread().setContextClassLoader(currentClassLoader);
    }
  }

  public void shutdown() {
    log.info("Shutting down Enhanced AsyncProcessStartup...");
    try {
      if (reconfigurationManager != null) reconfigurationManager.stopMonitoring();
      if (processMonitor != null) processMonitor.stop();
      if (healthChecker != null) healthChecker.stop();
      if (recoveryManager != null) recoveryManager.shutdown();
      if (circuitBreaker != null) circuitBreaker.shutdown();

      activeSubscriptions.values().forEach(subscription -> {
        try {
          if (!subscription.isDisposed()) subscription.dispose();
        } catch (Exception e) {
          log.warn("Error disposing subscription: {}", e.getMessage());
        }
      });
      activeSubscriptions.clear();

      if (jobProcessor != null) {
        jobProcessor.shutdownSchedulers();
      }

      log.info("Enhanced AsyncProcessStartup shutdown completed");
    } catch (Exception e) {
      log.error("Error during enhanced shutdown", e);
    }
  }

  public void forceHealthCheck() {
    log.info("Forcing system health check...");
    if (healthChecker != null) {
      log.info("Health checker status: {}", healthChecker.isKafkaHealthy());
    }
    if (recoveryManager != null && healthChecker != null && healthChecker.isKafkaHealthy()) {
      recoveryManager.setRecoveryEnabled(true);
    }
    if (reconfigurationManager != null) {
      reconfigurationManager.forceConfigurationReload();
    }
  }

  public void forceConsumerRecovery(String consumerId) {
    if (recoveryManager != null) {
      try {
        recoveryManager.forceRecoverConsumer(consumerId);
        log.info("Forced recovery completed for consumer: {}", consumerId);
      } catch (Exception e) {
        log.error("Failed to force recovery for consumer {}: {}", consumerId, e.getMessage(), e);
        throw e;
      }
    } else {
      throw new IllegalStateException("Recovery manager not initialized");
    }
  }
}
