package com.etendoerp.asyncprocess.startup;

import com.etendoerp.asyncprocess.circuit.KafkaCircuitBreaker;
import com.etendoerp.asyncprocess.config.AsyncProcessConfig;
import com.etendoerp.asyncprocess.config.AsyncProcessReconfigurationManager;
import com.etendoerp.asyncprocess.health.KafkaHealthChecker;
import com.etendoerp.asyncprocess.model.AsyncProcessExecution;
import com.etendoerp.asyncprocess.model.AsyncProcessState;
import com.etendoerp.asyncprocess.monitoring.AsyncProcessMonitor;
import com.etendoerp.asyncprocess.recovery.ConsumerRecoveryManager;
import com.etendoerp.asyncprocess.retry.RetryPolicy;
import com.etendoerp.asyncprocess.retry.SimpleRetryPolicy;
import com.etendoerp.asyncprocess.serdes.AsyncProcessExecutionDeserializer;
import com.etendoerp.reactor.EtendoReactorSetup;
import com.smf.jobs.Action;
import com.smf.jobs.model.Job;
import com.smf.jobs.model.JobLine;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.criterion.Restrictions;
import org.openbravo.base.session.OBPropertiesProvider;
import org.openbravo.base.util.OBClassLoader;
import org.openbravo.base.weld.WeldUtils;
import org.openbravo.client.application.Process;
import org.openbravo.client.kernel.ComponentProvider;
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.service.OBDal;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.receiver.internals.ConsumerFactory;
import reactor.kafka.receiver.internals.DefaultKafkaReceiver;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import javax.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static com.etendoerp.asyncprocess.util.TopicUtil.createTopic;

/**
 * Enhanced startup process for async process workflows with advanced configuration,
 * health monitoring, auto-recovery, and circuit breaker protection.
 */
@ApplicationScoped
@ComponentProvider.Qualifier(AsyncProcessStartup.ASYNC_PROCESS_STARTUP)
public class AsyncProcessStartup implements EtendoReactorSetup {
  public static final String ASYNC_PROCESS_STARTUP = "asyncProcessStartup";
  private static final Logger log = LogManager.getLogger();
  private static final String DEFAULT_RESULT_SUB_TOPIC = "result";
  private static final String DEFAULT_ERROR_SUB_TOPIC = "error";

  // Default configuration
  private static final int DEFAULT_PARALLEL_THREADS = 8;
  private static final int DEFAULT_MAX_RETRIES = 3;
  private static final long DEFAULT_RETRY_DELAY = 1000; // ms
  private static final int DEFAULT_PREFETCH_COUNT = 1;
  public static final String KAFKA_TOPIC_PARTITIONS = "kafka.topic.partitions";
  public static final String KAFKA_URL = "kafka.url";
  private static final int DEFAULT_KAFKA_TOPIC_PARTITIONS = 5;
  public static final String DEFAULT_KAFKA_URL = "localhost:29092";
  private static final String DEFAULT_KAFKA_URL_TOMCAT_IN_DOCKER = "kafka:9092";

  // Map to maintain schedulers per job
  private final Map<String, ScheduledExecutorService> jobSchedulers = new HashMap<>();
  private final Map<String, Disposable> activeSubscriptions = new HashMap<>();

  // Enhanced components for resilience and monitoring
  private KafkaHealthChecker healthChecker;
  private ConsumerRecoveryManager recoveryManager;
  private AsyncProcessReconfigurationManager reconfigurationManager;
  private KafkaCircuitBreaker circuitBreaker;
  private AsyncProcessMonitor processMonitor;
  private volatile boolean isInitialized = false;

  @Override
  public void init() {
    log.info("Etendo Reactor Startup with Enhanced Resilience and Monitoring");

    try {
      var obProps = OBPropertiesProvider.getInstance().getOpenbravoProperties();
      String kafkaHost = getKafkaHost(obProps);

      // Initialize health checker first
      initializeHealthChecker(kafkaHost);

      // Initialize circuit breaker
      initializeCircuitBreaker();

      // Initialize monitoring
      initializeMonitoring();

      // Initialize recovery manager
      initializeRecoveryManager();

      // Initialize reconfiguration manager
      initializeReconfigurationManager();

      // Perform initial setup - moved outside circuit breaker due to context requirements
      performInitialSetup(obProps, kafkaHost);

      isInitialized = true;
      log.info("Enhanced async process startup completed successfully");

    } catch (Exception e) {
      log.error("Critical error during startup initialization", e);
      handleStartupFailure(e);
    }
  }

  /**
   * Initializes the health checker component.
   */
  private void initializeHealthChecker(String kafkaHost) {
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
      throw new RuntimeException("Health checker initialization failed", e);
    }
  }

  /**
   * Initializes the circuit breaker component.
   */
  private void initializeCircuitBreaker() {
    try {
      KafkaCircuitBreaker.CircuitBreakerConfig config =
          new KafkaCircuitBreaker.CircuitBreakerConfig(
              5,                              // failureThreshold - 5 failures
              java.time.Duration.ofMinutes(2), // timeout - 2 minutes
              java.time.Duration.ofSeconds(30), // retryInterval - 30 seconds
              10000,                          // slowCallDurationThreshold - 10 seconds
              50,                             // slowCallRateThreshold - 50%
              10                              // minimumNumberOfCalls
          );

      circuitBreaker = new KafkaCircuitBreaker("async-process-kafka", config);

      circuitBreaker.setStateChangeListener((name, from, to, reason) -> {
        log.info("Circuit breaker '{}' state changed from {} to {}: {}", name, from, to, reason);
        if (processMonitor != null) {
          // Record circuit breaker events in monitoring
          processMonitor.recordKafkaConnection(to == KafkaCircuitBreaker.State.CLOSED);
        }
      });

      log.info("Circuit breaker initialized");

    } catch (Exception e) {
      log.error("Failed to initialize circuit breaker", e);
      throw new RuntimeException("Circuit breaker initialization failed", e);
    }
  }

  /**
   * Initializes the monitoring component.
   */
  private void initializeMonitoring() {
    try {
      processMonitor = new AsyncProcessMonitor(10000, 30000); // 10s metrics, 30s alerts

      // Add alert listener for logging
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
      // Don't fail startup for monitoring issues
    }
  }

  /**
   * Initializes the recovery manager component.
   */
  private void initializeRecoveryManager() {
    try {
      recoveryManager = new ConsumerRecoveryManager(healthChecker, 5, 10000, 2);

      // Set consumer recreation function
      recoveryManager.setConsumerRecreationFunction(consumerInfo -> {
        try {
          return executeWithClassLoaderContext(() -> 
              createReceiver(
                  consumerInfo.getTopic(),
                  consumerInfo.isRegExp(),
                  consumerInfo.getConfig(),
                  consumerInfo.getGroupId(),
                  consumerInfo.getKafkaHost()
              )
          );
        } catch (Exception e) {
          log.error("Failed to recreate consumer {}: {}", consumerInfo.getConsumerId(), e.getMessage());
          throw new RuntimeException("Consumer recreation failed", e);
        }
      });

      log.info("Consumer recovery manager initialized");

    } catch (Exception e) {
      log.error("Failed to initialize recovery manager", e);
      throw new RuntimeException("Recovery manager initialization failed", e);
    }
  }

  /**
   * Initializes the reconfiguration manager component.
   */
  private void initializeReconfigurationManager() {
    try {
      reconfigurationManager = new AsyncProcessReconfigurationManager(
          this, healthChecker, recoveryManager, 60000); // Check every minute

      reconfigurationManager.addConfigurationChangeListener(
          new AsyncProcessReconfigurationManager.ConfigurationChangeListener() {
            @Override
            public void onJobAdded(AsyncProcessReconfigurationManager.JobConfiguration jobConfig) {
              log.info("Configuration: Job added - {}", jobConfig.getJob().getName());
            }

            @Override
            public void onJobRemoved(String jobId) {
              log.info("Configuration: Job removed - {}", jobId);
            }

            @Override
            public void onJobModified(AsyncProcessReconfigurationManager.JobConfiguration oldConfig,
                AsyncProcessReconfigurationManager.JobConfiguration newConfig) {
              log.info("Configuration: Job modified - {}", newConfig.getJob().getName());
            }
          });

      reconfigurationManager.startMonitoring();
      log.info("Reconfiguration manager initialized and started");

    } catch (Exception e) {
      log.error("Failed to initialize reconfiguration manager", e);
      // Don't fail startup for reconfiguration issues
    }
  }

  /**
   * Performs the initial setup of jobs and consumers.
   * 
   * CONTEXT-AWARE CIRCUIT BREAKER SOLUTION:
   * =====================================
   * Esta implementación resuelve el problema del contexto de clases cuando se ejecuta código
   * dentro del circuit breaker. El circuit breaker ejecuta en threads del pool de ForkJoin
   * que no tienen acceso al contexto de clases de Etendo/Openbravo.
   * 
   * ESTRATEGIA IMPLEMENTADA:
   * 1. FASE DE PRELOAD (Hilo Principal): Se ejecuta en el hilo principal con contexto completo
   *    - Carga de jobs desde base de datos
   *    - Pre-carga de action suppliers (createActionFactory)
   *    - Acceso a OBContext y OBDal
   * 
   * 2. FASE KAFKA (Circuit Breaker): Se ejecuta dentro del circuit breaker
   *    - Operaciones de Kafka (AdminClient, KafkaSender)
   *    - Creación de topics
   *    - Procesamiento reactivo con suppliers pre-cargados
   *    - **PRESERVACIÓN DE CONTEXT**: executeWithClassLoaderContext() asegura que
   *      las clases de Kafka (StringDeserializer, etc.) estén disponibles
   * 
   * BENEFICIOS:
   * - Evita ClassNotFoundException en threads asíncronos
   * - Mantiene circuit breaker para operaciones de Kafka
   * - Mejor rendimiento al pre-cargar classes una sola vez
   * - Debugging más fácil con errores inmediatos en startup
   * - **NUEVO**: Contexto de clases preservado para Kafka operations
   * 
   * Ejecuta operaciones que requieren contexto de Etendo fuera del circuit breaker,
   * y luego usa el circuit breaker solo para operaciones de Kafka con contexto preservado.
   */
  private void performInitialSetup(Properties obProps, String kafkaHost) {
    try {
      // PASO 1: Pre-cargar todo lo que requiere contexto de Etendo (fuera del circuit breaker)
      log.debug("Starting context-aware preloading phase...");
      final Map<String, Supplier<Action>> actionSuppliers = preloadActionSuppliers();
      final List<Job> jobs = loadAsyncJobs();
      
      if (jobs.isEmpty()) {
          log.info("No async process found, reactor will not connect to any topic until restart.");
          return;
      }
      
      // PASO 2: Crear configuraciones sin contexto
      log.debug("Creating Kafka configurations...");
      Properties kafkaProps = getKafkaServerConfigProps(kafkaHost);
      
      // PASO 3: Usar circuit breaker solo para operaciones de Kafka
      log.debug("Starting Kafka operations with circuit breaker protection...");
      
      CompletableFuture<Void> setupFuture = circuitBreaker.executeAsync(() -> {
          return CompletableFuture.runAsync(() -> {
              executeWithClassLoaderContext(() -> {
                  try (AdminClient adminKafka = AdminClient.create(kafkaProps)) {
                      log.debug("Creating Kafka sender...");
                      KafkaSender<String, AsyncProcessExecution> kafkaSender = crateSender(kafkaHost);
                      
                      if (!isAsyncJobsEnabled()) {
                          log.warn("There are async jobs defined, but Kafka is disabled.");
                          return null;
                      }
                      
                      log.debug("Creating Kafka topics...");
                      createKafkaConnectTopics(obProps, adminKafka);
                      
                      // Procesar jobs con suppliers pre-cargados
                      log.debug("Processing jobs with preloaded suppliers...");
                      processJobsWithPreloadedSuppliers(jobs, adminKafka, kafkaSender, kafkaHost, actionSuppliers);
                      
                      return null;
                  } catch (Exception e) {
                      log.error("Kafka operation failed during setup", e);
                      throw new RuntimeException("Kafka setup failed", e);
                  }
              });
          });
      });
      
      // Manejar el resultado de forma síncrona para mantener compatibilidad
      try {
          setupFuture.get(30, TimeUnit.SECONDS); // Timeout de 30 segundos
          log.info("Initial setup completed successfully");
      } catch (Exception e) {
          log.error("Initial setup failed within timeout", e);
          handleSetupFailure(e);
          // Re-lanzar la excepción para que el startup falle apropiadamente
          throw new RuntimeException("Initial setup failed", e);
      }
      
    } catch (Exception e) {
      log.error("Critical error during initial setup", e);
      throw new RuntimeException("Initial setup failed", e);
    }
  }

  /**
   * Processes a single job with enhanced error handling.
   */
private Flux<Map.Entry<String, List<Flux<ReceiverRecord<String, AsyncProcessExecution>>>>> processJob(
      Job job, AdminClient adminKafka, KafkaSender<String, AsyncProcessExecution> kafkaSender,
      String kafkaHost, Map<String, Supplier<Action>> actionSuppliers) {

    try {
      // Configure or create the scheduler for this job
      configureJobScheduler(job);

      var jobLines = job.getJOBSJobLineList();
      jobLines.sort(Comparator.comparing(JobLine::getLineNo));

      return Flux.fromStream(jobLines.stream()).map(jobLine -> {
        try {
          return processJobLine(job, jobLine, jobLines, adminKafka, kafkaSender, kafkaHost, actionSuppliers);
        } catch (Exception e) {
          log.error("Error processing job line {} for job {}: {}", jobLine.getId(), job.getId(), e.getMessage(), e);
          return Map.entry(jobLine.getId(), new ArrayList<Flux<ReceiverRecord<String, AsyncProcessExecution>>>());
        }
      });

    } catch (Exception e) {
      log.error("Error processing job {}: {}", job.getId(), e.getMessage(), e);
      return Flux.empty();
    }
  }

  /**
   * Processes a single job line with enhanced error handling.
   */
  private Map.Entry<String, List<Flux<ReceiverRecord<String, AsyncProcessExecution>>>> processJobLine(
      Job job, JobLine jobLine, List<JobLine> jobLines, AdminClient adminKafka,
      KafkaSender<String, AsyncProcessExecution> kafkaSender, String kafkaHost,
      Map<String, Supplier<Action>> actionSuppliers) {

    try {
      // Get the configuration for this job line
      AsyncProcessConfig config = getJobLineConfig(jobLine);

      String topic = calculateCurrentTopic(jobLine, jobLines);
      // Validate if the topic exists, if not create it
      int numPartitions = getNumPartitions();
      existsOrCreateTopic(adminKafka, topic, numPartitions);

      int k = 1;
      if ((jobLine.getJobsJob() != null && BooleanUtils.isTrue(jobLine.getJobsJob().isEtapConsumerPerPartition()))
          || BooleanUtils.isTrue(jobLine.isEtapConsumerPerPartition())) {
        k = numPartitions;
      }

      List<Flux<ReceiverRecord<String, AsyncProcessExecution>>> receivers = new ArrayList<>();

      for (int i = 0; i < k; i++) {
        try {
          String consumerId = jobLine.getId() + "-" + i;
          String groupId = getGroupId(jobLine);

          var receiver = createReceiver(topic, job.isEtapIsregularexp(), config, groupId, kafkaHost);

          // Create retry policy based on configuration
          RetryPolicy retryPolicy = new SimpleRetryPolicy(config.getMaxRetries(), config.getRetryDelayMs());

          Supplier<Action> actionFactory = actionSuppliers.get(jobLine.getId());

          if (actionFactory == null) {
            log.error("FATAL: No action supplier found for job line {}. Skipping.", jobLine.getId());
            return Map.entry(jobLine.getId(), new ArrayList<>());
          }
          // Create enhanced consumer with monitoring
          ReceiverRecordConsumer recordConsumer = new ReceiverRecordConsumer(
              jobLine.getId() + k,
              actionFactory,
              calculateNextTopic(jobLine, jobLines),
              calculateErrorTopic(job),
              convertState(jobLine.getEtapTargetstatus()),
              kafkaSender,
              job.getClient().getId(),
              job.getOrganization().getId(),
              retryPolicy,
              getJobScheduler(job.getId())
          );

          // Subscribe with error handling and monitoring
          Disposable subscription = receiver.doOnNext(record -> {
            // Record consumer activity
            if (processMonitor != null) {
              processMonitor.recordConsumerActivity(consumerId, groupId, topic);
            }
          }).doOnError(error -> {
            log.error("Error in consumer {} for topic {}: {}", consumerId, topic, error.getMessage(), error);
            if (processMonitor != null) {
              processMonitor.recordConsumerConnectionLost(consumerId);
            }
            // Schedule recovery
            if (recoveryManager != null) {
              try {
                scheduleConsumerRecovery(consumerId, groupId, topic, error.getMessage());
              } catch (Exception e) {
                log.error("Failed to schedule recovery for consumer {}: {}", consumerId, e.getMessage());
              }
            }
          }).subscribe(recordConsumer);

          activeSubscriptions.put(consumerId, subscription);

          // Register consumer with recovery manager
          if (recoveryManager != null) {
            ConsumerRecoveryManager.ConsumerInfo consumerInfo =
                new ConsumerRecoveryManager.ConsumerInfo(
                    consumerId, groupId, topic, job.isEtapIsregularexp(), config,
                    jobLine.getId(), actionFactory,
                    calculateNextTopic(jobLine, jobLines), calculateErrorTopic(job),
                    convertState(jobLine.getEtapTargetstatus()), kafkaSender,
                    job.getClient().getId(), job.getOrganization().getId(),
                    retryPolicy, getJobScheduler(job.getId()), kafkaHost
                );
            consumerInfo.setSubscription(subscription);
            recoveryManager.registerConsumer(consumerInfo);
          }

          // Register with health checker
          if (healthChecker != null) {
            healthChecker.registerConsumerGroup(groupId);
          }

          receivers.add(receiver);

        } catch (Exception e) {
          log.error("An error has occurred creating consumer {} for job line {}: {}", i, jobLine.getId(), e.getMessage(), e);
        }
      }

      return Map.entry(jobLine.getId(), receivers);

    } catch (Exception e) {
      log.error("Error processing job line {}: {}", jobLine.getId(), e.getMessage(), e);
      return Map.entry(jobLine.getId(), new ArrayList<>());
    }
  }

  /**
   * Schedules consumer recovery.
   */
  private void scheduleConsumerRecovery(String consumerId, String groupId, String topic, String reason) {
    log.info("Scheduling recovery for consumer {} due to: {}", consumerId, reason);
    // The recovery manager will handle the actual recovery logic
  }

  /**
   * Schedules retry setup when initial setup fails.
   */
  private void scheduleRetrySetup(Properties obProps, String kafkaHost) {
    ScheduledExecutorService retryScheduler = Executors.newSingleThreadScheduledExecutor();
    retryScheduler.schedule(() -> {
      try {
        log.info("Retrying async process setup...");
        circuitBreaker.execute(() -> {
          performInitialSetup(obProps, kafkaHost);
          return true;
        });
        log.info("Retry setup completed successfully");
      } catch (Exception e) {
        log.warn("Retry setup failed, will try again later: {}", e.getMessage());
        scheduleRetrySetup(obProps, kafkaHost);
      } finally {
        retryScheduler.shutdown();
      }
    }, 30, TimeUnit.SECONDS);
  }

  /**
   * Handles startup failure.
   */
  private void handleStartupFailure(Exception e) {
    log.error("Startup failure - attempting graceful degradation", e);

    // Try to start health checker at minimum
    try {
      if (healthChecker == null) {
        var obProps = OBPropertiesProvider.getInstance().getOpenbravoProperties();
        String kafkaHost = getKafkaHost(obProps);
        initializeHealthChecker(kafkaHost);
      }
    } catch (Exception healthError) {
      log.error("Failed to start health checker during failure recovery", healthError);
    }

    // Don't throw - allow system to continue in degraded mode
  }

  /**
   * Handles job processing errors.
   */
  private void handleJobProcessingError(Throwable error) {
    log.error("Job processing error occurred", error);

    if (circuitBreaker != null) {
      // Open circuit breaker on severe errors
      if (error instanceof OutOfMemoryError ||
          error.getMessage().contains("Connection refused") ||
          error.getMessage().contains("Timeout")) {
        circuitBreaker.forceOpen();
      }
    }

    if (processMonitor != null) {
      processMonitor.recordKafkaConnection(false);
    }
  }

  /**
   * Creates Kafka Connect topics based on the provided properties.
   *
   * <p>This method retrieves a list of table names from the kafka.connect.tables property
   * in the provided Properties object. For each table name, it ensures the name starts
   * with "public." and constructs a topic name in the format "default.{table}". It then
   * checks if the topic exists or creates it with the specified number of partitions.
   *
   * @param props
   *     The Properties object containing Kafka configuration values.
   * @param adminKafka
   *     The AdminClient instance used to manage Kafka topics.
   */
  private void createKafkaConnectTopics(Properties props, AdminClient adminKafka) {
    // Retrieve the list of table names from the properties
    var tableNames = props.getProperty("kafka.connect.tables", null);
    if (StringUtils.isEmpty(tableNames)) {
      return; // Exit if no table names are provided
    }

    // Split the table names into an array
    String[] tables = tableNames.split(",");
    for (String table : tables) {
      // Ensure the table name starts with "public."
      if (!StringUtils.startsWithIgnoreCase(table, "public.")) {
        table = "public." + table;
      }

      // Construct the topic name
      String topic = "default." + table;

      // Retrieve the number of partitions for the topic
      int numPartitions = getNumPartitions();

      // Check if the topic exists or create it
      existsOrCreateTopic(adminKafka, topic, numPartitions);
    }
  }

  /**
   * Generates a unique group ID for a Kafka consumer based on the provided job line.
   *
   * <p>The method constructs a group ID string starting with "etendo-ap-group".
   * If the job line or its parent job is configured to use its own consumer,
   * the group ID is appended with the job name and, if available, the action name.
   * The resulting string is converted to lowercase and sanitized by replacing
   * spaces, underscores, and periods with hyphens.
   *
   * @param jobLine
   *     The `JobLine` object containing job and action details.
   * @return A sanitized, lowercase group ID string.
   */
  private static String getGroupId(JobLine jobLine) {
    var sb = new StringBuilder();
    sb.append("etendo-ap-group");
    sb.append("-").append(jobLine.getJobsJob().getName());
    return sb.toString().toLowerCase().replace(" ", "-").replace("_", "-").replace(".", "-");
  }

  /**
   * Retrieves the number of partitions for Kafka topics from the Openbravo properties.
   *
   * <p>If the `kafka.topic.partitions` property is not defined, the default number of partitions
   * is returned. If the property is defined but contains an invalid value, a warning is logged
   * and the default value is used.
   *
   * @return The number of partitions as an integer.
   */
  private static int getNumPartitions() {
    int numPartitions = DEFAULT_KAFKA_TOPIC_PARTITIONS;
    Properties obProps = OBPropertiesProvider.getInstance().getOpenbravoProperties();
    if (!obProps.containsKey(KAFKA_TOPIC_PARTITIONS)) {
      return numPartitions;
    }
    try {
      numPartitions = Integer.parseInt(obProps.getProperty(KAFKA_TOPIC_PARTITIONS));
    } catch (NumberFormatException e) {
      log.warn("Invalid number of partitions configured, using default: {}", DEFAULT_KAFKA_TOPIC_PARTITIONS);
    }
    return numPartitions;
  }

  private void existsOrCreateTopic(AdminClient adminKafka, String topic, int numPartitions) {
    try {
      if (!adminKafka.listTopics().names().get().contains(topic)) {
        log.info("Creating topic {}", topic);
        adminKafka.createTopics(
            Collections.singletonList(new org.apache.kafka.clients.admin.NewTopic(topic, numPartitions, (short) 1)));
      } else {
        log.info("Topic {} already exists", topic);
        var topicDescription = adminKafka.describeTopics(Collections.singletonList(topic)).all().get();
        if (topicDescription.get(topic).partitions().size() < numPartitions) {
          log.info("Increasing partitions for topic {} to {}", topic, numPartitions);
          adminKafka.createPartitions(Collections.singletonMap(topic, NewPartitions.increaseTo(numPartitions)));
        } else {
          log.info("Topic {} already has sufficient partitions", topic);
        }
      }
    } catch (Exception e) {
      log.error("Error checking or creating topic {}", topic, e);
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Configures the scheduler for a specific job.
   *
   * @param job
   *     The job to configure the scheduler for
   */
  private void configureJobScheduler(Job job) {
    int threads = getJobParallelThreads(job);
    jobSchedulers.computeIfAbsent(job.getId(), k -> {
      log.info("Configuring scheduler for job {} with {} threads", job.getId(), threads);
      return Executors.newScheduledThreadPool(threads);
    });
  }


  /**
   * Gets the scheduler for a specific job.
   *
   * @param jobId
   *     ID of the job
   * @return Configured ScheduledExecutorService for the job
   */
  private ScheduledExecutorService getJobScheduler(String jobId) {
    return jobSchedulers.get(jobId);
  }

  /**
   * Retrieves the number of parallel threads configured for a job.
   *
   * @param job
   *     The job to check
   * @return Number of threads or default value
   */
  private int getJobParallelThreads(Job job) {
    try {
      String threadsStr = job.get("etapParallelThreads") != null ? job.get("etapParallelThreads").toString() : null;
      return StringUtils.isEmpty(threadsStr) ? DEFAULT_PARALLEL_THREADS : Integer.parseInt(threadsStr);
    } catch (Exception e) {
      log.warn("Error reading parallel threads configuration for job {}, using default", job.getId());
      return DEFAULT_PARALLEL_THREADS;
    }
  }

  /**
   * Retrieves full configuration for a job line.
   *
   * @param jobLine
   *     The job line
   * @return Configured AsyncProcessConfig
   */
  private AsyncProcessConfig getJobLineConfig(JobLine jobLine) {
    AsyncProcessConfig config = new AsyncProcessConfig();

    try {
      String retriesStr = jobLine.get("etapMaxRetries") != null ? jobLine.get("etapMaxRetries").toString() : null;
      config.setMaxRetries(StringUtils.isEmpty(retriesStr) ? DEFAULT_MAX_RETRIES : Integer.parseInt(retriesStr));
    } catch (Exception e) {
      log.warn("Error reading max retries for job line {}, using default", jobLine.getId());
      config.setMaxRetries(DEFAULT_MAX_RETRIES);
    }

    try {
      String delayStr = jobLine.get("etapRetryDelayMs") != null ? jobLine.get("etapRetryDelayMs").toString() : null;
      config.setRetryDelayMs(StringUtils.isEmpty(delayStr) ? DEFAULT_RETRY_DELAY : Long.parseLong(delayStr));
    } catch (Exception e) {
      log.warn("Error reading retry delay for job line {}, using default", jobLine.getId());
      config.setRetryDelayMs(DEFAULT_RETRY_DELAY);
    }

    try {
      String prefetchStr = jobLine.get("etapPrefetchCount") != null ? jobLine.get(
          "etapPrefetchCount").toString() : null;
      config.setPrefetchCount(
          StringUtils.isEmpty(prefetchStr) ? DEFAULT_PREFETCH_COUNT : Integer.parseInt(prefetchStr));
    } catch (Exception e) {
      log.warn("Error reading prefetch count for job line {}, using default", jobLine.getId());
      config.setPrefetchCount(DEFAULT_PREFETCH_COUNT);
    }

    return config;
  }

  /**
   * Checks if asynchronous jobs are enabled based on the Openbravo properties configuration.
   *
   * <p>This method retrieves the `kafka.enable` property from the Openbravo properties file
   * and compares its value to "true" (case-insensitive). If the property is not defined,
   * it defaults to "false".
   *
   * @return `true` if asynchronous jobs are enabled, otherwise `false`.
   */
  private boolean isAsyncJobsEnabled() {
    var obProps = OBPropertiesProvider.getInstance().getOpenbravoProperties();
    return propInTrue(obProps, "kafka.enable");
  }

  private AsyncProcessState convertState(String status) {
    if (StringUtils.isEmpty(status)) {
      return AsyncProcessState.STARTED;
    }
    try {
      return AsyncProcessState.valueOf(status);
    } catch (IllegalArgumentException e) {
      return AsyncProcessState.STARTED;
    }
  }


  /**
   * Method called to create a consumer based on configured actions.
   *
   * @param action
   *     Process configuration to create the consumer for
   * @return Supplier of an Action implementation
   * @throws ClassNotFoundException
   *     If the class is not found
   */
  private Supplier<Action> createActionFactory(Process action) throws ClassNotFoundException {
    var handler = OBClassLoader.getInstance().loadClass(action.getJavaClassName());
    return new NewInstance(handler);
  }

  private static class NewInstance implements Supplier<Action> {
    private final Class<?> handler;

    public NewInstance(Class<?> handler) {
      this.handler = handler;
    }

    @Override
    public Action get() {
      return (Action) WeldUtils.getInstanceFromStaticBeanManager(handler);
    }
  }

  /**
   * Calculates the error topic based on configuration.
   *
   * @param job
   *     Global job
   * @return Error topic name
   */
  private String calculateErrorTopic(Job job) {
    return StringUtils.isEmpty(job.getEtapErrortopic()) ? createTopic(job,
        DEFAULT_ERROR_SUB_TOPIC) : job.getEtapErrortopic();
  }

  /**
   * Calculates the topic to send the response to. First, checks if it is configured. If not, it is generated based on the
   * next line. If the current one is the last, the default result topic is used.
   *
   * @param jobLine
   *     Current job line
   * @param jobLines
   *     All job lines
   * @return Calculated topic name
   */
  private String calculateNextTopic(JobLine jobLine, List<JobLine> jobLines) {
    var job = jobLine.getJobsJob();
    String nextTopic = jobLine.getEtapTargettopic();
    if (StringUtils.isEmpty(nextTopic)) {
      var position = jobLines.indexOf(jobLine);
      nextTopic = createTopic(job, (position < jobLines.size() - 1) ? jobLines.get(
          position + 1).getLineNo().toString() : DEFAULT_RESULT_SUB_TOPIC);
    }
    return nextTopic;
  }

  /**
   * Calculates the topic to subscribe to.
   *
   * @param jobLine
   *     Current job line
   * @param jobLines
   *     All job lines
   * @return Calculated topic name
   */
  private String calculateCurrentTopic(JobLine jobLine, List<JobLine> jobLines) {
    var position = jobLines.indexOf(jobLine);
    var job = jobLine.getJobsJob();
    String topic = position == 0 ? job.getEtapInitialTopic() : jobLines.get(position - 1).getEtapTargettopic();
    if (StringUtils.isEmpty(topic)) {
      topic = createTopic(job, jobLine.getLineNo());
    }
    return topic;
  }

  /**
   * Creates a receiver based on advanced configuration options.
   *
   * @param topic
   *     Topic to subscribe to
   * @param isRegExp
   *     Whether the topic is a regular expression
   * @param config
   *     Configuration options
   * @param groupId
   *     Consumer group ID
   * @param kafkaHost
   *     The Kafka host URL to connect to.
   * @return Flux of ReceiverRecord instances
   */
  public Flux<ReceiverRecord<String, AsyncProcessExecution>> createReceiver(String topic, boolean isRegExp,
      AsyncProcessConfig config, String groupId, String kafkaHost) {
    Map<String, Object> props = propsToHashMap(getKafkaServerConfigProps(kafkaHost));
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AsyncProcessExecutionDeserializer.class.getName());
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.getPrefetchCount());

    var receiverOptions = ReceiverOptions.<String, AsyncProcessExecution>create(props);
    if (isRegExp) {
      receiverOptions = receiverOptions.subscription(Pattern.compile(topic));
    } else {
      receiverOptions = receiverOptions.subscription(Collections.singleton(topic));
    }
    var kafkaReceiver = new DefaultKafkaReceiver<>(ConsumerFactory.INSTANCE, receiverOptions);
    return kafkaReceiver.receive();
  }

  /**
   * Creates a Kafka sender to publish responses.
   *
   * <p>This method initializes a Kafka sender with the necessary configuration properties
   * for producing messages. It sets up the client ID, acknowledgment mode, and serializers
   * for both keys and values. The sender is configured using the provided Kafka host URL.
   *
   * @param kafkaHost
   *     The Kafka host URL to connect to.
   * @return A `KafkaSender` instance configured for publishing messages.
   */
  public KafkaSender<String, AsyncProcessExecution> crateSender(String kafkaHost) {
    Map<String, Object> propsProducer = propsToHashMap(getKafkaServerConfigProps(kafkaHost));
    // Kafka properties already include the bootstrap server
    propsProducer.put(ProducerConfig.CLIENT_ID_CONFIG, "asyncprocess-producer");
    propsProducer.put(ProducerConfig.ACKS_CONFIG, "all");
    propsProducer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    propsProducer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AsyncProcessExecutionDeserializer.class.getName());
    SenderOptions<String, AsyncProcessExecution> senderOptions = SenderOptions.create(propsProducer);

    return KafkaSender.create(senderOptions);
  }

  /**
   * Converts a `Properties` object into a `Map<String, Object>`.
   *
   * <p>This method iterates over all property names in the given `Properties` object
   * and adds each property name and its corresponding value to a `HashMap`.
   *
   * @param properties
   *     The `Properties` object to be converted.
   * @return A `Map<String, Object>` containing all the properties and their values.
   */
  private Map<String, Object> propsToHashMap(Properties properties) {
    Map<String, Object> map = new HashMap<>();
    for (String name : properties.stringPropertyNames()) {
      map.put(name, properties.getProperty(name));
    }
    return map;
  }

  /**
   * Retrieves the Kafka server configuration properties.
   *
   * <p>This method creates a `Properties` object and sets the `bootstrap.servers`
   * configuration to "localhost:9092". This configuration is used to connect to
   * the Kafka server.
   *
   * @param kafkaHost
   * @return A `Properties` object containing the Kafka server configuration.
   */
  private static Properties getKafkaServerConfigProps(String kafkaHost) {
    var props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost);
    return props;
  }

  /**
   * Retrieves the Kafka host URL based on the provided Openbravo properties.
   *
   * <p>This method checks for specific keys in the `Properties` object to determine
   * the appropriate Kafka host URL to use. The priority order is as follows:
   * 1. If the `KAFKA_URL` key exists, its value is returned.
   * 2. If the `docker_` key exists, the default Kafka URL for Tomcat in Docker is returned.
   * 3. If neither key exists, the default Kafka URL is returned.
   *
   * @param obProps
   *     The `Properties` object containing configuration values.
   * @return The Kafka host URL as a `String`.
   */
  private static String getKafkaHost(Properties obProps) {
    // Check if the `KAFKA_URL` key exists in the properties
    if (obProps.containsKey(KAFKA_URL)) {
      String kafkaUrl = obProps.getProperty(KAFKA_URL);
      log.debug("Using Kafka URL from properties: {}", kafkaUrl);
      return kafkaUrl;
    }
    // Check if the `docker_` key exists in the properties
    if (propInTrue(obProps, "docker_com.etendoerp.tomcat")) {
      log.debug("Using default Kafka URL for Tomcat in Docker: {}", DEFAULT_KAFKA_URL_TOMCAT_IN_DOCKER);
      return DEFAULT_KAFKA_URL_TOMCAT_IN_DOCKER;
    }
    // Return the default Kafka URL if no specific keys are found
    log.debug("Using default Kafka URL: {}", DEFAULT_KAFKA_URL);
    return DEFAULT_KAFKA_URL;
  }

  /**
   * Checks if a given property in the `Properties` object is set to "true".
   *
   * <p>This method verifies if the specified property key exists in the `Properties` object
   * and whether its value (case-insensitive) is equal to "true". If the property does not exist,
   * it defaults to "false".
   *
   * @param obProps
   *     The `Properties` object containing configuration values.
   * @param propKey
   *     The key of the property to check.
   * @return `true` if the property exists and its value is "true", otherwise `false`.
   */
  private static boolean propInTrue(Properties obProps, String propKey) {
    return obProps.containsKey(propKey) && StringUtils.equalsIgnoreCase(obProps.getProperty(propKey, "false"), "true");
  }

  /**
   * Enhanced shutdown method with proper cleanup of all components.
   */
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

      for (ScheduledExecutorService scheduler : jobSchedulers.values()) {
        try {
          scheduler.shutdown();
          if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
            scheduler.shutdownNow();
          }
        } catch (InterruptedException e) {
          scheduler.shutdownNow();
          Thread.currentThread().interrupt();
        }
      }
      jobSchedulers.clear();
      log.info("Enhanced AsyncProcessStartup shutdown completed");
    } catch (Exception e) {
      log.error("Error during enhanced shutdown", e);
    }
  }

  /**
   * Gets the status of the async process system.
   */
  public Map<String, Object> getSystemStatus() {
    Map<String, Object> status = new HashMap<>();
    status.put("initialized", isInitialized);
    status.put("activeJobs", jobSchedulers.size());
    status.put("activeSubscriptions", activeSubscriptions.size());
    if (healthChecker != null) {
      status.put("kafkaHealthy", healthChecker.isKafkaHealthy());
      status.put("healthReport", healthChecker.getHealthReport());
    }
    if (circuitBreaker != null) {
      status.put("circuitBreakerState", circuitBreaker.getState());
      status.put("circuitBreakerMetrics", circuitBreaker.getMetrics());
    }
    if (recoveryManager != null) status.put("recoveryStatus", recoveryManager.getRecoveryStatus());
    if (reconfigurationManager != null) status.put("configurationStatus", reconfigurationManager.getConfigurationStatus());
    if (processMonitor != null) status.put("monitoringReport", processMonitor.getStatusReport());
    return status;
  }

  /**
   * Forces a system health check and recovery if needed.
   */
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

  /**
   * Manually triggers recovery for a specific consumer.
   */
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

  /**
   * Pre-carga los action suppliers en el contexto principal.
   * Este método debe ejecutarse en el hilo principal para tener acceso al contexto de Etendo.
   */
  private Map<String, Supplier<Action>> preloadActionSuppliers() {
    final Map<String, Supplier<Action>> actionSuppliers = new HashMap<>();
    
    try {
        OBContext.setOBContext("100", "0", "0", "0");
        
        var critJob = OBDal.getInstance().createCriteria(Job.class);
        critJob.add(Restrictions.eq(Job.PROPERTY_ETAPISASYNC, true));
        List<Job> jobs = critJob.list();
        
        log.info("Pre-loading action suppliers for {} jobs...", jobs.size());
        for (Job job : jobs) {
            for (JobLine jobLine : job.getJOBSJobLineList()) {
                try {
                    Supplier<Action> supplier = createActionFactory(jobLine.getAction());
                    actionSuppliers.put(jobLine.getId(), supplier);
                } catch (ClassNotFoundException e) {
                    log.error("CRITICAL: Could not load class for job line {}. This job will not work.", 
                             jobLine.getId(), e);
                    throw new RuntimeException("Failed to pre-load action class", e);
                }
            }
        }
        log.info("Successfully pre-loaded {} action suppliers.", actionSuppliers.size());
        
    } finally {
        OBContext.restorePreviousMode();
    }
    
    return actionSuppliers;
  }

  /**
   * Carga la lista de jobs asincrónicos.
   * Este método debe ejecutarse en el hilo principal para tener acceso al contexto de Etendo.
   */
  private List<Job> loadAsyncJobs() {
    try {
        OBContext.setOBContext("100", "0", "0", "0");
        
        var critJob = OBDal.getInstance().createCriteria(Job.class);
        critJob.add(Restrictions.eq(Job.PROPERTY_ETAPISASYNC, true));
        return critJob.list();
        
    } finally {
        OBContext.restorePreviousMode();
    }
  }

  /**
   * Procesa jobs con suppliers pre-cargados.
   * Este método no requiere contexto de Etendo ya que todos los suppliers están pre-cargados.
   */
  private void processJobsWithPreloadedSuppliers(List<Job> jobs, AdminClient adminKafka, 
                                                KafkaSender<String, AsyncProcessExecution> kafkaSender, 
                                                String kafkaHost, Map<String, Supplier<Action>> actionSuppliers) {
    log.info("Found {} async jobs to start", jobs.size());
    
    Flux.fromStream(jobs.stream())
        .flatMap(job -> processJob(job, adminKafka, kafkaSender, kafkaHost, actionSuppliers))
        .collectMap(Map.Entry::getKey, Map.Entry::getValue)
        .subscribe(
            flux -> {
                log.info("Created subscribers with enhanced resilience for jobs: {}", flux.keySet());
                if (processMonitor != null) {
                    flux.keySet().forEach(jobId -> processMonitor.recordJobExecution(jobId, "Job started", 0, true, false));
                }
            },
            error -> {
                log.error("Error during job processing", error);
                handleJobProcessingError(error);
            }
        );
  }

  /**
   * Maneja errores de configuración inicial.
   */
  private void handleSetupFailure(Throwable throwable) {
    log.error("Initial setup failed: {}", throwable.getMessage(), throwable);
    
    // Notificar al monitor si está disponible
    if (processMonitor != null) {
        // Usar recordJobExecution para registrar el error de setup
        processMonitor.recordJobExecution("SYSTEM_SETUP", "Initial Setup Failed", 0, false, false);
    }
    
    // Intentar recovery si es posible
    if (recoveryManager != null && recoveryManager.isRecoveryEnabled()) {
        log.info("Attempting recovery after setup failure...");
        // El recovery manager manejará la recuperación cuando Kafka esté disponible
    }
  }

  /**
   * Ejecuta una operación preservando el contexto de clases actual.
   * Útil para operaciones que se ejecutan en threads del pool que pueden perder el contexto.
   */
  private <T> T executeWithClassLoaderContext(Supplier<T> operation) {
    final ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
    final ClassLoader applicationClassLoader = this.getClass().getClassLoader();
    
    try {
      // Asegurar que tenemos el contexto de clases correcto
      if (currentClassLoader == null || currentClassLoader != applicationClassLoader) {
        Thread.currentThread().setContextClassLoader(applicationClassLoader);
      }
      
      return operation.get();
      
    } finally {
      // Restaurar el contexto original
      Thread.currentThread().setContextClassLoader(currentClassLoader);
    }
  }
}
