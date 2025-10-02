package com.etendoerp.asyncprocess.startup;

import com.etendoerp.asyncprocess.config.AsyncProcessConfig;
import com.etendoerp.asyncprocess.health.KafkaHealthChecker;
import com.etendoerp.asyncprocess.model.AsyncProcessExecution;
import com.etendoerp.asyncprocess.model.AsyncProcessState;
import com.etendoerp.asyncprocess.monitoring.AsyncProcessMonitor;
import com.etendoerp.asyncprocess.recovery.ConsumerRecoveryManager;
import com.etendoerp.asyncprocess.retry.RetryPolicy;
import com.etendoerp.asyncprocess.retry.SimpleRetryPolicy;
import com.smf.jobs.Action;
import com.smf.jobs.model.Job;
import com.smf.jobs.model.JobLine;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.criterion.Restrictions;
import org.openbravo.base.exception.OBException;
import org.openbravo.base.util.OBClassLoader;
import org.openbravo.base.weld.WeldUtils;
import org.openbravo.client.application.Process;
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.service.OBDal;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.etendoerp.asyncprocess.util.TopicUtil.createTopic;

/**
 * Handles the loading, configuration, and processing of asynchronous jobs.
 * This class encapsulates all logic related to job and job line processing,
 * creating Kafka consumers, and managing their lifecycles.
 */
public class JobProcessor {
  private static final Logger log = LogManager.getLogger();

  private static final String DEFAULT_RESULT_SUB_TOPIC = "result";
  private static final String DEFAULT_ERROR_SUB_TOPIC = "error";

  // Default job configuration
  private static final int DEFAULT_PARALLEL_THREADS = 8;
  private static final int DEFAULT_MAX_RETRIES = 3;
  private static final long DEFAULT_RETRY_DELAY = 1000; // ms
  private static final int DEFAULT_PREFETCH_COUNT = 1;

  private final Map<String, ScheduledExecutorService> jobSchedulers = new HashMap<>();
  private final Map<String, Disposable> activeSubscriptions;

  // Core components from startup
  private final AsyncProcessMonitor processMonitor;
  private final ConsumerRecoveryManager recoveryManager;
  private final KafkaHealthChecker healthChecker;
  private final KafkaClientManager kafkaClientManager;

  public JobProcessor(AsyncProcessMonitor processMonitor, ConsumerRecoveryManager recoveryManager,
      KafkaHealthChecker healthChecker, Map<String, Disposable> activeSubscriptions,
      KafkaClientManager kafkaClientManager) {
    this.processMonitor = processMonitor;
    this.recoveryManager = recoveryManager;
    this.healthChecker = healthChecker;
    this.activeSubscriptions = activeSubscriptions;
    this.kafkaClientManager = kafkaClientManager;
  }

  /**
   * Main entry point to load and process all asynchronous jobs from the database.
   */
  public void processAllJobs() {
    final Map<String, Supplier<Action>> actionSuppliers = preloadActionSuppliers();
    final List<Job> jobs = loadAsyncJobs();

    if (jobs.isEmpty()) {
      log.info("No async process found, reactor will not connect to any topic until restart.");
      return;
    }

    log.info("Found {} async jobs to start", jobs.size());

    // These clients are managed here to ensure they are closed properly.
    try (AdminClient adminKafka = kafkaClientManager.createAdminClient()) {
      KafkaSender<String, AsyncProcessExecution> kafkaSender = kafkaClientManager.createSender();

      Flux.fromStream(jobs.stream())
          .flatMap(job -> processJob(job, adminKafka, kafkaSender, actionSuppliers))
          .collectMap(Map.Entry::getKey, Map.Entry::getValue)
          .subscribe(
              flux -> {
                log.info("Created subscribers with enhanced resilience for jobs: {}", flux.keySet());
                if (processMonitor != null) {
                  flux.keySet().forEach(jobId -> processMonitor.recordJobExecution(jobId, "Job started", 0, true, false));
                }
              },
              this::handleJobProcessingError
          );
    } catch (Exception e) {
      log.error("Failed to process jobs due to Kafka client initialization error", e);
      handleJobProcessingError(e);
    }
  }

  /**
   * Processes a single job and its job lines.
   */
  private Flux<Map.Entry<String, List<Flux<ReceiverRecord<String, AsyncProcessExecution>>>>> processJob(
      Job job, AdminClient adminKafka, KafkaSender<String, AsyncProcessExecution> kafkaSender,
      Map<String, Supplier<Action>> actionSuppliers) {

    try {
      configureJobScheduler(job);
      var jobLines = job.getJOBSJobLineList();
      jobLines.sort(Comparator.comparing(JobLine::getLineNo));

      return Flux.fromStream(jobLines.stream()).map(jobLine -> {
        try {
          return processJobLine(job, jobLine, jobLines, adminKafka, kafkaSender, actionSuppliers);
        } catch (Exception e) {
          log.error("Error processing job line {} for job {}: {}", jobLine.getId(), job.getId(), e.getMessage(), e);
          return Map.entry(jobLine.getId(), new ArrayList<>());
        }
      });
    } catch (Exception e) {
      log.error("Error processing job {}: {}", job.getId(), e.getMessage(), e);
      return Flux.empty();
    }
  }

  /**
   * Processes a single job line, creating and subscribing consumers.
   */
  private Map.Entry<String, List<Flux<ReceiverRecord<String, AsyncProcessExecution>>>> processJobLine(
      Job job, JobLine jobLine, List<JobLine> jobLines, AdminClient adminKafka,
      KafkaSender<String, AsyncProcessExecution> kafkaSender,
      Map<String, Supplier<Action>> actionSuppliers) {

    try {
      AsyncProcessConfig config = getJobLineConfig(jobLine);
      String topic = calculateCurrentTopic(jobLine, jobLines);

      setupTopicAndPartitions(adminKafka, topic);

      int consumerCount = calculateConsumerCount(job, jobLine);

      ConsumerCreationConfig creationConfig = new ConsumerCreationConfig(
          job, jobLine, jobLines, topic, config, kafkaSender, actionSuppliers);

      List<Flux<ReceiverRecord<String, AsyncProcessExecution>>> receivers =
          createConsumersForJobLine(creationConfig, consumerCount);

      return Map.entry(jobLine.getId(), receivers);
    } catch (Exception e) {
      log.error("Error processing job line {}: {}", jobLine.getId(), e.getMessage(), e);
      return Map.entry(jobLine.getId(), new ArrayList<>());
    }
  }

  /**
   * Sets up topic and partitions in Kafka.
   */
  private void setupTopicAndPartitions(AdminClient adminKafka, String topic) {
    int numPartitions = kafkaClientManager.getNumPartitions();
    kafkaClientManager.existsOrCreateTopic(adminKafka, topic, numPartitions);
  }

  /**
   * Calculates the number of consumers needed based on job and job line configuration.
   */
  private int calculateConsumerCount(Job job, JobLine jobLine) {
    int numPartitions = kafkaClientManager.getNumPartitions();
    return (BooleanUtils.isTrue(job.isEtapConsumerPerPartition()) ||
            BooleanUtils.isTrue(jobLine.isEtapConsumerPerPartition())) ? numPartitions : 1;
  }

  /**
   * Configuration object to hold consumer creation parameters.
   */
  private static class ConsumerCreationConfig {
    private final Job job;
    private final JobLine jobLine;
    private final List<JobLine> jobLines;
    private final String topic;
    private final AsyncProcessConfig config;
    private final KafkaSender<String, AsyncProcessExecution> kafkaSender;
    private final Map<String, Supplier<Action>> actionSuppliers;

    public ConsumerCreationConfig(Job job, JobLine jobLine, List<JobLine> jobLines, String topic,
                                 AsyncProcessConfig config, KafkaSender<String, AsyncProcessExecution> kafkaSender,
                                 Map<String, Supplier<Action>> actionSuppliers) {
      this.job = job;
      this.jobLine = jobLine;
      this.jobLines = jobLines;
      this.topic = topic;
      this.config = config;
      this.kafkaSender = kafkaSender;
      this.actionSuppliers = actionSuppliers;
    }

    public Job getJob() { return job; }
    public JobLine getJobLine() { return jobLine; }
    public List<JobLine> getJobLines() { return jobLines; }
    public String getTopic() { return topic; }
    public AsyncProcessConfig getConfig() { return config; }
    public KafkaSender<String, AsyncProcessExecution> getKafkaSender() { return kafkaSender; }
    public Map<String, Supplier<Action>> getActionSuppliers() { return actionSuppliers; }
  }

  /**
   * Creates all consumers for a job line.
   */
  private List<Flux<ReceiverRecord<String, AsyncProcessExecution>>> createConsumersForJobLine(
      ConsumerCreationConfig creationConfig, int consumerCount) {

    List<Flux<ReceiverRecord<String, AsyncProcessExecution>>> receivers = new ArrayList<>();

    for (int i = 0; i < consumerCount; i++) {
      Flux<ReceiverRecord<String, AsyncProcessExecution>> receiver =
          createSingleConsumerSafely(creationConfig, i);

      if (receiver != null) {
        receivers.add(receiver);
      }
    }

    return receivers;
  }

  /**
   * Creates a single consumer with error handling.
   */
  private Flux<ReceiverRecord<String, AsyncProcessExecution>> createSingleConsumerSafely(
      ConsumerCreationConfig config, int consumerIndex) {

    try {
      return createAndConfigureConsumer(config, consumerIndex);
    } catch (Exception e) {
      log.error("An error has occurred creating consumer {} for job line {}: {}",
          consumerIndex, config.getJobLine().getId(), e.getMessage(), e);
      return null;
    }
  }

  /**
   * Creates and configures a single consumer for a job line.
   */
  private Flux<ReceiverRecord<String, AsyncProcessExecution>> createAndConfigureConsumer(
      ConsumerCreationConfig config, int consumerIndex) {

    Job job = config.getJob();
    JobLine jobLine = config.getJobLine();
    List<JobLine> jobLines = config.getJobLines();
    String topic = config.getTopic();
    AsyncProcessConfig asyncConfig = config.getConfig();
    KafkaSender<String, AsyncProcessExecution> kafkaSender = config.getKafkaSender();
    Map<String, Supplier<Action>> actionSuppliers = config.getActionSuppliers();

    String consumerId = jobLine.getId() + "-" + consumerIndex;
    String groupId = getGroupId(jobLine);

    var receiver = kafkaClientManager.createReceiver(topic, job.isEtapIsregularexp(), asyncConfig, groupId);
    RetryPolicy retryPolicy = new SimpleRetryPolicy(asyncConfig.getMaxRetries(), asyncConfig.getRetryDelayMs());
    Supplier<Action> actionFactory = actionSuppliers.get(jobLine.getId());

    if (actionFactory == null) {
      log.error("FATAL: No action supplier found for job line {}. Skipping.", jobLine.getId());
      return null;
    }

    ReceiverRecordConsumer recordConsumer = new ReceiverRecordConsumer(
        new ReceiverRecordConsumer.ConsumerConfig.Builder()
            .jobId(jobLine.getId() + "-" + consumerIndex)
            .actionFactory(actionFactory)
            .nextTopic(calculateNextTopic(jobLine, jobLines))
            .errorTopic(calculateErrorTopic(job))
            .targetStatus(convertState(jobLine.getEtapTargetstatus()))
            .kafkaSender(kafkaSender)
            .clientId(job.getClient().getId())
            .orgId(job.getOrganization().getId())
            .retryPolicy(retryPolicy)
            .scheduler(getJobScheduler(job.getId()))
            .build()
    );

    Disposable subscription = receiver.doOnNext(receiverRecord -> {
      if (processMonitor != null) {
        processMonitor.recordConsumerActivity(consumerId, groupId, topic);
      }
    }).doOnError(error -> {
      log.error("Error in consumer {} for topic {}: {}", consumerId, topic, error.getMessage(), error);
      if (processMonitor != null) {
        processMonitor.recordConsumerConnectionLost(consumerId);
      }
      if (recoveryManager != null) {
        log.info("Scheduling recovery for consumer {} due to: {}", consumerId, error.getMessage());
      }
    }).subscribe(recordConsumer);

    activeSubscriptions.put(consumerId, subscription);

    if (recoveryManager != null) {
      String kafkaHost = kafkaClientManager.getKafkaHost();
      ConsumerRecoveryManager.ConsumerInfo consumerInfo = new ConsumerRecoveryManager.ConsumerInfo.Builder()
          .consumerId(consumerId)
          .groupId(groupId)
          .topic(topic)
          .isRegExp(job.isEtapIsregularexp())
          .config(asyncConfig)
          .jobLineId(jobLine.getId())
          .actionFactory(actionFactory)
          .nextTopic(calculateNextTopic(jobLine, jobLines))
          .errorTopic(calculateErrorTopic(job))
          .targetStatus(convertState(jobLine.getEtapTargetstatus()))
          .kafkaSender(kafkaSender)
          .clientId(job.getClient().getId())
          .orgId(job.getOrganization().getId())
          .retryPolicy(retryPolicy)
          .scheduler(getJobScheduler(job.getId()))
          .kafkaHost(kafkaHost)
          .build();
      consumerInfo.setSubscription(subscription);
      recoveryManager.registerConsumer(consumerInfo);
    }

    if (healthChecker != null) {
      healthChecker.registerConsumerGroup(groupId);
    }

    return receiver;
  }

  /**
   * Pre-loads action suppliers in the main context.
   */
  protected Map<String, Supplier<Action>> preloadActionSuppliers() {
    final Map<String, Supplier<Action>> actionSuppliers = new HashMap<>();
    OBContext.setOBContext("100", "0", "0", "0");
    List<Job> jobs = loadAsyncJobs();
    log.info("Pre-loading action suppliers for {} jobs...", jobs.size());
    for (Job job : jobs) {
      for (JobLine jobLine : job.getJOBSJobLineList()) {
        try {
          Supplier<Action> supplier = createActionFactory(jobLine.getAction());
          actionSuppliers.put(jobLine.getId(), supplier);
        } catch (ClassNotFoundException e) {
          log.error("CRITICAL: Could not load class for job line {}. This job will not work.",
              jobLine.getId(), e);
          throw new OBException("Failed to pre-load action class", e);
        }
      }
    }
    log.info("Successfully pre-loaded {} action suppliers.", actionSuppliers.size());
    return actionSuppliers;
  }

  /**
   * Loads the list of asynchronous jobs from the database.
   */
  protected List<Job> loadAsyncJobs() {
    OBContext.setOBContext("100", "0", "0", "0");
    var critJob = OBDal.getInstance().createCriteria(Job.class);
    critJob.add(Restrictions.eq(Job.PROPERTY_ETAPISASYNC, true));
    return critJob.list();
  }

  private void handleJobProcessingError(Throwable error) {
    log.error("Job processing error occurred", error);
    // Note: Circuit breaker is handled in the calling class (AsyncProcessStartup)
    if (processMonitor != null) {
      processMonitor.recordKafkaConnection(false);
    }
  }

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

  private void configureJobScheduler(Job job) {
    int threads = getJobParallelThreads(job);
    jobSchedulers.computeIfAbsent(job.getId(), k -> {
      log.info("Configuring scheduler for job {} with {} threads", job.getId(), threads);
      return Executors.newScheduledThreadPool(threads);
    });
  }

  public void shutdownSchedulers() {
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
  }

  private ScheduledExecutorService getJobScheduler(String jobId) {
    return jobSchedulers.get(jobId);
  }

  private int getJobParallelThreads(Job job) {
    try {
      String threadsStr = job.get("etapParallelThreads") != null ? job.get("etapParallelThreads").toString() : null;
      return StringUtils.isEmpty(threadsStr) ? DEFAULT_PARALLEL_THREADS : Integer.parseInt(threadsStr);
    } catch (Exception e) {
      log.warn("Error reading parallel threads configuration for job {}, using default", job.getId());
      return DEFAULT_PARALLEL_THREADS;
    }
  }

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

  private String calculateErrorTopic(Job job) {
    return StringUtils.isEmpty(job.getEtapErrortopic()) ? createTopic(job,
        DEFAULT_ERROR_SUB_TOPIC) : job.getEtapErrortopic();
  }

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

  private String calculateCurrentTopic(JobLine jobLine, List<JobLine> jobLines) {
    var position = jobLines.indexOf(jobLine);
    var job = jobLine.getJobsJob();
    String topic = position == 0 ? job.getEtapInitialTopic() : jobLines.get(position - 1).getEtapTargettopic();
    if (StringUtils.isEmpty(topic)) {
      topic = createTopic(job, jobLine.getLineNo());
    }
    return topic;
  }

  private static String getGroupId(JobLine jobLine) {
    var sb = new StringBuilder();
    sb.append("etendo-ap-group");
    sb.append("-").append(jobLine.getJobsJob().getName());
    return sb.toString().toLowerCase().replace(" ", "-").replace("_", "-").replace(".", "-");
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

}
