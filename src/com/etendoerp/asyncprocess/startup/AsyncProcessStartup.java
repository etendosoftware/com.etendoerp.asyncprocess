package com.etendoerp.asyncprocess.startup;

import static com.etendoerp.asyncprocess.util.TopicUtil.createTopic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import javax.enterprise.context.ApplicationScoped;

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

import com.etendoerp.asyncprocess.config.AsyncProcessConfig;
import com.etendoerp.asyncprocess.model.AsyncProcessExecution;
import com.etendoerp.asyncprocess.model.AsyncProcessState;
import com.etendoerp.asyncprocess.retry.RetryPolicy;
import com.etendoerp.asyncprocess.retry.SimpleRetryPolicy;
import com.etendoerp.asyncprocess.serdes.AsyncProcessExecutionDeserializer;
import com.etendoerp.reactor.EtendoReactorSetup;
import com.smf.jobs.Action;
import com.smf.jobs.model.Job;
import com.smf.jobs.model.JobLine;

import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.receiver.internals.ConsumerFactory;
import reactor.kafka.receiver.internals.DefaultKafkaReceiver;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

/**
 * Startup process called to initialize defined workflows with advanced configuration.
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

  // Map to maintain schedulers per job
  private final Map<String, ScheduledExecutorService> jobSchedulers = new HashMap<>();

  @Override
  public void init() {
    log.info("Etendo Reactor Startup with Advanced Configuration");
    Properties props = getKafkaServerConfigProps();
    try (AdminClient adminKafka = AdminClient.create(props)) {
      KafkaSender<String, AsyncProcessExecution> kafkaSender = crateSender();
      OBContext.setOBContext("100", "0", "0", "0");
      var critJob = OBDal.getInstance().createCriteria(Job.class);
      critJob.add(Restrictions.eq(Job.PROPERTY_ETAPISASYNC, true));
      List<Job> list = critJob.list();
      if (list.isEmpty()) {
        log.info("No async process found, reactor will not connect to any topic until restart.");
        return;
      }
      log.info("Found {} async jobs to start", list.size());
      if (!isAsyncJobsEnabled()) {
        log.warn(
            "There are async jobs defined, but the Kafka integration is disabled, so the reactor will not connect to any topic until enabled.");
        log.warn(
            "To enable async jobs, set the property 'kafka.enable' to true in gradle.properties.");
        log.warn(
            "The recommended steps are editing the gradle.properties, and then running './gradlew setup smartbuild' to update and deploy the Openbravo.properties file.");
        return;
      }
      Flux.fromStream(list.stream()).flatMap(job -> {
        // Configure or create the scheduler for this job
        configureJobScheduler(job);

        var jobLines = job.getJOBSJobLineList();
        jobLines.sort((o1, o2) -> (int) (o1.getLineNo() - o2.getLineNo()));
        return Flux.fromStream(jobLines.stream()).map(jobLine -> {
          // Get the configuration for this job line
          AsyncProcessConfig config = getJobLineConfig(job, jobLine);

          String topic = calculateCurrentTopic(jobLine, jobLines);
          // Validate if the topic exists, if not create it
          int numPartitions = getNumPartitions();
          existsOrCreateTopic(adminKafka, topic, numPartitions);

          int k = 1;
          if (jobLine.getJobsJob().isEtapConsumerPerPartition() || jobLine.isEtapConsumerPerPartition()) {
            k = numPartitions;
          }
          List<Flux<ReceiverRecord<String, AsyncProcessExecution>>> receivers = new ArrayList<>();
          try {

            for (int i = 0; i < k; i++) {
              var receiver = createReceiver(topic, job.isEtapIsregularexp(), config, getGroupId(jobLine));

              // Create retry policy based on configuration
              RetryPolicy retryPolicy = new SimpleRetryPolicy(config.getMaxRetries(), config.getRetryDelayMs());

              receiver.subscribe(
                  new ReceiverRecordConsumer(jobLine.getId() + k, createActionFactory(jobLine.getAction()),
                      calculateNextTopic(jobLine, jobLines), calculateErrorTopic(job),
                      convertState(jobLine.getEtapTargetstatus()), kafkaSender, job.getClient().getId(),
                      job.getOrganization().getId(), retryPolicy, getJobScheduler(job.getId())));

              receivers.add(receiver);
            }
          } catch (Exception e) {
            log.error("An error has occurred on job line startup {}", e.getMessage());
            e.printStackTrace();
          }
          return Map.entry(jobLine.getId(), receivers);
        });
      }).collectMap(Map.Entry::getKey, Map.Entry::getValue).subscribe(
          flux -> log.info("Created subscribers with advanced configuration {}", flux.keySet()));
    } catch (Exception e) {
      log.error("An error has occurred on reactor startup", e);
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
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(threads);
    jobSchedulers.put(job.getId(), scheduler);
    log.info("Configured scheduler for job {} with {} threads", job.getId(), threads);
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
   * @param job
   *     The parent job
   * @param jobLine
   *     The job line
   * @return Configured AsyncProcessConfig
   */
  private AsyncProcessConfig getJobLineConfig(Job job, JobLine jobLine) {
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
    var kafkaEnabled = obProps.getProperty("kafka.enable", "false");
    return StringUtils.equalsIgnoreCase(kafkaEnabled, "true");
  }

  private AsyncProcessState convertState(String status) {
    var state = AsyncProcessState.STARTED;
    if (!StringUtils.isEmpty(status)) {
      switch (status) {
        case "WAITING":
          state = AsyncProcessState.WAITING;
          break;
        case "ACCEPTED":
          state = AsyncProcessState.ACCEPTED;
          break;
        case "DONE":
          state = AsyncProcessState.DONE;
          break;
        case "REJECTED":
          state = AsyncProcessState.REJECTED;
          break;
        case "ERROR":
          state = AsyncProcessState.ERROR;
          break;
        default:
      }
    }
    return state;
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
   * @return Flux of ReceiverRecord instances
   */
  public Flux<ReceiverRecord<String, AsyncProcessExecution>> createReceiver(String topic, boolean isRegExp,
      AsyncProcessConfig config, String groupId) {
    Map<String, Object> props = propsToHashMap(getKafkaServerConfigProps());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AsyncProcessExecutionDeserializer.class.getName());

    // Configure prefetch count
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.getPrefetchCount());

    // Configure prefetch
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
   * Creates a global sender to publish responses.
   *
   * @return KafkaSender instance
   */
  public KafkaSender<String, AsyncProcessExecution> crateSender() {
    Map<String, Object> propsProducer = propsToHashMap(getKafkaServerConfigProps());
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
   * @return A `Properties` object containing the Kafka server configuration.
   */
  private static Properties getKafkaServerConfigProps() {
    var props = new Properties();
    var obProps = OBPropertiesProvider.getInstance().getOpenbravoProperties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        obProps.getProperty(KAFKA_URL, DEFAULT_KAFKA_URL));
    return props;
  }

  /**
   * Method to clean up service resources.
   */
  public void shutdown() {
    log.info("Shutting down AsyncProcessStartup...");
    for (ScheduledExecutorService scheduler : jobSchedulers.values()) {
      try {
        scheduler.shutdown();
        if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
          scheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        scheduler.shutdownNow();
      }
    }
  }
}
