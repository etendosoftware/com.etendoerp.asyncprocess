package com.etendoerp.asyncprocess.startup;

import static com.etendoerp.asyncprocess.util.TopicUtil.createTopic;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import javax.enterprise.context.ApplicationScoped;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.criterion.Restrictions;
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
 * Startup process called to initialize defined workflows with advanced configuration
 */
@ApplicationScoped
@ComponentProvider.Qualifier(AsyncProcessStartup.ASYNC_PROCESS_STARTUP)
public class AsyncProcessStartup implements EtendoReactorSetup {
  public static final String ASYNC_PROCESS_STARTUP = "asyncProcessStartup";
  private static final Logger log = LogManager.getLogger();
  private static final String DEFAULT_RESULT_SUB_TOPIC = "result";
  private static final String DEFAULT_ERROR_SUB_TOPIC = "error";

  // Configuración por defecto
  private static final int DEFAULT_PARALLEL_THREADS = 1;
  private static final int DEFAULT_MAX_RETRIES = 3;
  private static final long DEFAULT_RETRY_DELAY = 1000; // ms
  private static final int DEFAULT_PREFETCH_COUNT = 1;

  // Mapa para mantener los schedulers por job
  private final Map<String, ScheduledExecutorService> jobSchedulers = new HashMap<>();

  @Override
  public void init() {
    log.info("Etendo Reactor Startup with Advanced Configuration");
    try {
      KafkaSender<String, AsyncProcessExecution> kafkaSender = crateSender();
      OBContext.setOBContext("100", "0", "0", "0");
      var critJob = OBDal.getInstance().createCriteria(Job.class);
      critJob.add(Restrictions.eq(Job.PROPERTY_ETAPISASYNC, true));
      Flux.fromStream(critJob.list().stream())
          .flatMap(job -> {
            // Configurar o crear el scheduler para este job
            configureJobScheduler(job);

            var jobLines = job.getJOBSJobLineList();
            jobLines.sort((o1, o2) -> (int) (o1.getLineNo() - o2.getLineNo()));
            return Flux.fromStream(jobLines.stream())
                .map(jobLine -> {
                  // Obtener la configuración para esta línea de trabajo
                  AsyncProcessConfig config = getJobLineConfig(job, jobLine);

                  var receiver = createReceiver(
                      calculateCurrentTopic(jobLine, jobLines),
                      job.isEtapIsregularexp(),
                      config
                  );
                  try {
                    // Crear política de reintentos basada en la configuración
                    RetryPolicy retryPolicy = new SimpleRetryPolicy(
                        config.getMaxRetries(),
                        config.getRetryDelayMs()
                    );

                    receiver.subscribe(
                        new ReceiverRecordConsumer(
                            job.getId(),
                            createActionFactory(jobLine.getAction()),
                            calculateNextTopic(jobLine, jobLines),
                            calculateErrorTopic(job),
                            convertState(jobLine.getEtapTargetstatus()),
                            kafkaSender,
                            job.getClient().getId(),
                            job.getOrganization().getId(),
                            retryPolicy,
                            getJobScheduler(job.getId())
                        )
                    );
                  } catch (Exception e) {
                    log.error("An error has occurred on job line startup {}", e.getMessage());
                    e.printStackTrace();
                  }
                  return Map.entry(jobLine.getId(), receiver);
                });
          })
          .collectMap(Map.Entry::getKey, Map.Entry::getValue)
          .subscribe(fluxo -> log.info("Created subscribers with advanced configuration {}", flux.keySet()));
    } catch (Exception e) {
      log.error("An error has occurred on reactor startup", e);
    }
  }

  /**
   * Configura el Scheduler para un job específico
   * @param job El job para el cual configurar el scheduler
   */
  private void configureJobScheduler(Job job) {
    int threads = getJobParallelThreads(job);
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(threads);
    jobSchedulers.put(job.getId(), scheduler);
    log.info("Configured scheduler for job {} with {} threads", job.getId(), threads);
  }

  /**
   * Obtiene el scheduler para un job específico
   * @param jobId ID del job
   * @return ScheduledExecutorService configurado para el job
   */
  private ScheduledExecutorService getJobScheduler(String jobId) {
    return jobSchedulers.get(jobId);
  }

  /**
   * Obtiene el número de hilos paralelos configurados para un job
   * @param job El job a consultar
   * @return Número de hilos configurados o el valor por defecto
   */
  private int getJobParallelThreads(Job job) {
    try {
      String threadsStr = job.get("etapParallelThreads") != null ?
          job.get("etapParallelThreads").toString() : null;
      return StringUtils.isEmpty(threadsStr) ?
          DEFAULT_PARALLEL_THREADS : Integer.parseInt(threadsStr);
    } catch (Exception e) {
      log.warn("Error reading parallel threads configuration for job {}, using default", job.getId());
      return DEFAULT_PARALLEL_THREADS;
    }
  }

  /**
   * Obtiene la configuración completa para una línea de job
   * @param job El job padre
   * @param jobLine La línea de job
   * @return Configuración para la línea de job
   */
  private AsyncProcessConfig getJobLineConfig(Job job, JobLine jobLine) {
    AsyncProcessConfig config = new AsyncProcessConfig();

    // Configurar número de reintentos
    try {
      String retriesStr = jobLine.get("etapMaxRetries") != null ?
          jobLine.get("etapMaxRetries").toString() : null;
      config.setMaxRetries(StringUtils.isEmpty(retriesStr) ?
          DEFAULT_MAX_RETRIES : Integer.parseInt(retriesStr));
    } catch (Exception e) {
      log.warn("Error reading max retries for job line {}, using default", jobLine.getId());
      config.setMaxRetries(DEFAULT_MAX_RETRIES);
    }

    // Configurar delay entre reintentos
    try {
      String delayStr = jobLine.get("etapRetryDelayMs") != null ?
          jobLine.get("etapRetryDelayMs").toString() : null;
      config.setRetryDelayMs(StringUtils.isEmpty(delayStr) ?
          DEFAULT_RETRY_DELAY : Long.parseLong(delayStr));
    } catch (Exception e) {
      log.warn("Error reading retry delay for job line {}, using default", jobLine.getId());
      config.setRetryDelayMs(DEFAULT_RETRY_DELAY);
    }

    // Configurar prefetch count (cuántos mensajes procesar a la vez)
    try {
      String prefetchStr = jobLine.get("etapPrefetchCount") != null ?
          jobLine.get("etapPrefetchCount").toString() : null;
      config.setPrefetchCount(StringUtils.isEmpty(prefetchStr) ?
          DEFAULT_PREFETCH_COUNT : Integer.parseInt(prefetchStr));
    } catch (Exception e) {
      log.warn("Error reading prefetch count for job line {}, using default", jobLine.getId());
      config.setPrefetchCount(DEFAULT_PREFETCH_COUNT);
    }

    return config;
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
   * Method called to create a consumer based on configured actions
   *
   * @param action
   *     in which we need to create a consumer
   * @return a consumer called dynamically based on configuration
   * @throws ClassNotFoundException
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
   * Calculate current topic based on configuration
   *
   * @param job
   *     Global job
   * @return error topic
   */
  private String calculateErrorTopic(Job job) {
    return StringUtils.isEmpty(job.getEtapErrortopic()) ?
        createTopic(job, DEFAULT_ERROR_SUB_TOPIC) :
        job.getEtapErrortopic();
  }

  /**
   * Calculate the subject to send the response. First, check if it is configured. If not, it is generated based on the
   * next line. If the current one is the last one, the default result topic is used.
   *
   * @param jobLine
   *     Current job line
   * @param jobLines
   *     all lines
   * @return calculated topic
   */
  private String calculateNextTopic(JobLine jobLine, List<JobLine> jobLines) {
    var job = jobLine.getJobsJob();
    String nextTopic = jobLine.getEtapTargettopic();
    if (StringUtils.isEmpty(nextTopic)) {
      var position = jobLines.indexOf(jobLine);
      nextTopic = createTopic(job,
          (position < jobLines.size() - 1) ?
              jobLines.get(position + 1).getLineNo().toString() :
              DEFAULT_RESULT_SUB_TOPIC);
    }
    return nextTopic;
  }

  /**
   * Method to calculate topic to subscribe
   *
   * @param jobLine
   *     current job line
   * @param jobLines
   *     all job lines
   * @return calculated topic
   */
  private String calculateCurrentTopic(JobLine jobLine, List<JobLine> jobLines) {
    var position = jobLines.indexOf(jobLine);
    var job = jobLine.getJobsJob();
    String topic = position == 0 ?
        job.getEtapInitialTopic() :
        jobLines.get(position - 1).getEtapTargettopic();
    if (StringUtils.isEmpty(topic)) {
      topic = createTopic(job, jobLine.getLineNo());
    }
    return topic;
  }

  /**
   * Create a receiver based on configuration with advanced options
   *
   * @param topic
   *     topic to subscribe
   * @param isRegExp
   *     indicates if the indicated topic is a regular expression
   * @param config
   *     advanced configuration options
   * @return Created receiver
   */
  public Flux<ReceiverRecord<String, AsyncProcessExecution>> createReceiver(
      String topic, boolean isRegExp, AsyncProcessConfig config) {
    Map<String, Object> props = new HashMap<>();

    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "etendo-ap-group");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        AsyncProcessExecutionDeserializer.class.getName());

    // Configurar prefetch
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
   * Create a global sender, needed to publish responses
   *
   * @return Generated sender
   */
  public KafkaSender<String, AsyncProcessExecution> crateSender() {
    Map<String, Object> propsProducer = new HashMap<>();
    propsProducer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    propsProducer.put(ProducerConfig.CLIENT_ID_CONFIG, "asyncprocess-producer");
    propsProducer.put(ProducerConfig.ACKS_CONFIG, "all");
    propsProducer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    propsProducer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AsyncProcessExecutionDeserializer.class.getName());
    SenderOptions<String, AsyncProcessExecution> senderOptions = SenderOptions.create(propsProducer);

    return KafkaSender.create(senderOptions);
  }

  /**
   * Método para limpiar los recursos del servicio
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
