package com.etendoerp.asyncprocess.startup;

import static com.etendoerp.asyncprocess.util.TopicUtil.createTopic;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import javax.enterprise.context.ApplicationScoped;

import org.apache.commons.lang3.StringUtils;
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

import com.etendoerp.asyncprocess.model.AsyncProcessExecution;
import com.etendoerp.asyncprocess.model.AsyncProcessState;
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
 * Startup process called to initialize defined workflows
 */
@ApplicationScoped
@ComponentProvider.Qualifier(AsyncProcessStartup.ASYNC_PROCESS_STARTUP)
public class AsyncProcessStartup implements EtendoReactorSetup {
  public static final String ASYNC_PROCESS_STARTUP = "asyncProcessStartup";
  private static final Logger log = LogManager.getLogger();
  private static final String DEFAULT_RESULT_SUB_TOPIC = "result";
  private static final String DEFAULT_ERROR_SUB_TOPIC = "error";

  @Override
  public void init() {
    log.info("Etendo Reactor Startup");
    try {
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
        log.info(
            "There are async jobs defined, but the Kafka integration is disabled, so the reactor will not connect to any topic until enabled.");
        log.info(
            "To enable async jobs, set the property 'kafka.enable' to true in gradle.properties and Openbravo.properties.");
        log.info(
            "The recommended steps are editing the gradle.properties, and then running './gradlew setup smartbuild' to update and deploy the Openbravo.properties file.");
        return;
      }
      Flux.fromStream(list.stream())
          .flatMap(job -> {
            var jobLines = job.getJOBSJobLineList();
            jobLines.sort((o1, o2) -> (int) (o1.getLineNo() - o2.getLineNo()));
            return Flux.fromStream(jobLines.stream())
                .map(jobLine -> {
                  var receiver = createReceiver(
                      calculateCurrentTopic(jobLine, jobLines),
                      job.isEtapIsregularexp()
                  );
                  try {
                    receiver.subscribe(
                        new ReceiverRecordConsumer(
                            job.getId(),
                            createActionFactory(jobLine.getAction()),
                            calculateNextTopic(jobLine, jobLines),
                            calculateErrorTopic(job),
                            convertState(jobLine.getEtapTargetstatus()),
                            kafkaSender,
                            job.getClient().getId(),
                            job.getOrganization().getId()
                        )
                    );
                  } catch (Exception e) {
                    log.error("An error has ocurred on job line startup {}", e.getMessage());
                    e.printStackTrace();
                  }
                  return Map.entry(jobLine.getId(), receiver);
                });
          })
          .collectMap(Map.Entry::getKey, Map.Entry::getValue)
          .subscribe(flux -> log.info("Created subscribers {}", flux.keySet()));
    } catch (Exception e) {
      log.error("An error has ocurred on reactor startup", e);
    }
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
   * Method called to create a consumer based on configured actions
   *
   * @param action
   *     in which we need to create a consumer
   * @return a consumer called dinamically based on configuration
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
   * Create a receiver based on configuration
   *
   * @param topic
   *     topic to subscribe
   * @param isRegExp
   *     indicates if the indicated topic is a regular expression
   * @return Created receiver
   */
  public Flux<ReceiverRecord<String, AsyncProcessExecution>> createReceiver(String topic, boolean isRegExp) {
    Map<String, Object> props = new HashMap<>();

    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "etendo-ap-group");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        AsyncProcessExecutionDeserializer.class.getName());

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

}
