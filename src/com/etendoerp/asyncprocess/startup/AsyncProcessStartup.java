package com.etendoerp.asyncprocess.startup;

import static com.etendoerp.asyncprocess.util.TopicUtil.createTopic;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  private KafkaSender<String, AsyncProcessExecution> kafkaSender;

  @Override
  public void init() {
    log.info("Etendo Reactor Startup");
    try {
      kafkaSender = crateSender();
      OBContext.setOBContext("100", "0", "0", "0");
      var critJob = OBDal.getInstance().createCriteria(Job.class);
      critJob.add(Restrictions.eq(Job.PROPERTY_ETAPISASYNC, true));
      Flux.fromStream(critJob.list().stream())
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
          .subscribe(flux -> {
            log.info("Created subscribers {}", flux.keySet());
          });
    } catch (Exception e) {
      log.error("An error has ocurred on reactor startup {}", e.getMessage());
      e.printStackTrace();
    }
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
   * Calculate topic to send the response
   *
   * @param jobLine
   *     Current job line
   * @param jobLines
   *     all lines
   * @return calculated topic
   */
  private String calculateNextTopic(JobLine jobLine, List<JobLine> jobLines) {
    String nextTopic;
    if (StringUtils.isEmpty(jobLine.getEtapTargettopic())) {
      return jobLines.indexOf(jobLine) == jobLines.size() - 1 ?
          createTopic(jobLine.getJobsJob(), DEFAULT_RESULT_SUB_TOPIC) :
          createTopic(jobLine.getJobsJob(), jobLine);
    } else {
      nextTopic = jobLine.getEtapTargettopic();
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
      topic = createTopic(job, jobLine);
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
