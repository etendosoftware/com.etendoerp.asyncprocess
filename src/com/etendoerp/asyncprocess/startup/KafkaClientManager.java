package com.etendoerp.asyncprocess.startup;

import com.etendoerp.asyncprocess.config.AsyncProcessConfig;
import com.etendoerp.asyncprocess.model.AsyncProcessExecution;
import com.etendoerp.asyncprocess.serdes.AsyncProcessExecutionDeserializer;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openbravo.base.session.OBPropertiesProvider;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.receiver.internals.ConsumerFactory;
import reactor.kafka.receiver.internals.DefaultKafkaReceiver;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import static com.etendoerp.asyncprocess.startup.AsyncProcessStartup.KAFKA_TOPIC_PARTITIONS;

/**
 * Manages Kafka client instances (Admin, Sender, Receiver) and topic operations.
 * This class centralizes Kafka connection and configuration logic.
 */
public class KafkaClientManager {
  private static final Logger log = LogManager.getLogger();
  private static final int DEFAULT_KAFKA_TOPIC_PARTITIONS = 5;

  private final String kafkaHost;

  public KafkaClientManager(String kafkaHost) {
    this.kafkaHost = kafkaHost;
  }

  public String getKafkaHost() {
    return this.kafkaHost;
  }

  /**
   * Creates a new Kafka AdminClient.
   * The caller is responsible for closing the client.
   * @return A new AdminClient instance.
   */
  public AdminClient createAdminClient() {
    return AdminClient.create(getKafkaServerConfigProps());
  }

  /**
   * Creates a new KafkaSender for producing messages.
   * @return A new KafkaSender instance.
   */
  public KafkaSender<String, AsyncProcessExecution> createSender() {
    Map<String, Object> propsProducer = propsToHashMap(getKafkaServerConfigProps());
    propsProducer.put(ProducerConfig.CLIENT_ID_CONFIG, "asyncprocess-producer");
    propsProducer.put(ProducerConfig.ACKS_CONFIG, "all");
    propsProducer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    propsProducer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AsyncProcessExecutionDeserializer.class.getName());
    SenderOptions<String, AsyncProcessExecution> senderOptions = SenderOptions.create(propsProducer);
    return KafkaSender.create(senderOptions);
  }

  /**
   * Creates a Kafka receiver (consumer) for a specific topic.
   */
  public Flux<ReceiverRecord<String, AsyncProcessExecution>> createReceiver(String topic, boolean isRegExp,
      AsyncProcessConfig config, String groupId) {
    Map<String, Object> props = propsToHashMap(getKafkaServerConfigProps());
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
   * Checks if a topic exists, and creates it if it doesn't.
   * Also increases partitions if the existing topic has fewer than required.
   */
  public void existsOrCreateTopic(AdminClient adminKafka, String topic, int numPartitions) {
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
   * Creates Kafka Connect topics based on the Openbravo properties.
   * @param adminKafka The AdminClient to use for topic creation.
   */
  public void createKafkaConnectTopics(AdminClient adminKafka) {
    Properties props = OBPropertiesProvider.getInstance().getOpenbravoProperties();
    var tableNames = props.getProperty("kafka.connect.tables", null);
    if (StringUtils.isEmpty(tableNames)) {
      return;
    }

    String[] tables = tableNames.split(",");
    for (String table : tables) {
      if (!StringUtils.startsWithIgnoreCase(table, "public.")) {
        table = "public." + table;
      }
      String topic = "default." + table;
      int numPartitions = getNumPartitions();
      existsOrCreateTopic(adminKafka, topic, numPartitions);
    }
  }

  /**
   * Gets the configured number of partitions for topics.
   */
  public int getNumPartitions() {
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

  /**
   * Retrieves the base Kafka server configuration properties.
   */
  private Properties getKafkaServerConfigProps() {
    var props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaHost);
    return props;
  }

  /**
   * Converts a Properties object into a Map.
   */
  private Map<String, Object> propsToHashMap(Properties properties) {
    Map<String, Object> map = new HashMap<>();
    for (String name : properties.stringPropertyNames()) {
      map.put(name, properties.getProperty(name));
    }
    return map;
  }
}
