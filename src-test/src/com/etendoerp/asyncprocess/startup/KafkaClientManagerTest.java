package com.etendoerp.asyncprocess.startup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.openbravo.base.session.OBPropertiesProvider;

import com.etendoerp.asyncprocess.config.AsyncProcessConfig;
import com.etendoerp.asyncprocess.model.AsyncProcessExecution;

import reactor.kafka.sender.KafkaSender;

/**
 * Unit tests for the {@link KafkaClientManager} class.
 * <p>
 * This test class verifies the Kafka client management functionality, including
 * creation of admin clients, senders, receivers, and topic management.
 */
@MockitoSettings(strictness = Strictness.LENIENT)
@ExtendWith(MockitoExtension.class)
public class KafkaClientManagerTest {

  private static final String TEST_KAFKA_HOST = "localhost:9092";
  private static final String TEST_TOPIC = "test-topic";
  private static final String TEST_GROUP_ID = "test-group";
  public static final String KAFKA_TOPIC_PARTITIONS = "kafka.topic.partitions";

  @Mock
  private AdminClient mockAdminClient;

  @Mock
  private AsyncProcessConfig mockConfig;

  @Mock
  private Properties mockProperties;

  @Mock
  private OBPropertiesProvider mockPropertiesProvider;

  private KafkaClientManager manager;
  private MockedStatic<AdminClient> mockedAdminClient;
  private MockedStatic<KafkaSender> mockedKafkaSender;
  private MockedStatic<OBPropertiesProvider> mockedOBProperties;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    mockedAdminClient = Mockito.mockStatic(AdminClient.class);
    mockedKafkaSender = Mockito.mockStatic(KafkaSender.class);
    mockedOBProperties = Mockito.mockStatic(OBPropertiesProvider.class);

    mockedAdminClient.when(() -> AdminClient.create(any(Properties.class))).thenReturn(mockAdminClient);
    mockedKafkaSender.when(() -> KafkaSender.create(any())).thenReturn(mock(KafkaSender.class));
    mockedOBProperties.when(OBPropertiesProvider::getInstance).thenReturn(mockPropertiesProvider);
    when(mockPropertiesProvider.getOpenbravoProperties()).thenReturn(mockProperties);
    when(mockProperties.getProperty(anyString())).thenReturn(null);
    when(mockProperties.getProperty(anyString(), anyString())).thenReturn("5");

    manager = new KafkaClientManager(TEST_KAFKA_HOST);
  }

  @AfterEach
  void tearDown() {
    mockedAdminClient.close();
    mockedKafkaSender.close();
    mockedOBProperties.close();
  }

  @Test
  void testConstructor() {
    assertNotNull(manager);
    assertEquals(TEST_KAFKA_HOST, manager.getKafkaHost());
  }

  @Test
  void testCreateAdminClient() {
    AdminClient adminClient = manager.createAdminClient();
    assertNotNull(adminClient);
    mockedAdminClient.verify(() -> AdminClient.create(any(Properties.class)));
  }

  @Test
  void testCreateSender() {
    KafkaSender<String, AsyncProcessExecution> sender = manager.createSender();
    assertNotNull(sender);
  }

  @Test
  void testCreateReceiver() {
    when(mockConfig.getPrefetchCount()).thenReturn(5);
    var receiver = manager.createReceiver(TEST_TOPIC, false, mockConfig, TEST_GROUP_ID);
    assertNotNull(receiver);
  }

  @Test
  void testCreateReceiverWithRegex() {
    when(mockConfig.getPrefetchCount()).thenReturn(5);
    var receiver = manager.createReceiver("test-.*", true, mockConfig, TEST_GROUP_ID);
    assertNotNull(receiver);
  }

  @Test
  void testExistsOrCreateTopic() {
    when(mockAdminClient.listTopics()).thenReturn(mock(org.apache.kafka.clients.admin.ListTopicsResult.class));
    when(mockAdminClient.listTopics().names()).thenReturn(KafkaFuture.completedFuture(Collections.emptySet()));
    when(mockAdminClient.describeTopics(any())).thenReturn(mock(org.apache.kafka.clients.admin.DescribeTopicsResult.class));
    when(mockAdminClient.describeTopics(any()).all()).thenReturn(KafkaFuture.completedFuture(
        Map.of(TEST_TOPIC, mock(org.apache.kafka.clients.admin.TopicDescription.class))));

    manager.existsOrCreateTopic(mockAdminClient, TEST_TOPIC, 5);
    verify(mockAdminClient).createTopics(any());
  }

  @Test
  void testExistsOrCreateTopicIncreasePartitions() {
    var mockTopicDescription = mock(org.apache.kafka.clients.admin.TopicDescription.class);
    when(mockTopicDescription.partitions()).thenReturn(java.util.Arrays.asList(
        mock(org.apache.kafka.common.TopicPartitionInfo.class),
        mock(org.apache.kafka.common.TopicPartitionInfo.class),
        mock(org.apache.kafka.common.TopicPartitionInfo.class)
    ));

    when(mockAdminClient.listTopics()).thenReturn(mock(org.apache.kafka.clients.admin.ListTopicsResult.class));
    when(mockAdminClient.listTopics().names()).thenReturn(KafkaFuture.completedFuture(java.util.Set.of(TEST_TOPIC)));
    when(mockAdminClient.describeTopics(any())).thenReturn(mock(org.apache.kafka.clients.admin.DescribeTopicsResult.class));
    when(mockAdminClient.describeTopics(any()).all()).thenReturn(KafkaFuture.completedFuture(
        Map.of(TEST_TOPIC, mockTopicDescription)));

    manager.existsOrCreateTopic(mockAdminClient, TEST_TOPIC, 5);
    verify(mockAdminClient).createPartitions(any());
  }

  @Test
  void testCreateKafkaConnectTopics() {
    when(mockProperties.getProperty("kafka.connect.tables", null)).thenReturn("table1,table2");
    when(mockAdminClient.listTopics()).thenReturn(mock(org.apache.kafka.clients.admin.ListTopicsResult.class));
    when(mockAdminClient.listTopics().names()).thenReturn(KafkaFuture.completedFuture(Collections.emptySet()));
    manager.createKafkaConnectTopics(mockAdminClient);
    verify(mockAdminClient, org.mockito.Mockito.times(2)).createTopics(any());
  }

  @Test
  void testCreateKafkaConnectTopicsNoTables() {
    manager.createKafkaConnectTopics(mockAdminClient);
    verify(mockAdminClient, org.mockito.Mockito.never()).createTopics(any());
  }

  @Test
  void testGetNumPartitions() {
    int partitions = manager.getNumPartitions();
    assertEquals(5, partitions);
  }

  @Test
  void testGetNumPartitionsCustom() {
    when(mockProperties.containsKey(KAFKA_TOPIC_PARTITIONS)).thenReturn(true);
    when(mockProperties.getProperty(KAFKA_TOPIC_PARTITIONS)).thenReturn("10");
    int partitions = manager.getNumPartitions();
    assertEquals(10, partitions);
  }

  @Test
  void testGetNumPartitionsInvalid() {
    when(mockProperties.getProperty(KAFKA_TOPIC_PARTITIONS)).thenReturn("invalid");
    int partitions = manager.getNumPartitions();
    assertEquals(5, partitions); // Default
  }
}
