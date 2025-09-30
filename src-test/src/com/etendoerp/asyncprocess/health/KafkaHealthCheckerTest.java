package com.etendoerp.asyncprocess.health;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import com.etendoerp.asyncprocess.model.AsyncProcessExecution;

/**
 * Unit tests for the {@link KafkaHealthChecker} class.
 * <p>
 * This test class verifies the health checking functionality for Kafka connectivity and consumer groups.
 * It uses Mockito to mock Kafka clients and simulate various scenarios.
 */
@MockitoSettings(strictness = Strictness.LENIENT)
@ExtendWith(MockitoExtension.class)
public class KafkaHealthCheckerTest {

  private static final String TEST_KAFKA_HOST = "localhost:9092";
  private static final String TEST_GROUP_ID = "test-group";

  @Mock
  private AdminClient mockAdminClient;

  private KafkaConsumer<String, AsyncProcessExecution> mockConsumer;
  // No longer needed: we inject the mock via subclassing the checker.

  @Mock
  private ListConsumerGroupsResult mockListResult;

  @Mock
  private DescribeConsumerGroupsResult mockDescribeResult;

  @Mock
  private ConsumerGroupDescription mockGroupDescription;

  @Mock
  private Runnable mockOnHealthRestored;

  @Mock
  private Runnable mockOnHealthLost;

  @Mock
  private KafkaHealthChecker.ConsumerHealthListener mockConsumerListener;

  private KafkaHealthChecker healthChecker;
  private MockedStatic<AdminClient> mockedAdminClient;
  private MockedConstruction<KafkaConsumer> mockedKafkaConsumer;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    mockedAdminClient = Mockito.mockStatic(AdminClient.class);
    // Instead of mocking construction, we inject a mock consumer by subclassing
    // KafkaHealthChecker and overriding the factory method createKafkaConsumer.
    mockConsumer = mock(KafkaConsumer.class);
    try {
      when(mockConsumer.listTopics(any(Duration.class))).thenReturn(Collections.emptyMap());
    } catch (Exception ignored) {
      // listTopics signature may declare checked exceptions; safe to ignore for stubbing
    }

    // Default setup for healthy scenarios
    mockedAdminClient.when(() -> AdminClient.create(any(Properties.class))).thenReturn(mockAdminClient);
    when(mockAdminClient.listConsumerGroups()).thenReturn(mockListResult);
    when(mockListResult.all()).thenReturn(KafkaFuture.completedFuture(Collections.emptyList()));
    when(mockAdminClient.describeConsumerGroups(any())).thenReturn(mockDescribeResult);
    when(mockDescribeResult.describedGroups()).thenReturn(Collections.singletonMap(TEST_GROUP_ID, KafkaFuture.completedFuture(mockGroupDescription)));
    when(mockGroupDescription.members()).thenReturn(Collections.emptyList());
    when(mockGroupDescription.state()).thenReturn(ConsumerGroupState.STABLE);

    healthChecker = new KafkaHealthChecker(TEST_KAFKA_HOST, 1, 1000) {
      @Override
      public KafkaConsumer<String, AsyncProcessExecution> createKafkaConsumer(Properties props) {
        return mockConsumer;
      }
    };
    // Call health check synchronously to ensure the mocked consumer is used.
    healthChecker.performHealthCheck();
  }

  @AfterEach
  void tearDown() {
    if (healthChecker != null) {
      healthChecker.stop();
    }
    if (mockedAdminClient != null) {
      mockedAdminClient.close();
    }
    if (mockedKafkaConsumer != null) {
      mockedKafkaConsumer.close();
    }
  }

  @Test
  void testConstructorWithDefaultValues() {
    KafkaHealthChecker checker = new KafkaHealthChecker(TEST_KAFKA_HOST);
    assertNotNull(checker);
    assertTrue(checker.isKafkaHealthy());
    checker.stop();
  }

  @Test
  void testConstructorWithCustomValues() {
    long checkInterval = 10;
    long timeout = 2000;
    KafkaHealthChecker checker = new KafkaHealthChecker(TEST_KAFKA_HOST, checkInterval, timeout);
    assertNotNull(checker);
    checker.stop();
  }

  @Test
  void testStartAndStop() {
    healthChecker.start();
    assertTrue(healthChecker.isKafkaHealthy());
    healthChecker.stop();
  }

  @Test
  void testKafkaHealthCheckSuccess() throws Exception {
    healthChecker.start();
    Thread.sleep(1500); // Wait for check to run
    assertTrue(healthChecker.isKafkaHealthy());
  verify(mockConsumer, Mockito.atLeastOnce()).listTopics(any(Duration.class));
  }

  @Test
  void testKafkaHealthCheckFailure() throws Exception {
    when(mockConsumer.listTopics(any(Duration.class))).thenThrow(new RuntimeException("Connection failed"));
    healthChecker.performHealthCheckNow();
    assertFalse(healthChecker.isKafkaHealthy());
  }

  @Test
  void testConsumerGroupHealthCheck() throws Exception {
    healthChecker.registerConsumerGroup(TEST_GROUP_ID);
    // Arrange: make the mocked group description appear healthy (non-empty members + STABLE)
    when(mockGroupDescription.members()).thenReturn(Collections.singletonList(mock(org.apache.kafka.clients.admin.MemberDescription.class)));
    when(mockGroupDescription.state()).thenReturn(ConsumerGroupState.STABLE);
    // Call the consumer group health check directly using the mocked admin client
    healthChecker.checkConsumerGroupHealth(mockAdminClient, TEST_GROUP_ID);
    assertTrue(healthChecker.isConsumerGroupHealthy(TEST_GROUP_ID));
  }

  @Test
  void testConsumerGroupUnhealthy() throws Exception {
    when(mockGroupDescription.members()).thenReturn(Collections.singletonList(mock(org.apache.kafka.clients.admin.MemberDescription.class)));
    when(mockGroupDescription.state()).thenReturn(ConsumerGroupState.DEAD);
    healthChecker.registerConsumerGroup(TEST_GROUP_ID);
    healthChecker.setConsumerHealthListener(mockConsumerListener);
    // Directly call health check to avoid scheduling/timing issues
    healthChecker.checkConsumerGroupHealth(mockAdminClient, TEST_GROUP_ID);
    assertFalse(healthChecker.isConsumerGroupHealthy(TEST_GROUP_ID));
    verify(mockConsumerListener, Mockito.atLeastOnce()).onConsumerUnhealthy(anyString(), anyString());
  }

  @Test
  void testHealthCallbacks() throws Exception {
    healthChecker.setOnKafkaHealthRestored(mockOnHealthRestored);
    healthChecker.setOnKafkaHealthLost(mockOnHealthLost);

    // First fail (synchronously)
    when(mockConsumer.listTopics(any(Duration.class))).thenThrow(new RuntimeException("Fail"));
    healthChecker.performHealthCheckNow();
    verify(mockOnHealthLost, Mockito.atLeastOnce()).run();

  // Then succeed (synchronously)
  // Reset the mock to clear previous stubbings and ensure a clean behavior
  Mockito.reset(mockConsumer);
  Mockito.doReturn(Collections.emptyMap()).when(mockConsumer).listTopics(any(Duration.class));
  // Ensure the synchronous health check reports success
  assertTrue(healthChecker.performHealthCheckNow());
  }

  @Test
  void testGetHealthReport() {
    healthChecker.registerConsumerGroup(TEST_GROUP_ID);
    String report = healthChecker.getHealthReport();
    assertNotNull(report);
    assertTrue(report.contains("Kafka Healthy"));
    assertTrue(report.contains(TEST_GROUP_ID));
  }

  @Test
  void testGetConsumerHealthStatus() {
    healthChecker.registerConsumerGroup(TEST_GROUP_ID);
    Map<String, KafkaHealthChecker.ConsumerHealthStatus> status = healthChecker.getConsumerHealthStatus();
    assertNotNull(status);
    assertTrue(status.containsKey(TEST_GROUP_ID));
  }

  @Test
  void testGetLastSuccessfulCheck() {
    long before = System.currentTimeMillis();
    // performHealthCheck executes synchronously here and updates lastSuccessfulCheck
    healthChecker.performHealthCheck();
    long after = System.currentTimeMillis();
    assertTrue(healthChecker.getLastSuccessfulCheck() >= before && healthChecker.getLastSuccessfulCheck() <= after);
  }
}