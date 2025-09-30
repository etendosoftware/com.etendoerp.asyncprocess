package com.etendoerp.asyncprocess.health;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import com.etendoerp.asyncprocess.health.KafkaHealthChecker.ConsumerHealthListener;
import com.etendoerp.asyncprocess.health.KafkaHealthChecker.ConsumerHealthStatus;

/**
 * Unit tests for the KafkaHealthChecker class.
 * Tests the functionality related to Kafka health monitoring and consumer group status checking.
 */
@RunWith(MockitoJUnitRunner.class)
public class KafkaHealthCheckerTest {

  private static final String TEST_KAFKA_HOST = "localhost:9092";
  private static final String TEST_CONSUMER_GROUP = "etendo-ap-group-test";
  private static final long TEST_CHECK_INTERVAL = 5;
  private static final long TEST_TIMEOUT = 1000;

  @Mock
  private ScheduledExecutorService mockScheduler;

  @Mock
  private ConsumerHealthListener mockConsumerHealthListener;

  @Mock
  private AdminClient mockAdminClient;

  @Mock
  private KafkaConsumer<String, Object> mockKafkaConsumer;

  @Mock
  private ListConsumerGroupsResult mockListResult;

  @Mock
  private DescribeConsumerGroupsResult mockDescribeResult;

  @Mock
  private ConsumerGroupDescription mockGroupDescription;

  @Mock
  private KafkaFuture<ConsumerGroupDescription> mockDescriptionFuture;

  private KafkaHealthChecker healthChecker;
  private Runnable mockHealthRestoredCallback;
  private Runnable mockHealthLostCallback;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    healthChecker = new KafkaHealthChecker(TEST_KAFKA_HOST, TEST_CHECK_INTERVAL, TEST_TIMEOUT);
    
    mockHealthRestoredCallback = mock(Runnable.class);
    mockHealthLostCallback = mock(Runnable.class);
    
    healthChecker.setOnKafkaHealthRestored(mockHealthRestoredCallback);
    healthChecker.setOnKafkaHealthLost(mockHealthLostCallback);
    healthChecker.setConsumerHealthListener(mockConsumerHealthListener);
  }

  @After
  public void tearDown() {
    if (healthChecker != null) {
      healthChecker.stop();
    }
  }

  /**
   * Tests the default constructor initialization.
   */
  @Test
  public void testDefaultConstructor() {
    // GIVEN & WHEN
    KafkaHealthChecker defaultChecker = new KafkaHealthChecker(TEST_KAFKA_HOST);

    // THEN
    assertNotNull("Health checker should not be null", defaultChecker);
    assertTrue("Initial health status should be true", defaultChecker.isKafkaHealthy());
    assertTrue("Last successful check should be recent", 
        System.currentTimeMillis() - defaultChecker.getLastSuccessfulCheck() < 1000);
    
    defaultChecker.stop();
  }

  /**
   * Tests the parameterized constructor initialization.
   */
  @Test
  public void testParameterizedConstructor() {
    // GIVEN & WHEN
    KafkaHealthChecker customChecker = new KafkaHealthChecker(TEST_KAFKA_HOST, TEST_CHECK_INTERVAL, TEST_TIMEOUT);

    // THEN
    assertNotNull("Health checker should not be null", customChecker);
    assertTrue("Initial health status should be true", customChecker.isKafkaHealthy());
    
    customChecker.stop();
  }

  /**
   * Tests registering a consumer group for monitoring.
   */
  @Test
  public void testRegisterConsumerGroup() {
    // GIVEN
    String groupId = TEST_CONSUMER_GROUP;

    // WHEN
    healthChecker.registerConsumerGroup(groupId);

    // THEN
    Map<String, ConsumerHealthStatus> healthStatus = healthChecker.getConsumerHealthStatus();
    assertTrue("Consumer group should be registered", healthStatus.containsKey(groupId));
    assertTrue("Consumer group should be initially healthy", healthStatus.get(groupId).isHealthy());
    assertEquals("Group ID should match", groupId, healthStatus.get(groupId).getGroupId());
  }

  /**
   * Tests checking if a consumer group is healthy.
   */
  @Test
  public void testIsConsumerGroupHealthy() {
    // GIVEN
    String groupId = TEST_CONSUMER_GROUP;
    healthChecker.registerConsumerGroup(groupId);

    // WHEN & THEN
    assertTrue("Consumer group should be healthy initially", 
        healthChecker.isConsumerGroupHealthy(groupId));
    
    // Test with non-registered group
    assertFalse("Non-registered group should not be healthy", 
        healthChecker.isConsumerGroupHealthy("non-existent-group"));
  }

  /**
   * Tests the health report generation.
   */
  @Test
  public void testGetHealthReport() {
    // GIVEN
    healthChecker.registerConsumerGroup(TEST_CONSUMER_GROUP);

    // WHEN
    String report = healthChecker.getHealthReport();

    // THEN
    assertNotNull("Health report should not be null", report);
    assertTrue("Report should contain Kafka health info", report.contains("Kafka Healthy:"));
    assertTrue("Report should contain last check info", report.contains("Last Successful Check:"));
    assertTrue("Report should contain consumer groups info", report.contains("Consumer Groups:"));
    assertTrue("Report should contain registered group", report.contains(TEST_CONSUMER_GROUP));
    assertTrue("Report should show group as healthy", report.contains("HEALTHY"));
  }

  /**
   * Tests successful health check callback execution.
   */
  @Test
  public void testHealthRestoredCallback() throws Exception {
    // GIVEN
    KafkaHealthChecker spyChecker = spy(healthChecker);
    spyChecker.setOnKafkaHealthRestored(mockHealthRestoredCallback);
    
    // Simulate health lost first by setting internal state via reflection
    java.lang.reflect.Field healthField = KafkaHealthChecker.class.getDeclaredField("isKafkaHealthy");
    healthField.setAccessible(true);
    ((java.util.concurrent.atomic.AtomicBoolean) healthField.get(spyChecker)).set(false);

    // Use reflection to access the private method
    java.lang.reflect.Method updateMethod = KafkaHealthChecker.class.getDeclaredMethod("updateKafkaHealth", boolean.class);
    updateMethod.setAccessible(true);

    // WHEN - Simulate health restored
    updateMethod.invoke(spyChecker, true);

    // THEN
    verify(mockHealthRestoredCallback, times(1)).run();
    assertTrue("Kafka should be healthy", spyChecker.isKafkaHealthy());
  }

  /**
   * Tests health lost callback execution.
   */
  @Test
  public void testHealthLostCallback() throws Exception {
    // GIVEN
    KafkaHealthChecker spyChecker = spy(healthChecker);
    spyChecker.setOnKafkaHealthLost(mockHealthLostCallback);
    
    // Ensure initial state is healthy via reflection
    java.lang.reflect.Field healthField = KafkaHealthChecker.class.getDeclaredField("isKafkaHealthy");
    healthField.setAccessible(true);
    ((java.util.concurrent.atomic.AtomicBoolean) healthField.get(spyChecker)).set(true);

    // Use reflection to access the private method
    java.lang.reflect.Method updateMethod = KafkaHealthChecker.class.getDeclaredMethod("updateKafkaHealth", boolean.class);
    updateMethod.setAccessible(true);

    // WHEN - Simulate health lost
    updateMethod.invoke(spyChecker, false);

    // THEN
    verify(mockHealthLostCallback, times(1)).run();
    assertFalse("Kafka should be unhealthy", spyChecker.isKafkaHealthy());
  }

  /**
   * Tests callback error handling.
   */
  @Test
  public void testCallbackErrorHandling() throws Exception {
    // GIVEN
    KafkaHealthChecker spyChecker = spy(healthChecker);
    Runnable faultyCallback = mock(Runnable.class);
    doThrow(new RuntimeException("Callback error")).when(faultyCallback).run();
    spyChecker.setOnKafkaHealthRestored(faultyCallback);
    
    // Simulate health lost first via reflection
    java.lang.reflect.Field healthField = KafkaHealthChecker.class.getDeclaredField("isKafkaHealthy");
    healthField.setAccessible(true);
    ((java.util.concurrent.atomic.AtomicBoolean) healthField.get(spyChecker)).set(false);

    // Use reflection to access the private method
    java.lang.reflect.Method updateMethod = KafkaHealthChecker.class.getDeclaredMethod("updateKafkaHealth", boolean.class);
    updateMethod.setAccessible(true);

    // WHEN - Simulate health restored with faulty callback
    updateMethod.invoke(spyChecker, true);

    // THEN - Should not throw exception and should still update health
    assertTrue("Kafka should be healthy despite callback error", spyChecker.isKafkaHealthy());
    verify(faultyCallback, times(1)).run();
  }

  /**
   * Tests consumer group health check with execution exception.
   */
  @Test
  public void testCheckConsumerGroupHealthWithException() throws Exception {
    // GIVEN
    healthChecker.registerConsumerGroup(TEST_CONSUMER_GROUP);
    
    when(mockDescriptionFuture.get(anyLong(), any(TimeUnit.class))).thenThrow(new ExecutionException("Test exception", new RuntimeException()));
    when(mockDescribeResult.describedGroups()).thenReturn(Collections.singletonMap(TEST_CONSUMER_GROUP, mockDescriptionFuture));

    // WHEN
    healthChecker.checkConsumerGroupHealth(mockAdminClient, TEST_CONSUMER_GROUP);

    // THEN
    assertFalse("Consumer group should be unhealthy due to exception", healthChecker.isConsumerGroupHealthy(TEST_CONSUMER_GROUP));
    ConsumerHealthStatus status = healthChecker.getConsumerHealthStatus().get(TEST_CONSUMER_GROUP);
    assertNotNull("Status should exist", status);
    assertTrue("Last error should contain exception info", status.getLastError().contains("Check failed"));
  }

  /**
   * Tests consumer group health check with timeout exception.
   */
  @Test
  public void testCheckConsumerGroupHealthWithTimeout() throws Exception {
    // GIVEN
    healthChecker.registerConsumerGroup(TEST_CONSUMER_GROUP);
    
    when(mockDescriptionFuture.get(anyLong(), any(TimeUnit.class))).thenThrow(new TimeoutException("Timeout"));
    when(mockDescribeResult.describedGroups()).thenReturn(Collections.singletonMap(TEST_CONSUMER_GROUP, mockDescriptionFuture));

    // WHEN
    healthChecker.checkConsumerGroupHealth(mockAdminClient, TEST_CONSUMER_GROUP);

    // THEN
    assertFalse("Consumer group should be unhealthy due to timeout", healthChecker.isConsumerGroupHealthy(TEST_CONSUMER_GROUP));
    ConsumerHealthStatus status = healthChecker.getConsumerHealthStatus().get(TEST_CONSUMER_GROUP);
    assertNotNull("Status should exist", status);
    assertTrue("Last error should contain timeout info", status.getLastError().contains("Check failed"));
  }

  /**
   * Tests that callbacks are not called when health status doesn't change.
   */
  @Test
  public void testNoCallbackWhenHealthStatusUnchanged() throws Exception {
    // GIVEN
    KafkaHealthChecker spyChecker = spy(healthChecker);
    spyChecker.setOnKafkaHealthRestored(mockHealthRestoredCallback);
    spyChecker.setOnKafkaHealthLost(mockHealthLostCallback);

    // Use reflection to access the private method
    java.lang.reflect.Method updateMethod = KafkaHealthChecker.class.getDeclaredMethod("updateKafkaHealth", boolean.class);
    updateMethod.setAccessible(true);

    // WHEN - Update with same health status (initially true)
    updateMethod.invoke(spyChecker, true);

    // THEN
    verify(mockHealthRestoredCallback, never()).run();
    verify(mockHealthLostCallback, never()).run();
  }

  /**
   * Tests the stop method and scheduler shutdown.
   */
  @Test
  public void testStop() throws InterruptedException {
    // GIVEN
    KafkaHealthChecker checkerWithMockScheduler = new KafkaHealthChecker(TEST_KAFKA_HOST, TEST_CHECK_INTERVAL, TEST_TIMEOUT) {
      @Override
      public void start() {
        // Override to use mock scheduler
      }
    };
    
    // Use reflection to set the mock scheduler
    try {
      java.lang.reflect.Field schedulerField = KafkaHealthChecker.class.getDeclaredField("scheduler");
      schedulerField.setAccessible(true);
      schedulerField.set(checkerWithMockScheduler, mockScheduler);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    when(mockScheduler.awaitTermination(10, TimeUnit.SECONDS)).thenReturn(true);

    // WHEN
    checkerWithMockScheduler.stop();

    // THEN
    verify(mockScheduler, times(1)).shutdown();
    verify(mockScheduler, times(1)).awaitTermination(10, TimeUnit.SECONDS);
    verify(mockScheduler, never()).shutdownNow();
  }

  /**
   * Tests the stop method when scheduler doesn't terminate gracefully.
   */
  @Test
  public void testStopWithForcefulShutdown() throws InterruptedException {
    // GIVEN
    KafkaHealthChecker checkerWithMockScheduler = new KafkaHealthChecker(TEST_KAFKA_HOST, TEST_CHECK_INTERVAL, TEST_TIMEOUT) {
      @Override
      public void start() {
        // Override to use mock scheduler
      }
    };
    
    // Use reflection to set the mock scheduler
    try {
      java.lang.reflect.Field schedulerField = KafkaHealthChecker.class.getDeclaredField("scheduler");
      schedulerField.setAccessible(true);
      schedulerField.set(checkerWithMockScheduler, mockScheduler);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    when(mockScheduler.awaitTermination(10, TimeUnit.SECONDS)).thenReturn(false);

    // WHEN
    checkerWithMockScheduler.stop();

    // THEN
    verify(mockScheduler, times(1)).shutdown();
    verify(mockScheduler, times(1)).awaitTermination(10, TimeUnit.SECONDS);
    verify(mockScheduler, times(1)).shutdownNow();
  }

  /**
   * Tests the stop method when interrupted during shutdown.
   */
  @Test
  public void testStopWithInterruption() throws InterruptedException {
    // GIVEN
    KafkaHealthChecker checkerWithMockScheduler = new KafkaHealthChecker(TEST_KAFKA_HOST, TEST_CHECK_INTERVAL, TEST_TIMEOUT) {
      @Override
      public void start() {
        // Override to use mock scheduler
      }
    };
    
    // Use reflection to set the mock scheduler
    try {
      java.lang.reflect.Field schedulerField = KafkaHealthChecker.class.getDeclaredField("scheduler");
      schedulerField.setAccessible(true);
      schedulerField.set(checkerWithMockScheduler, mockScheduler);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    when(mockScheduler.awaitTermination(10, TimeUnit.SECONDS)).thenThrow(new InterruptedException("Test interruption"));

    // WHEN
    checkerWithMockScheduler.stop();

    // THEN
    verify(mockScheduler, times(1)).shutdown();
    verify(mockScheduler, times(1)).awaitTermination(10, TimeUnit.SECONDS);
    verify(mockScheduler, times(1)).shutdownNow();
  }
}
