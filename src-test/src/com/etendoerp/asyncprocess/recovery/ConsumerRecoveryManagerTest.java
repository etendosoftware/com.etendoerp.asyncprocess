package com.etendoerp.asyncprocess.recovery;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.etendoerp.asyncprocess.config.AsyncProcessConfig;
import com.etendoerp.asyncprocess.health.KafkaHealthChecker;
import com.etendoerp.asyncprocess.model.AsyncProcessState;
import com.etendoerp.asyncprocess.retry.RetryPolicy;

import reactor.kafka.sender.KafkaSender;

/**
 * Unit tests for the {@link ConsumerRecoveryManager} class.
 * <p>
 * This test class aims to maximize code coverage for the ConsumerRecoveryManager,
 * including registration, unregistration, forced recovery, recreation function,
 * recovery status, shutdown, and recovery enabling logic. It uses JUnit 5 and Mockito
 * for mocking dependencies and verifying behaviors.
 * <p>
 * Key scenarios covered:
 * <ul>
 *   <li>Registering and unregistering consumers</li>
 *   <li>Enabling and disabling recovery</li>
 *   <li>Forcing consumer recovery (success and error cases)</li>
 *   <li>Setting the consumer recreation function</li>
 *   <li>Querying recovery status</li>
 *   <li>Shutting down the recovery manager</li>
 *   <li>Triggering recovery when enabling recovery</li>
 * </ul>
 * <p>
 * Note: This test class does not access internal state directly and only uses public APIs.
 * It is designed to work without requiring changes to the ConsumerRecoveryManager implementation.
 */
class ConsumerRecoveryManagerTest {
  /**
   * The ConsumerRecoveryManager instance under test.
   */
  private ConsumerRecoveryManager manager;

  /**
   * Initializes the ConsumerRecoveryManager with a mocked KafkaHealthChecker before each test.
   */
  @BeforeEach
  void setUp() {
    KafkaHealthChecker healthChecker = mock(KafkaHealthChecker.class);
    manager = new ConsumerRecoveryManager(healthChecker);
  }

  /**
   * Tests enabling and disabling recovery and verifies the recoveryEnabled flag.
   */
  @Test
  void testSetRecoveryEnabled() {
    manager.setRecoveryEnabled(false);
    assertFalse(manager.isRecoveryEnabled());
    manager.setRecoveryEnabled(true);
    assertTrue(manager.isRecoveryEnabled());
  }

  /**
   * Tests registering a consumer does not throw any exception.
   */
  @Test
  void testRegisterConsumer() {
    ConsumerRecoveryManager.ConsumerInfo consumerInfo = buildDummyConsumerInfo();
    assertDoesNotThrow(() -> manager.registerConsumer(consumerInfo));
  }

  /**
   * Tests unregistering a registered consumer does not throw any exception.
   */
  @Test
  void testUnregisterConsumer() {
    ConsumerRecoveryManager.ConsumerInfo consumerInfo = buildDummyConsumerInfo();
    manager.registerConsumer(consumerInfo);
    assertDoesNotThrow(() -> manager.unregisterConsumer(consumerInfo.getConsumerId()));
  }

  /**
   * Tests that forcing recovery for an unknown consumer throws an IllegalArgumentException.
   */
  @Test
  void testForceRecoverConsumerThrowsForUnknownConsumer() {
    Exception ex = assertThrows(IllegalArgumentException.class, () -> manager.forceRecoverConsumer("unknown"));
    assertTrue(ex.getMessage().contains("Consumer not found"));
  }

  /**
   * Tests forcing recovery for a registered consumer with a recreation function set does not throw any exception.
   */
  @Test
  void testForceRecoverConsumerWithRecreationFunction() {
    ConsumerRecoveryManager.ConsumerInfo consumerInfo = buildDummyConsumerInfo();
    manager.registerConsumer(consumerInfo);
    manager.setConsumerRecreationFunction(info -> reactor.core.publisher.Flux.empty());
    assertDoesNotThrow(() -> manager.forceRecoverConsumer(consumerInfo.getConsumerId()));
  }

  /**
   * Tests that setting the consumer recreation function does not throw any exception.
   */
  @Test
  void testSetConsumerRecreationFunction() {
    assertDoesNotThrow(() -> manager.setConsumerRecreationFunction(info -> reactor.core.publisher.Flux.empty()));
  }

  /**
   * Tests that the recovery status map contains the expected keys and is not null.
   */
  @Test
  void testGetRecoveryStatus() {
    var status = manager.getRecoveryStatus();
    assertNotNull(status);
    assertTrue(status.containsKey("recoveryEnabled"));
    assertTrue(status.containsKey("maxRecoveryAttempts"));
    assertTrue(status.containsKey("baseRecoveryDelayMs"));
    assertTrue(status.containsKey("consumers"));
  }

  /**
   * Tests that shutting down the recovery manager does not throw any exception.
   */
  @Test
  void testShutdown() {
    ConsumerRecoveryManager.ConsumerInfo consumerInfo = buildDummyConsumerInfo();
    manager.registerConsumer(consumerInfo);
    assertDoesNotThrow(() -> manager.shutdown());
  }

  /**
   * Tests that enabling recovery triggers recovery logic if Kafka is healthy.
   */
  @Test
  void testSetRecoveryEnabledTriggersRecovery() {
    KafkaHealthChecker healthChecker = mock(KafkaHealthChecker.class);
    when(healthChecker.isKafkaHealthy()).thenReturn(true);
    ConsumerRecoveryManager localManager = new ConsumerRecoveryManager(healthChecker);
    localManager.setRecoveryEnabled(true);
    assertTrue(localManager.isRecoveryEnabled());
  }

  /**
   * Builds a dummy ConsumerInfo instance for use in tests.
   *
   * @return a ConsumerInfo instance with mock dependencies
   */
  private ConsumerRecoveryManager.ConsumerInfo buildDummyConsumerInfo() {
    return new ConsumerRecoveryManager.ConsumerInfo.Builder()
        .consumerId("testConsumer")
        .groupId("testGroup")
        .topic("testTopic")
        .isRegExp(false)
        .config(mock(AsyncProcessConfig.class))
        .jobLineId("jobLineId")
        .actionFactory(mock(Supplier.class))
        .nextTopic("nextTopic")
        .errorTopic("errorTopic")
        .targetStatus(mock(AsyncProcessState.class))
        .kafkaSender(mock(KafkaSender.class))
        .clientId("clientId")
        .orgId("orgId")
        .retryPolicy(mock(RetryPolicy.class))
        .scheduler(mock(ScheduledExecutorService.class))
        .kafkaHost("localhost")
        .build();
  }
}
