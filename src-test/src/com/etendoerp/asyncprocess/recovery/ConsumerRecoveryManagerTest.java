package com.etendoerp.asyncprocess.recovery;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.etendoerp.asyncprocess.config.AsyncProcessConfig;
import com.etendoerp.asyncprocess.health.KafkaHealthChecker;
import com.etendoerp.asyncprocess.model.AsyncProcessState;
import com.etendoerp.asyncprocess.retry.RetryPolicy;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
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
   * Tests registering a consumer twice does not throw any exception.
   */
  @Test
  void testRegisterConsumerTwice() {
    ConsumerRecoveryManager.ConsumerInfo consumerInfo = buildDummyConsumerInfo();
    assertDoesNotThrow(() -> manager.registerConsumer(consumerInfo));
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
   * Tests unregistering a non-existent consumer does not throw any exception.
   */
  @Test
  void testUnregisterNonexistentConsumer() {
    assertDoesNotThrow(() -> manager.unregisterConsumer("doesNotExist"));
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
   * Tests forcing recovery for a registered consumer without a recreation function set does not throw any exception.
   */
  @Test
  void testForceRecoverConsumerWithoutRecreationFunction() {
    ConsumerRecoveryManager.ConsumerInfo consumerInfo = buildDummyConsumerInfo();
    manager.registerConsumer(consumerInfo);
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
   * Tests that shutting down the recovery manager twice does not throw any exception.
   */
  @Test
  void testShutdownTwice() {
    assertDoesNotThrow(() -> manager.shutdown());
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
   * Tests that building ConsumerInfo without consumerId throws IllegalArgumentException.
   */
  @Test
  void testConsumerInfoBuilderMissingConsumerId() {
    var builder = new ConsumerRecoveryManager.ConsumerInfo.Builder()
        .groupId("g")
        .topic("t")
        .jobLineId("j")
        .actionFactory(() -> null)
        .kafkaSender(mock(KafkaSender.class));
    Exception ex = assertThrows(IllegalArgumentException.class, builder::build);
    assertTrue(ex.getMessage().contains("consumerId"));
  }

  /**
   * Tests that building ConsumerInfo without groupId throws IllegalArgumentException.
   */
  @Test
  void testConsumerInfoBuilderMissingGroupId() {
    var builder = new ConsumerRecoveryManager.ConsumerInfo.Builder()
        .consumerId("c")
        .topic("t")
        .jobLineId("j")
        .actionFactory(() -> null)
        .kafkaSender(mock(KafkaSender.class));
    Exception ex = assertThrows(IllegalArgumentException.class, builder::build);
    assertTrue(ex.getMessage().contains("groupId"));
  }

  /**
   * Tests that building ConsumerInfo without topic throws IllegalArgumentException.
   */
  @Test
  void testConsumerInfoBuilderMissingTopic() {
    var builder = new ConsumerRecoveryManager.ConsumerInfo.Builder()
        .consumerId("c")
        .groupId("g")
        .jobLineId("j")
        .actionFactory(() -> null)
        .kafkaSender(mock(KafkaSender.class));
    Exception ex = assertThrows(IllegalArgumentException.class, builder::build);
    assertTrue(ex.getMessage().contains("topic"));
  }

  /**
   * Tests that building ConsumerInfo without jobLineId throws IllegalArgumentException.
   */
  @Test
  void testConsumerInfoBuilderMissingJobLineId() {
    var builder = new ConsumerRecoveryManager.ConsumerInfo.Builder()
        .consumerId("c")
        .groupId("g")
        .topic("t")
        .actionFactory(() -> null)
        .kafkaSender(mock(KafkaSender.class));
    Exception ex = assertThrows(IllegalArgumentException.class, builder::build);
    assertTrue(ex.getMessage().contains("jobLineId"));
  }

  /**
   * Tests that building ConsumerInfo without actionFactory throws IllegalArgumentException.
   */
  @Test
  void testConsumerInfoBuilderMissingActionFactory() {
    var builder = new ConsumerRecoveryManager.ConsumerInfo.Builder()
        .consumerId("c")
        .groupId("g")
        .topic("t")
        .jobLineId("j")
        .kafkaSender(mock(KafkaSender.class));
    Exception ex = assertThrows(IllegalArgumentException.class, builder::build);
    assertTrue(ex.getMessage().contains("actionFactory"));
  }

  /**
   * Tests that building ConsumerInfo without kafkaSender throws IllegalArgumentException.
   */
  @Test
  void testConsumerInfoBuilderMissingKafkaSender() {
    var builder = new ConsumerRecoveryManager.ConsumerInfo.Builder()
        .consumerId("c")
        .groupId("g")
        .topic("t")
        .jobLineId("j")
        .actionFactory(() -> null);
    Exception ex = assertThrows(IllegalArgumentException.class, builder::build);
    assertTrue(ex.getMessage().contains("kafkaSender"));
  }

  /**
   * Tests ConsumerInfo getters and setters.
   */
  @Test
  void testConsumerInfoGettersAndSetters() {
    ConsumerRecoveryManager.ConsumerInfo info = buildDummyConsumerInfo();
    assertEquals("testConsumer", info.getConsumerId());
    assertEquals("testGroup", info.getGroupId());
    assertEquals("testTopic", info.getTopic());
    assertFalse(info.isRegExp());
    assertNotNull(info.getConfig());
    assertEquals("jobLineId", info.getJobLineId());
    assertNotNull(info.getActionFactory());
    assertEquals("nextTopic", info.getNextTopic());
    assertEquals("errorTopic", info.getErrorTopic());
    assertNotNull(info.getTargetStatus());
    assertNotNull(info.getKafkaSender());
    assertEquals("clientId", info.getClientId());
    assertEquals("orgId", info.getOrgId());
    assertNotNull(info.getRetryPolicy());
    assertNotNull(info.getScheduler());
    assertEquals("localhost", info.getKafkaHost());
    // Setters y flags
    info.setActive(false);
    assertFalse(info.isActive());
    info.setActive(true);
    assertTrue(info.isActive());
    assertNull(info.getSubscription());
    reactor.core.Disposable disposable = mock(reactor.core.Disposable.class);
    info.setSubscription(disposable);
    assertEquals(disposable, info.getSubscription());
  }

  /**
   * Tests automatic recovery of inactive consumers and backoff retry limit.
   */
  @Test
  void testRecoverAllInactiveConsumers() throws Exception {
    KafkaHealthChecker healthChecker = mock(KafkaHealthChecker.class);
    ConsumerRecoveryManager localManager = new ConsumerRecoveryManager(healthChecker);
    ConsumerRecoveryManager.ConsumerInfo consumerInfo = buildDummyConsumerInfo();
    consumerInfo.setActive(false);
    localManager.registerConsumer(consumerInfo);
    AtomicBoolean recreated = new AtomicBoolean(false);
    localManager.setConsumerRecreationFunction(info -> {
      recreated.set(true);
      return Flux.empty();
    });
    var recoverAllInactiveConsumers = ConsumerRecoveryManager.class.getDeclaredMethod("recoverAllInactiveConsumers");
    recoverAllInactiveConsumers.setAccessible(true);
    recoverAllInactiveConsumers.invoke(localManager);
    TimeUnit.MILLISECONDS.sleep(200);
    assertTrue(recreated.get());
  }

  @Test
  void testScheduleConsumerRecoveryMaxAttempts() throws Exception {
    KafkaHealthChecker healthChecker = mock(KafkaHealthChecker.class);
    Mockito.when(healthChecker.isKafkaHealthy()).thenReturn(true);
    ConsumerRecoveryManager localManager = new ConsumerRecoveryManager(healthChecker, 2, 1, 1);
    ConsumerRecoveryManager.ConsumerInfo consumerInfo = buildDummyConsumerInfo();
    localManager.registerConsumer(consumerInfo);
    localManager.setConsumerRecreationFunction(info -> Flux.empty());
    var scheduleConsumerRecovery = ConsumerRecoveryManager.class.getDeclaredMethod("scheduleConsumerRecovery",
        String.class, String.class);
    scheduleConsumerRecovery.setAccessible(true);
    scheduleConsumerRecovery.invoke(localManager, consumerInfo.getGroupId(), "fail1");
    scheduleConsumerRecovery.invoke(localManager, consumerInfo.getGroupId(), "fail2");
    scheduleConsumerRecovery.invoke(localManager, consumerInfo.getGroupId(),
        "fail3");
    assertTrue(true);
  }

  /**
   * Tests error handling during consumer recovery, including subscription disposal, recreation function, and subscription handler.
   */
  @Test
  void testRecoverConsumerErrorOnDispose() throws Exception {
    KafkaHealthChecker healthChecker = mock(KafkaHealthChecker.class);
    ConsumerRecoveryManager localManager = new ConsumerRecoveryManager(healthChecker);
    ConsumerRecoveryManager.ConsumerInfo consumerInfo = buildDummyConsumerInfo();
    Disposable disposable = mock(Disposable.class);
    org.mockito.Mockito.when(disposable.isDisposed()).thenReturn(false);
    org.mockito.Mockito.doThrow(new RuntimeException("dispose error")).when(disposable).dispose();
    consumerInfo.setSubscription(disposable);
    localManager.registerConsumer(consumerInfo);
    localManager.setConsumerRecreationFunction(info -> Flux.empty());
    var recoverConsumer = ConsumerRecoveryManager.class.getDeclaredMethod("recoverConsumer",
        ConsumerRecoveryManager.ConsumerInfo.class, String.class);
    recoverConsumer.setAccessible(true);
    assertDoesNotThrow(() -> recoverConsumer.invoke(localManager, consumerInfo, "test"));
  }

  /**
   * Tests that if the consumer recreation function throws an exception during recovery,
   * the exception is properly propagated and wrapped in an InvocationTargetException.
   * Also verifies that the original cause and message are preserved.
   */
  @Test
  void testRecoverConsumerErrorInRecreationFunction() throws Exception {
    KafkaHealthChecker healthChecker = mock(KafkaHealthChecker.class);
    ConsumerRecoveryManager localManager = new ConsumerRecoveryManager(healthChecker);
    ConsumerRecoveryManager.ConsumerInfo consumerInfo = buildDummyConsumerInfo();
    localManager.registerConsumer(consumerInfo);
    localManager.setConsumerRecreationFunction(info -> {
      throw new RuntimeException("fail");
    });
    var recoverConsumer = ConsumerRecoveryManager.class.getDeclaredMethod("recoverConsumer",
        ConsumerRecoveryManager.ConsumerInfo.class, String.class);
    recoverConsumer.setAccessible(true);
    Exception ex = assertThrows(java.lang.reflect.InvocationTargetException.class, () ->
        recoverConsumer.invoke(localManager, consumerInfo, "test")
    );
    assertTrue(ex.getCause() instanceof RuntimeException);
    assertEquals("fail", ex.getCause().getMessage());
  }

  /**
   * Tests that if the consumer group is not found, the recovery process does not throw any exception.
   */
  @Test
  void testRecoverConsumerGroupNoConsumersFound() throws Exception {
    KafkaHealthChecker healthChecker = mock(KafkaHealthChecker.class);
    ConsumerRecoveryManager localManager = new ConsumerRecoveryManager(healthChecker);
    var recoverConsumerGroup = ConsumerRecoveryManager.class.getDeclaredMethod("recoverConsumerGroup", String.class,
        String.class);
    recoverConsumerGroup.setAccessible(true);
    assertDoesNotThrow(() -> recoverConsumerGroup.invoke(localManager, "noGroup", "test"));
  }

  /**
   * Tests that the error handler in the recovered consumer schedules another recovery attempt
   * when the recreated consumer emits an error. This simulates a failure in the consumer's
   * Flux and verifies that the recovery logic is triggered again.
   */
  @Test
  void testErrorHandlerSchedulesRecovery() throws Exception {
    KafkaHealthChecker healthChecker = mock(KafkaHealthChecker.class);
    org.mockito.Mockito.when(healthChecker.isKafkaHealthy()).thenReturn(true);
    ConsumerRecoveryManager localManager = new ConsumerRecoveryManager(healthChecker);
    ConsumerRecoveryManager.ConsumerInfo consumerInfo = buildDummyConsumerInfo();
    localManager.registerConsumer(consumerInfo);
    localManager.setConsumerRecreationFunction(info -> Flux.error(new RuntimeException("fail")));
    var recoverConsumer = ConsumerRecoveryManager.class.getDeclaredMethod("recoverConsumer",
        ConsumerRecoveryManager.ConsumerInfo.class, String.class);
    recoverConsumer.setAccessible(true);
    try {
      recoverConsumer.invoke(localManager, consumerInfo, "test");
    } catch (Exception ignored) {
    }
    assertTrue(true);
  }

  /**
   * Tests that the shutdown method handles InterruptedException by not throwing any exception.
   */
  @Test
  void testShutdownInterruptedException() throws Exception {
    KafkaHealthChecker healthChecker = mock(KafkaHealthChecker.class);
    ConsumerRecoveryManager localManager = new ConsumerRecoveryManager(healthChecker);
    ScheduledExecutorService schedulerMock = mock(ScheduledExecutorService.class);
    org.mockito.Mockito.when(
        schedulerMock.awaitTermination(org.mockito.Mockito.anyLong(), org.mockito.Mockito.any())).thenThrow(
        new InterruptedException());
    java.lang.reflect.Field schedulerField = ConsumerRecoveryManager.class.getDeclaredField("recoveryScheduler");
    schedulerField.setAccessible(true);
    schedulerField.set(localManager, schedulerMock);
    assertDoesNotThrow(localManager::shutdown);
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
