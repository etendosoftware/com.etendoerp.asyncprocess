package com.etendoerp.asyncprocess.monitoring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link AsyncProcessMonitor}.
 */
class AsyncProcessMonitorTest {

  private AsyncProcessMonitor monitor;

  @BeforeEach
  void setUp() {
    monitor = new AsyncProcessMonitor(100, 200); // short intervals for testing
  }

  @AfterEach
  void tearDown() {
    monitor.stop();
  }

  /**
   * Tests recording job execution and validates job metrics aggregation.
   */
  @Test
  void testRecordJobExecutionAndMetrics() throws Exception {
    monitor.recordJobExecution("job1", "Test Job", 50, true, false);
    monitor.recordJobExecution("job1", "Test Job", 100, false, true);
    Field jobMetricsField = AsyncProcessMonitor.class.getDeclaredField("jobMetrics");
    jobMetricsField.setAccessible(true);
    Map<String, AsyncProcessMonitor.JobMetrics> jobMetrics = (Map<String, AsyncProcessMonitor.JobMetrics>) jobMetricsField.get(
        monitor);
    AsyncProcessMonitor.JobMetrics metrics = jobMetrics.get("job1");
    assertNotNull(metrics);
    assertEquals(2, metrics.getMessagesProcessed());
    assertEquals(1, metrics.getMessagesSucceeded());
    assertEquals(1, metrics.getMessagesFailed());
    assertEquals(1, metrics.getMessagesRetried());
    assertEquals(150, metrics.getTotalProcessingTime());
    assertTrue(metrics.getAverageProcessingTime() > 0);
    assertEquals(50.0, metrics.getSuccessRate());
    assertEquals(50.0, metrics.getFailureRate());
    assertEquals(50.0, metrics.getRetryRate());
  }

  /**
   * Tests recording consumer activity and validates consumer metrics aggregation.
   */
  @Test
  void testRecordConsumerActivityAndMetrics() throws Exception {
    monitor.recordConsumerActivity("c1", "g1", "t1");
    monitor.recordConsumerActivity("c1", "g1", "t1");
    monitor.recordConsumerConnectionLost("c1");
    monitor.recordConsumerReconnection("c1");
    monitor.updateConsumerLag("c1", 42);
    Field consumerMetricsField = AsyncProcessMonitor.class.getDeclaredField("consumerMetrics");
    consumerMetricsField.setAccessible(true);
    Map<String, AsyncProcessMonitor.ConsumerMetrics> consumerMetrics = (Map<String, AsyncProcessMonitor.ConsumerMetrics>) consumerMetricsField.get(
        monitor);
    AsyncProcessMonitor.ConsumerMetrics metrics = consumerMetrics.get("c1");
    assertNotNull(metrics);
    assertEquals(2, metrics.getMessagesConsumed());
    assertEquals(1, metrics.getConnectionLost());
    assertEquals(1, metrics.getReconnections());
    assertEquals(42, metrics.getLag());
    assertFalse(metrics.isIdle(100000));
  }

  /**
   * Tests Kafka metrics aggregation and success/failure rates.
   */
  @Test
  void testKafkaMetrics() throws Exception {
    monitor.recordKafkaConnection(true);
    monitor.recordKafkaConnection(false);
    monitor.recordKafkaMessageSent(10, true);
    monitor.recordKafkaMessageSent(20, false);
    Field kafkaMetricsField = AsyncProcessMonitor.class.getDeclaredField("kafkaMetrics");
    kafkaMetricsField.setAccessible(true);
    AsyncProcessMonitor.KafkaMetrics metrics = (AsyncProcessMonitor.KafkaMetrics) kafkaMetricsField.get(monitor);
    assertEquals(2, metrics.getConnectionAttempts());
    assertEquals(1, metrics.getConnectionFailures());
    assertEquals(2, metrics.getMessagesSent());
    assertEquals(1, metrics.getSendFailures());
    assertTrue(metrics.getConnectionSuccessRate() < 100.0);
    assertTrue(metrics.getSendSuccessRate() < 100.0);
  }

  /**
   * Tests system metrics updates and calculations.
   */
  @Test
  void testSystemMetrics() throws Exception {
    Field systemMetricsField = AsyncProcessMonitor.class.getDeclaredField("systemMetrics");
    systemMetricsField.setAccessible(true);
    AsyncProcessMonitor.SystemMetrics sys = (AsyncProcessMonitor.SystemMetrics) systemMetricsField.get(monitor);
    sys.updateMemoryUsage(1000, 2000);
    sys.updateThreadCount(5);
    sys.updateCpuUsage(80);
    assertEquals(1000, sys.getHeapUsedBytes());
    assertEquals(2000, sys.getHeapMaxBytes());
    assertEquals(5, sys.getActiveThreads());
    assertEquals(80, sys.getCpuUsagePercent());
    assertEquals(50.0, sys.getHeapUsagePercent());
  }

  /**
   * Tests that metrics listeners are notified via the scheduler.
   * Uses Thread.sleep for scheduling; in production, use Awaitility or similar.
   */
  @Test
  void testMetricsListenerNotification() throws Exception {
    AtomicBoolean notified = new AtomicBoolean(false);
    Field metricsListenersField = AsyncProcessMonitor.class.getDeclaredField("metricsListeners");
    metricsListenersField.setAccessible(true);
    List<AsyncProcessMonitor.MetricsListener> metricsListeners = (List<AsyncProcessMonitor.MetricsListener>) metricsListenersField.get(
        monitor);
    metricsListeners.add(snapshot -> notified.set(true));
    monitor.start();
    Thread.sleep(300); // Wait for scheduler execution
    assertTrue(notified.get());
  }

  /**
   * Tests that alert listeners are notified when an alert is triggered.
   */
  @Test
  void testAlertListenerNotification() throws Exception {
    // Create an alert rule that always triggers
    AsyncProcessMonitor.AlertRule rule = new AsyncProcessMonitor.AlertRule(
        "testRule",
        AsyncProcessMonitor.AlertType.HIGH_FAILURE_RATE,
        AsyncProcessMonitor.AlertSeverity.CRITICAL,
        snap -> true,
        "Test alert!"
    );
    Field alertRulesField = AsyncProcessMonitor.class.getDeclaredField("alertRules");
    alertRulesField.setAccessible(true);
    List<AsyncProcessMonitor.AlertRule> alertRules = (List<AsyncProcessMonitor.AlertRule>) alertRulesField.get(monitor);
    alertRules.add(rule);
    AtomicBoolean alertTriggered = new AtomicBoolean(false);
    Field alertListenersField = AsyncProcessMonitor.class.getDeclaredField("alertListeners");
    alertListenersField.setAccessible(true);
    List<AsyncProcessMonitor.AlertListener> alertListeners = (List<AsyncProcessMonitor.AlertListener>) alertListenersField.get(
        monitor);
    alertListeners.add(new AsyncProcessMonitor.AlertListener() {
      @Override
      public void onAlert(AsyncProcessMonitor.Alert alert) {
        alertTriggered.set(true);
      }

      @Override
      public void onAlertResolved(AsyncProcessMonitor.Alert alert) {
        // Intentionally empty: not required for this test
      }
    });
    // Invoke the private checkAlerts method via reflection
    java.lang.reflect.Method checkAlertsMethod = AsyncProcessMonitor.class.getDeclaredMethod("checkAlerts");
    checkAlertsMethod.setAccessible(true);
    checkAlertsMethod.invoke(monitor);
    assertTrue(alertTriggered.get());
  }

  /**
   * Tests that start and stop methods are idempotent and the scheduler is properly shut down.
   */
  @Test
  void testStartStopIdempotency() throws Exception {
    monitor.start();
    monitor.start();
    monitor.stop();
    monitor.stop();
    Field monitoringSchedulerField = AsyncProcessMonitor.class.getDeclaredField("monitoringScheduler");
    monitoringSchedulerField.setAccessible(true);
    java.util.concurrent.ScheduledExecutorService monitoringScheduler = (java.util.concurrent.ScheduledExecutorService) monitoringSchedulerField.get(
        monitor);
    assertTrue(monitoringScheduler.isShutdown() || monitoringScheduler.isTerminated());
  }

  /**
   * Tests adding and removing a metrics listener.
   */
  @Test
  void testAddRemoveMetricsListener() {
    AsyncProcessMonitor.MetricsListener listener = snapshot -> {
    };
    monitor.addMetricsListener(listener);
    monitor.removeMetricsListener(listener);
  }

  /**
   * Tests adding and removing an alert listener.
   */
  @Test
  void testAddRemoveAlertListener() {
    AsyncProcessMonitor.AlertListener listener = new AsyncProcessMonitor.AlertListener() {
      public void onAlert(AsyncProcessMonitor.Alert alert) {
      }

      public void onAlertResolved(AsyncProcessMonitor.Alert alert) {
      }
    };
    monitor.addAlertListener(listener);
    monitor.removeAlertListener(listener);
  }

  /**
   * Tests that a metrics listener receives a snapshot via broadcastMetrics (reflection).
   */
  @Test
  void testSnapshotViaBroadcastMetrics() throws Exception {
    AtomicBoolean notified = new AtomicBoolean(false);
    Field metricsListenersField = AsyncProcessMonitor.class.getDeclaredField("metricsListeners");
    metricsListenersField.setAccessible(true);
    List<AsyncProcessMonitor.MetricsListener> metricsListeners = (List<AsyncProcessMonitor.MetricsListener>) metricsListenersField.get(
        monitor);
    metricsListeners.add(snapshot -> {
      assertNotNull(snapshot.getJobMetrics());
      assertNotNull(snapshot.getConsumerMetrics());
      assertNotNull(snapshot.getKafkaMetrics());
      assertNotNull(snapshot.getSystemMetrics());
      notified.set(true);
    });
    java.lang.reflect.Method broadcastMetricsMethod = AsyncProcessMonitor.class.getDeclaredMethod("broadcastMetrics");
    broadcastMetricsMethod.setAccessible(true);
    broadcastMetricsMethod.invoke(monitor);
    assertTrue(notified.get());
  }

  /**
   * Tests that no exception is thrown when there are no listeners or rules.
   */
  @Test
  void testNoListenersNoRulesSafe() throws Exception {
    // Clear listeners and rules
    Field alertRulesField = AsyncProcessMonitor.class.getDeclaredField("alertRules");
    alertRulesField.setAccessible(true);
    ((List<?>) alertRulesField.get(monitor)).clear();
    Field alertListenersField = AsyncProcessMonitor.class.getDeclaredField("alertListeners");
    alertListenersField.setAccessible(true);
    ((List<?>) alertListenersField.get(monitor)).clear();
    Field metricsListenersField = AsyncProcessMonitor.class.getDeclaredField("metricsListeners");
    metricsListenersField.setAccessible(true);
    ((List<?>) metricsListenersField.get(monitor)).clear();
    // Should not throw exception when executing checkAlerts or broadcastMetrics
    java.lang.reflect.Method checkAlertsMethod = AsyncProcessMonitor.class.getDeclaredMethod("checkAlerts");
    checkAlertsMethod.setAccessible(true);
    checkAlertsMethod.invoke(monitor);
    java.lang.reflect.Method broadcastMetricsMethod = AsyncProcessMonitor.class.getDeclaredMethod("broadcastMetrics");
    broadcastMetricsMethod.setAccessible(true);
    broadcastMetricsMethod.invoke(monitor);
  }

  /**
   * Tests the full alert state lifecycle: trigger and resolve.
   */
  @Test
  void testAlertStateTriggerAndResolve() throws Exception {
    // Rule that alternates between true and false
    AtomicBoolean trigger = new AtomicBoolean(true);
    AsyncProcessMonitor.AlertRule rule = new AsyncProcessMonitor.AlertRule(
        "toggleRule",
        AsyncProcessMonitor.AlertType.HIGH_FAILURE_RATE,
        AsyncProcessMonitor.AlertSeverity.CRITICAL,
        snap -> trigger.get(),
        "Toggle alert!"
    );
    Field alertRulesField = AsyncProcessMonitor.class.getDeclaredField("alertRules");
    alertRulesField.setAccessible(true);
    List<AsyncProcessMonitor.AlertRule> alertRules = (List<AsyncProcessMonitor.AlertRule>) alertRulesField.get(monitor);
    alertRules.add(rule);
    AtomicBoolean alertTriggered = new AtomicBoolean(false);
    AtomicBoolean alertResolved = new AtomicBoolean(false);
    Field alertListenersField = AsyncProcessMonitor.class.getDeclaredField("alertListeners");
    alertListenersField.setAccessible(true);
    List<AsyncProcessMonitor.AlertListener> alertListeners = (List<AsyncProcessMonitor.AlertListener>) alertListenersField.get(
        monitor);
    alertListeners.add(new AsyncProcessMonitor.AlertListener() {
      @Override
      public void onAlert(AsyncProcessMonitor.Alert alert) {
        alertTriggered.set(true);
      }

      @Override
      public void onAlertResolved(AsyncProcessMonitor.Alert alert) {
        alertResolved.set(true);
      }
    });
    // Trigger alert
    java.lang.reflect.Method checkAlertsMethod = AsyncProcessMonitor.class.getDeclaredMethod("checkAlerts");
    checkAlertsMethod.setAccessible(true);
    checkAlertsMethod.invoke(monitor);
    assertTrue(alertTriggered.get());
    // Resolve alert
    trigger.set(false);
    checkAlertsMethod.invoke(monitor);
    assertTrue(alertResolved.get());
  }

  /**
   * Tests that exceptions thrown by metrics listeners are caught and do not propagate.
   */
  @Test
  void testMetricsListenerExceptionIsCaught() throws Exception {
    Field metricsListenersField = AsyncProcessMonitor.class.getDeclaredField("metricsListeners");
    metricsListenersField.setAccessible(true);
    List<AsyncProcessMonitor.MetricsListener> metricsListeners = (List<AsyncProcessMonitor.MetricsListener>) metricsListenersField.get(
        monitor);
    metricsListeners.add(snapshot -> {
      throw new RuntimeException("fail");
    });
    java.lang.reflect.Method broadcastMetricsMethod = AsyncProcessMonitor.class.getDeclaredMethod("broadcastMetrics");
    broadcastMetricsMethod.setAccessible(true);
    // Should not throw exception outside the method
    broadcastMetricsMethod.invoke(monitor);
  }

  /**
   * Tests that exceptions thrown by alert listeners are caught and do not propagate.
   */
  @Test
  void testAlertListenerExceptionIsCaught() throws Exception {
    AsyncProcessMonitor.AlertRule rule = new AsyncProcessMonitor.AlertRule(
        "exRule",
        AsyncProcessMonitor.AlertType.HIGH_FAILURE_RATE,
        AsyncProcessMonitor.AlertSeverity.CRITICAL,
        snap -> true,
        "Test alert!"
    );
    Field alertRulesField = AsyncProcessMonitor.class.getDeclaredField("alertRules");
    alertRulesField.setAccessible(true);
    List<AsyncProcessMonitor.AlertRule> alertRules = (List<AsyncProcessMonitor.AlertRule>) alertRulesField.get(monitor);
    alertRules.add(rule);
    Field alertListenersField = AsyncProcessMonitor.class.getDeclaredField("alertListeners");
    alertListenersField.setAccessible(true);
    List<AsyncProcessMonitor.AlertListener> alertListeners = (List<AsyncProcessMonitor.AlertListener>) alertListenersField.get(
        monitor);
    alertListeners.add(new AsyncProcessMonitor.AlertListener() {
      public void onAlert(AsyncProcessMonitor.Alert alert) {
        throw new RuntimeException("fail");
      }

      public void onAlertResolved(AsyncProcessMonitor.Alert alert) {
      }
    });
    java.lang.reflect.Method checkAlertsMethod = AsyncProcessMonitor.class.getDeclaredMethod("checkAlerts");
    checkAlertsMethod.setAccessible(true);
    // Should not throw exception outside the method
    checkAlertsMethod.invoke(monitor);
  }

  /**
   * Tests that exceptions thrown by alert rule predicates are caught and do not propagate.
   */
  @Test
  void testAlertRulePredicateExceptionIsCaught() throws Exception {
    AsyncProcessMonitor.AlertRule rule = new AsyncProcessMonitor.AlertRule(
        "exPredRule",
        AsyncProcessMonitor.AlertType.HIGH_FAILURE_RATE,
        AsyncProcessMonitor.AlertSeverity.CRITICAL,
        snap -> {
          throw new RuntimeException("fail");
        },
        "Test alert!"
    );
    Field alertRulesField = AsyncProcessMonitor.class.getDeclaredField("alertRules");
    alertRulesField.setAccessible(true);
    List<AsyncProcessMonitor.AlertRule> alertRules = (List<AsyncProcessMonitor.AlertRule>) alertRulesField.get(monitor);
    alertRules.add(rule);
    java.lang.reflect.Method checkAlertsMethod = AsyncProcessMonitor.class.getDeclaredMethod("checkAlerts");
    checkAlertsMethod.setAccessible(true);
    // Should not throw exception outside the method
    checkAlertsMethod.invoke(monitor);
  }

  /**
   * Tests that stop() handles InterruptedException and shuts down the scheduler.
   */
  @Test
  void testStopHandlesInterruptedException() throws Exception {
    // Simulate a ScheduledExecutorService that throws InterruptedException
    Field monitoringSchedulerField = AsyncProcessMonitor.class.getDeclaredField("monitoringScheduler");
    monitoringSchedulerField.setAccessible(true);
    java.util.concurrent.ScheduledExecutorService mockScheduler = new java.util.concurrent.ScheduledThreadPoolExecutor(
        1) {
      @Override
      public boolean awaitTermination(long timeout, java.util.concurrent.TimeUnit unit) throws InterruptedException {
        throw new InterruptedException("simulated");
      }
    };
    monitoringSchedulerField.set(monitor, mockScheduler);
    monitor.stop(); // Should cover the InterruptedException catch
  }
}
