package com.etendoerp.asyncprocess.monitoring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link AsyncProcessMonitor}.
 */
class AsyncProcessMonitorTest {
  private static final Logger log = LogManager.getLogger();

  public static final String METRICS_LISTENERS = "metricsListeners";
  public static final String TEST_ALERT_MESSAGE = "Test alert!";
  public static final String ALERT_RULES = "alertRules";
  public static final String ALERT_LISTENERS = "alertListeners";
  public static final String CHECK_ALERTS = "checkAlerts";
  public static final String BROADCAST_METRICS = "broadcastMetrics";
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
   * Uses CountDownLatch for deterministic waiting instead of Thread.sleep.
   */
  @Test
  void testMetricsListenerNotification() throws Exception {
    java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);
    Field metricsListenersField = AsyncProcessMonitor.class.getDeclaredField(METRICS_LISTENERS);
    metricsListenersField.setAccessible(true);
    List<AsyncProcessMonitor.MetricsListener> metricsListeners = (List<AsyncProcessMonitor.MetricsListener>) metricsListenersField.get(
        monitor);
    metricsListeners.add(snapshot -> latch.countDown());
    monitor.start();
    // Wait for the listener to be notified (max 5 seconds)
    assertTrue(latch.await(5, java.util.concurrent.TimeUnit.SECONDS), "Metrics listener should be notified within 5 seconds");
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
        snap -> true, TEST_ALERT_MESSAGE
    );
    Field alertRulesField = AsyncProcessMonitor.class.getDeclaredField(ALERT_RULES);
    alertRulesField.setAccessible(true);
    List<AsyncProcessMonitor.AlertRule> alertRules = (List<AsyncProcessMonitor.AlertRule>) alertRulesField.get(monitor);
    alertRules.add(rule);
    AtomicBoolean alertTriggered = new AtomicBoolean(false);
    Field alertListenersField = AsyncProcessMonitor.class.getDeclaredField(ALERT_LISTENERS);
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
    java.lang.reflect.Method checkAlertsMethod = AsyncProcessMonitor.class.getDeclaredMethod(
        CHECK_ALERTS);
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
  void testAddRemoveMetricsListener() throws Exception {
    AsyncProcessMonitor.MetricsListener listener = snapshot -> {
    };

    Field metricsListenersField = AsyncProcessMonitor.class.getDeclaredField(METRICS_LISTENERS);
    metricsListenersField.setAccessible(true);
    List<AsyncProcessMonitor.MetricsListener> metricsListeners = (List<AsyncProcessMonitor.MetricsListener>) metricsListenersField.get(monitor);

    int initialSize = metricsListeners.size();
    monitor.addMetricsListener(listener);
    assertEquals(initialSize + 1, metricsListeners.size(), "Listener should be added");

    monitor.removeMetricsListener(listener);
    assertEquals(initialSize, metricsListeners.size(), "Listener should be removed");
  }

  /**
   * Tests adding and removing an alert listener.
   */
  @Test
  void testAddRemoveAlertListener() throws Exception {
    AsyncProcessMonitor.AlertListener listener = new AsyncProcessMonitor.AlertListener() {
      public void onAlert(AsyncProcessMonitor.Alert alert) {
        log.info("Alert received: {}", alert.getMessage());
      }

      public void onAlertResolved(AsyncProcessMonitor.Alert alert) {
        log.info("Alert resolved: {}", alert.getMessage());
      }
    };

    Field alertListenersField = AsyncProcessMonitor.class.getDeclaredField(ALERT_LISTENERS);
    alertListenersField.setAccessible(true);
    List<AsyncProcessMonitor.AlertListener> alertListeners = (List<AsyncProcessMonitor.AlertListener>) alertListenersField.get(monitor);

    int initialSize = alertListeners.size();
    monitor.addAlertListener(listener);
    assertEquals(initialSize + 1, alertListeners.size(), "Alert listener should be added");

    monitor.removeAlertListener(listener);
    assertEquals(initialSize, alertListeners.size(), "Alert listener should be removed");
  }

  /**
   * Tests that a metrics listener receives a snapshot via broadcastMetrics (reflection).
   */
  @Test
  void testSnapshotViaBroadcastMetrics() throws Exception {
    AtomicBoolean notified = new AtomicBoolean(false);
    Field metricsListenersField = AsyncProcessMonitor.class.getDeclaredField(METRICS_LISTENERS);
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
    java.lang.reflect.Method broadcastMetricsMethod = AsyncProcessMonitor.class.getDeclaredMethod(
        BROADCAST_METRICS);
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
    Field alertRulesField = AsyncProcessMonitor.class.getDeclaredField(ALERT_RULES);
    alertRulesField.setAccessible(true);
    List<?> alertRules = (List<?>) alertRulesField.get(monitor);
    alertRules.clear();

    Field alertListenersField = AsyncProcessMonitor.class.getDeclaredField(ALERT_LISTENERS);
    alertListenersField.setAccessible(true);
    List<?> alertListeners = (List<?>) alertListenersField.get(monitor);
    alertListeners.clear();

    Field metricsListenersField = AsyncProcessMonitor.class.getDeclaredField(METRICS_LISTENERS);
    metricsListenersField.setAccessible(true);
    List<?> metricsListeners = (List<?>) metricsListenersField.get(monitor);
    metricsListeners.clear();

    // Verify lists are empty
    assertTrue(alertRules.isEmpty(), "Alert rules should be empty");
    assertTrue(alertListeners.isEmpty(), "Alert listeners should be empty");
    assertTrue(metricsListeners.isEmpty(), "Metrics listeners should be empty");

    // Should not throw exception when executing checkAlerts or broadcastMetrics
    java.lang.reflect.Method checkAlertsMethod = AsyncProcessMonitor.class.getDeclaredMethod(
        CHECK_ALERTS);
    checkAlertsMethod.setAccessible(true);
    checkAlertsMethod.invoke(monitor);
    java.lang.reflect.Method broadcastMetricsMethod = AsyncProcessMonitor.class.getDeclaredMethod(
        BROADCAST_METRICS);
    broadcastMetricsMethod.setAccessible(true);
    broadcastMetricsMethod.invoke(monitor);

    // If we reach here without exception, the test passes
    assertTrue(true, "Methods executed without throwing exceptions");
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
    Field alertRulesField = AsyncProcessMonitor.class.getDeclaredField(ALERT_RULES);
    alertRulesField.setAccessible(true);
    List<AsyncProcessMonitor.AlertRule> alertRules = (List<AsyncProcessMonitor.AlertRule>) alertRulesField.get(monitor);
    alertRules.add(rule);
    AtomicBoolean alertTriggered = new AtomicBoolean(false);
    AtomicBoolean alertResolved = new AtomicBoolean(false);
    Field alertListenersField = AsyncProcessMonitor.class.getDeclaredField(ALERT_LISTENERS);
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
    java.lang.reflect.Method checkAlertsMethod = AsyncProcessMonitor.class.getDeclaredMethod(
        CHECK_ALERTS);
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
    Field metricsListenersField = AsyncProcessMonitor.class.getDeclaredField(METRICS_LISTENERS);
    metricsListenersField.setAccessible(true);
    List<AsyncProcessMonitor.MetricsListener> metricsListeners = (List<AsyncProcessMonitor.MetricsListener>) metricsListenersField.get(
        monitor);
    metricsListeners.add(snapshot -> {
      throw new RuntimeException("fail");
    });
    java.lang.reflect.Method broadcastMetricsMethod = AsyncProcessMonitor.class.getDeclaredMethod(
        BROADCAST_METRICS);
    broadcastMetricsMethod.setAccessible(true);

    // Should not throw exception outside the method
    try {
      broadcastMetricsMethod.invoke(monitor);
      // If we reach here, the exception was caught and handled properly
      assertTrue(true, "Exception was caught and handled properly");
    } catch (Exception e) {
      // If an exception propagates, the test should fail
      assertTrue(false, "Exception should have been caught but was propagated: " + e.getMessage());
    }
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
        snap -> true, TEST_ALERT_MESSAGE
    );
    Field alertRulesField = AsyncProcessMonitor.class.getDeclaredField(ALERT_RULES);
    alertRulesField.setAccessible(true);
    List<AsyncProcessMonitor.AlertRule> alertRules = (List<AsyncProcessMonitor.AlertRule>) alertRulesField.get(monitor);
    alertRules.add(rule);
    Field alertListenersField = AsyncProcessMonitor.class.getDeclaredField(ALERT_LISTENERS);
    alertListenersField.setAccessible(true);
    List<AsyncProcessMonitor.AlertListener> alertListeners = (List<AsyncProcessMonitor.AlertListener>) alertListenersField.get(
        monitor);
    alertListeners.add(new AsyncProcessMonitor.AlertListener() {
      public void onAlert(AsyncProcessMonitor.Alert alert) {
        throw new RuntimeException("fail");
      }

      public void onAlertResolved(AsyncProcessMonitor.Alert alert) {
        log.info("Alert resolved: {}", alert.getMessage());
      }
    });
    java.lang.reflect.Method checkAlertsMethod = AsyncProcessMonitor.class.getDeclaredMethod(
        CHECK_ALERTS);
    checkAlertsMethod.setAccessible(true);

    // Should not throw exception outside the method
    try {
      checkAlertsMethod.invoke(monitor);
      // If we reach here, the exception was caught and handled properly
      assertTrue(true, "Alert listener exception was caught and handled properly");
    } catch (Exception e) {
      // If an exception propagates, the test should fail
      assertTrue(false, "Alert listener exception should have been caught but was propagated: " + e.getMessage());
    }
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
        }, TEST_ALERT_MESSAGE
    );
    Field alertRulesField = AsyncProcessMonitor.class.getDeclaredField(ALERT_RULES);
    alertRulesField.setAccessible(true);
    List<AsyncProcessMonitor.AlertRule> alertRules = (List<AsyncProcessMonitor.AlertRule>) alertRulesField.get(monitor);
    alertRules.add(rule);
    java.lang.reflect.Method checkAlertsMethod = AsyncProcessMonitor.class.getDeclaredMethod(
        CHECK_ALERTS);
    checkAlertsMethod.setAccessible(true);

    // Should not throw exception outside the method
    try {
      checkAlertsMethod.invoke(monitor);
      // If we reach here, the exception was caught and handled properly
      assertTrue(true, "Alert rule predicate exception was caught and handled properly");
    } catch (Exception e) {
      // If an exception propagates, the test should fail
      assertTrue(false, "Alert rule predicate exception should have been caught but was propagated: " + e.getMessage());
    }
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

    // Clear the interrupted status before the test
    Thread.interrupted();

    monitor.stop(); // Should cover the InterruptedException catch

    // Verify that the scheduler is shut down
    assertTrue(mockScheduler.isShutdown(), "Scheduler should be shut down");

    // Verify that the interrupted status is restored
    assertTrue(Thread.currentThread().isInterrupted(), "Thread interrupted status should be restored");
  }

  /**
   * Triggers multiple default alert rules to increase coverage of rule predicates.
   */
  @Test
  void testDefaultAlertRulesTriggered() throws Exception {
    // Slow processing time (>30s average)
    monitor.recordJobExecution("jobSlow", "Slow Job", 31000, true, false);

    // Consumer lag (>1000)
    monitor.recordConsumerActivity("cLag", "g1", "t1");
    monitor.updateConsumerLag("cLag", 2001);

    // Idle consumer (set lastActivity far in past >5 min)
    monitor.recordConsumerActivity("cIdle", "g1", "t1");
    Field consumerMetricsField = AsyncProcessMonitor.class.getDeclaredField("consumerMetrics");
    consumerMetricsField.setAccessible(true);
    Map<String, AsyncProcessMonitor.ConsumerMetrics> consumerMetrics = (Map<String, AsyncProcessMonitor.ConsumerMetrics>) consumerMetricsField.get(monitor);
    AsyncProcessMonitor.ConsumerMetrics idle = consumerMetrics.get("cIdle");
    Field lastActivityField = AsyncProcessMonitor.ConsumerMetrics.class.getDeclaredField("lastActivity");
    lastActivityField.setAccessible(true);
    java.util.concurrent.atomic.AtomicLong lastActivity = (java.util.concurrent.atomic.AtomicLong) lastActivityField.get(idle);
    lastActivity.set(System.currentTimeMillis() - 301_000); // >300000 threshold

    // Kafka connection issues (success rate < 90%)
    monitor.recordKafkaConnection(false); // failure
    monitor.recordKafkaConnection(true);
    monitor.recordKafkaConnection(true);
    monitor.recordKafkaConnection(true);
    monitor.recordKafkaConnection(true); // 5 attempts, 1 failure => 80%

    // High memory usage (>80%)
    Field systemMetricsField = AsyncProcessMonitor.class.getDeclaredField("systemMetrics");
    systemMetricsField.setAccessible(true);
    AsyncProcessMonitor.SystemMetrics sys = (AsyncProcessMonitor.SystemMetrics) systemMetricsField.get(monitor);
    sys.updateMemoryUsage(90, 100); // 90%

    AtomicBoolean anyAlert = new AtomicBoolean(false);
    Field alertListenersField = AsyncProcessMonitor.class.getDeclaredField(ALERT_LISTENERS);
    alertListenersField.setAccessible(true);
    List<AsyncProcessMonitor.AlertListener> alertListeners = (List<AsyncProcessMonitor.AlertListener>) alertListenersField.get(monitor);
    final int[] triggeredCount = {0};
    alertListeners.add(new AsyncProcessMonitor.AlertListener() {
      public void onAlert(AsyncProcessMonitor.Alert alert) { anyAlert.set(true); triggeredCount[0]++; }
      public void onAlertResolved(AsyncProcessMonitor.Alert alert) { }
    });

    java.lang.reflect.Method checkAlertsMethod = AsyncProcessMonitor.class.getDeclaredMethod(CHECK_ALERTS);
    checkAlertsMethod.setAccessible(true);
    checkAlertsMethod.invoke(monitor);

    // Expect at least 5 default alerts (all except high failure rate)
    assertTrue(anyAlert.get(), "At least one default alert should trigger");
    assertTrue(triggeredCount[0] >= 5, "Expected >=5 alerts triggered, got " + triggeredCount[0]);
  }

  /**
   * Ensures removeAlertRule also clears internal alert state.
   */
  @Test
  void testRemoveAlertRuleClearsState() throws Exception {
    AsyncProcessMonitor.AlertRule temp = new AsyncProcessMonitor.AlertRule(
        "temp-remove", AsyncProcessMonitor.AlertType.HIGH_FAILURE_RATE,
        AsyncProcessMonitor.AlertSeverity.WARNING, snap -> true, "Temp rule");
    monitor.addAlertRule(temp);

    // Trigger once to create state
    java.lang.reflect.Method checkAlertsMethod = AsyncProcessMonitor.class.getDeclaredMethod(CHECK_ALERTS);
    checkAlertsMethod.setAccessible(true);
    checkAlertsMethod.invoke(monitor);

    Field alertStatesField = AsyncProcessMonitor.class.getDeclaredField("alertStates");
    alertStatesField.setAccessible(true);
    Map<String, AsyncProcessMonitor.AlertState> alertStates = (Map<String, AsyncProcessMonitor.AlertState>) alertStatesField.get(monitor);
    assertTrue(alertStates.containsKey("temp-remove"), "Alert state should exist after trigger");

    monitor.removeAlertRule("temp-remove");

    // Access alertRules list to ensure removal
    Field alertRulesField = AsyncProcessMonitor.class.getDeclaredField(ALERT_RULES);
    alertRulesField.setAccessible(true);
    List<AsyncProcessMonitor.AlertRule> alertRules = (List<AsyncProcessMonitor.AlertRule>) alertRulesField.get(monitor);

    assertFalse(alertRules.stream().anyMatch(r -> r.getName().equals("temp-remove")), "Rule list should not contain removed rule");
    assertFalse(alertStates.containsKey("temp-remove"), "Alert state should be cleared for removed rule");
  }

  /**
   * Forces the stop() path where awaitTermination returns false causing shutdownNow().
   */
  @Test
  void testStopForcesShutdownOnTimeout() throws Exception {
    Field monitoringSchedulerField = AsyncProcessMonitor.class.getDeclaredField("monitoringScheduler");
    monitoringSchedulerField.setAccessible(true);
    java.util.concurrent.ScheduledThreadPoolExecutor custom = new java.util.concurrent.ScheduledThreadPoolExecutor(1) {
      @SuppressWarnings("unused")
      private boolean shutdownNowCalled = false;
      @Override
      public java.util.List<Runnable> shutdownNow() {
        try {
          Field f = this.getClass().getDeclaredField("shutdownNowCalled");
          f.setAccessible(true);
            f.setBoolean(this, true);
        } catch (Exception ignored) { }
        return super.shutdownNow();
      }
      @Override
      public boolean awaitTermination(long timeout, java.util.concurrent.TimeUnit unit) {
        return false; // force timeout path
      }
    };
    monitoringSchedulerField.set(monitor, custom);

    monitor.stop();

    assertTrue(custom.isShutdown(), "Scheduler should be shutdown");
    try {
      Field f = custom.getClass().getDeclaredField("shutdownNowCalled");
      f.setAccessible(true);
      assertTrue(f.getBoolean(custom), "shutdownNow should have been invoked");
    } catch (NoSuchFieldException e) {
      // Ignore if field not found
    }
  }
}
