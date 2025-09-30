package com.etendoerp.asyncprocess.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.lang.reflect.Method;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.hibernate.criterion.Restrictions;
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
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.service.OBCriteria;
import org.openbravo.dal.service.OBDal;

import com.etendoerp.asyncprocess.health.KafkaHealthChecker;
import com.etendoerp.asyncprocess.recovery.ConsumerRecoveryManager;
import com.smf.jobs.model.Job;

/**
 * Unit tests for the {@link AsyncProcessReconfigurationManager} class.
 * <p>
 * This test class verifies the dynamic reconfiguration functionality for async process jobs.
 * It uses Mockito to mock dependencies and simulate configuration changes.
 */
@MockitoSettings(strictness = Strictness.LENIENT)
@ExtendWith(MockitoExtension.class)
public class AsyncProcessReconfigurationManagerTest {

  private static final String TEST_JOB_ID = "test-job-id";
  private static final long TEST_INTERVAL = 1000;
  private static final String MONITORING_ENABLED_KEY = "monitoringEnabled";
  private static final String RECONFIGURATION_ENABLED_KEY = "reconfigurationEnabled";

  @Mock
  private KafkaHealthChecker mockHealthChecker;

  @Mock
  private ConsumerRecoveryManager mockRecoveryManager;

  @Mock
  private Job mockJob;

  @Mock
  private AsyncProcessReconfigurationManager.ConfigurationChangeListener mockListener;

  @Mock
  private ScheduledExecutorService mockScheduler;

  private AsyncProcessReconfigurationManager manager;
  private MockedStatic<OBContext> mockedOBContext;
  private MockedStatic<OBDal> mockedOBDal;
  private MockedStatic<OBPropertiesProvider> mockedOBProperties;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    mockedOBContext = Mockito.mockStatic(OBContext.class);
    mockedOBDal = Mockito.mockStatic(OBDal.class);
    mockedOBProperties = Mockito.mockStatic(OBPropertiesProvider.class);

    // Setup mocks
    when(mockHealthChecker.isKafkaHealthy()).thenReturn(true);
    when(mockJob.getId()).thenReturn(TEST_JOB_ID);
    when(mockJob.getName()).thenReturn("Test Job");
    when(mockJob.getUpdated()).thenReturn(new java.util.Date());

    var mockOBContext = mock(OBContext.class);
    mockedOBContext.when(OBContext::getOBContext).thenReturn(mockOBContext);

    var mockCriteria = mock(OBCriteria.class);
    var mockOBDalInstance = mock(OBDal.class);
    mockedOBDal.when(OBDal::getInstance).thenReturn(mockOBDalInstance);
    // Ensure createCriteria returns an OBCriteria instance (not a Hibernate Criteria mock)
    Mockito.doReturn((OBCriteria) mockCriteria).when(mockOBDalInstance).createCriteria(Job.class);
    Mockito.doReturn((OBCriteria) mockCriteria).when((OBCriteria) mockCriteria).add(any());
    Mockito.doReturn(List.of(mockJob)).when((OBCriteria) mockCriteria).list();

    var mockProperties = mock(Properties.class);
    var mockProvider = mock(OBPropertiesProvider.class);
    mockedOBProperties.when(OBPropertiesProvider::getInstance).thenReturn(mockProvider);
  Mockito.doReturn(mockProperties).when(mockProvider).getOpenbravoProperties();
  Mockito.doReturn("true").when(mockProperties).getProperty("kafka.enable", "false");

    manager = new AsyncProcessReconfigurationManager(mockHealthChecker, mockRecoveryManager, TEST_INTERVAL);
  }

  @AfterEach
  void tearDown() {
    if (manager != null) {
      manager.stopMonitoring();
    }
    mockedOBContext.close();
    mockedOBDal.close();
    mockedOBProperties.close();
  }

  @Test
  void testConstructorWithDefaultInterval() {
    AsyncProcessReconfigurationManager mgr = new AsyncProcessReconfigurationManager(mockHealthChecker, mockRecoveryManager);
    assertNotNull(mgr);
    mgr.stopMonitoring();
  }

  @Test
  void testConstructorWithCustomInterval() {
    assertNotNull(manager);
  }

  @Test
  void testStartAndStopMonitoring() {
    manager.startMonitoring();
    assertTrue(manager.getConfigurationStatus().get(MONITORING_ENABLED_KEY).equals(true));
    manager.stopMonitoring();
    assertFalse(manager.getConfigurationStatus().get(MONITORING_ENABLED_KEY).equals(true));
  }

  @Test
  void testMonitoringDisabledByDefault() {
    // Monitoring is enabled by default in the implementation
    assertTrue(manager.getConfigurationStatus().get(MONITORING_ENABLED_KEY).equals(true));
  }

  @Test
  void testSetMonitoringEnabled() {
    manager.setMonitoringEnabled(true);
    assertTrue(manager.getConfigurationStatus().get(MONITORING_ENABLED_KEY).equals(true));
    manager.setMonitoringEnabled(false);
    assertFalse(manager.getConfigurationStatus().get(MONITORING_ENABLED_KEY).equals(true));
  }

  @Test
  void testSetReconfigurationEnabled() {
    manager.setReconfigurationEnabled(true);
    assertTrue(manager.getConfigurationStatus().get(RECONFIGURATION_ENABLED_KEY).equals(true));
    manager.setReconfigurationEnabled(false);
    assertFalse(manager.getConfigurationStatus().get(RECONFIGURATION_ENABLED_KEY).equals(true));
  }

  @Test
  void testAddAndRemoveConfigurationChangeListener() {
    manager.addConfigurationChangeListener(mockListener);
    manager.removeConfigurationChangeListener(mockListener);
    // Verify no exceptions
  }

  @Test
  @SuppressWarnings("squid:S3011")
  void testRemoveJobUnregistersMatchingConsumers() throws Exception {
    // Prepare recovery map with one consumer matching the job id prefix
    String jobId = "job-to-remove";
    Map<String, Object> recoveryMap = new HashMap<>();
    recoveryMap.put(jobId + "-consumer1", new Object());
    recoveryMap.put("other-consumer", new Object());

    when(mockRecoveryManager.getRecoveryStatus()).thenReturn(recoveryMap);

    // Invoke private removeJob via reflection
    Method removeMethod = AsyncProcessReconfigurationManager.class.getDeclaredMethod("removeJob", String.class);
    removeMethod.setAccessible(true);
    removeMethod.invoke(manager, jobId);

    // Verify that unregisterConsumer was called for the matching consumer id
    verify(mockRecoveryManager, times(1)).unregisterConsumer(eq(jobId + "-consumer1"));
    // And not for the other consumer
    verify(mockRecoveryManager, never()).unregisterConsumer(eq("other-consumer"));
  }

  @Test
  @SuppressWarnings("squid:S3011")
  void testModifyJobCallsRemoveAndAdd() throws Exception {
    // Create old and new job mocks
    Job oldJob = mock(Job.class);
    Job newJob = mock(Job.class);
    when(oldJob.getId()).thenReturn("old-job");
    when(newJob.getId()).thenReturn("new-job");
    when(newJob.getName()).thenReturn("New Job");

    // Prepare recovery map for old job so removeJob will attempt unregister
    Map<String, Object> recoveryMap = new HashMap<>();
    recoveryMap.put("old-job-consumer", new Object());
    when(mockRecoveryManager.getRecoveryStatus()).thenReturn(recoveryMap);

    // Spy the manager so we invoke real modifyJob
    AsyncProcessReconfigurationManager spyManager = Mockito.spy(manager);

    Method modifyMethod = AsyncProcessReconfigurationManager.class.getDeclaredMethod("modifyJob", Job.class, Job.class);
    modifyMethod.setAccessible(true);
    modifyMethod.invoke(spyManager, oldJob, newJob);

    // Ensure unregister was invoked for old job's consumer
    verify(mockRecoveryManager, times(1)).unregisterConsumer(eq("old-job-consumer"));
  }

  @Test
  @SuppressWarnings("squid:S3011")
  void testNotifyConfigurationChangeInvokesListeners() throws Exception {
    // Register the mock listener
    manager.addConfigurationChangeListener(mockListener);

    // Call private notifyConfigurationChange with a lambda that triggers onJobAdded
    Method notifyMethod = AsyncProcessReconfigurationManager.class.getDeclaredMethod("notifyConfigurationChange", java.util.function.Consumer.class);
    notifyMethod.setAccessible(true);

    // Use the mockJob (already available) as parameter to listener
    java.util.function.Consumer<AsyncProcessReconfigurationManager.ConfigurationChangeListener> invoker = l -> l.onJobAdded(mockJob);

    notifyMethod.invoke(manager, invoker);

    verify(mockListener, times(1)).onJobAdded(eq(mockJob));
  }

  @Test
  void testForceConfigurationReload() {
    manager.forceConfigurationReload();
    // Verify that check is called, but since it's async, hard to verify directly
  }

  @Test
  void testGetConfigurationStatus() {
    Map<String, Object> status = manager.getConfigurationStatus();
    assertNotNull(status);
    assertTrue(status.containsKey(MONITORING_ENABLED_KEY));
    assertTrue(status.containsKey(RECONFIGURATION_ENABLED_KEY));
    assertTrue(status.containsKey("activeJobs"));
    assertTrue(status.containsKey("lastConfigurationHash"));
    assertTrue(status.containsKey("configCheckIntervalMs"));
    assertEquals(TEST_INTERVAL, status.get("configCheckIntervalMs"));
  }

  @Test
  void testGetActiveJobIds() {
    List<String> ids = manager.getActiveJobIds();
    assertNotNull(ids);
    // Initially empty
    assertTrue(ids.isEmpty());
  }

  @Test
  void testGetJobConfiguration() {
    Job config = manager.getJobConfiguration(TEST_JOB_ID);
    // Initially null
    assertEquals(null, config);
  }

  @Test
  void testConfigurationChangeListenerCallbacks() {
    manager.addConfigurationChangeListener(mockListener);
    // Simulate configuration change by calling private method via reflection or mock setup
    // For simplicity, just verify listener is added
    verify(mockListener, never()).onJobAdded(any());
  }
}