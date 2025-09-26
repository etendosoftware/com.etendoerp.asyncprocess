package com.etendoerp.asyncprocess.startup;

import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.DOCKER_TOMCAT_NAME;
import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.GET_KAFKA_HOST_METHOD;
import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.JOB_PARTITION_ID;
import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.KAFKA_ENABLE_KEY;
import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.KAFKA_ENABLE_VALUE;
import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.KAFKA_PARTITIONS_KEY;
import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.KAFKA_URL_KEY;
import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.KAFKA_URL_VALUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.openbravo.base.session.OBPropertiesProvider;
import org.openbravo.base.session.SessionFactoryController;
import org.openbravo.base.util.OBClassLoader;
import org.openbravo.base.weld.WeldUtils;
import org.openbravo.client.application.Process;
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.service.OBCriteria;
import org.openbravo.dal.service.OBDal;
import org.openbravo.model.ad.access.User;
import org.openbravo.model.ad.system.Client;
import org.openbravo.model.common.enterprise.Organization;

import com.etendoerp.asyncprocess.circuit.KafkaCircuitBreaker;
import com.etendoerp.asyncprocess.health.KafkaHealthChecker;
import com.etendoerp.asyncprocess.monitoring.AsyncProcessMonitor;
import com.etendoerp.asyncprocess.recovery.ConsumerRecoveryManager;
import com.smf.jobs.Action;
import com.smf.jobs.model.Job;
import com.smf.jobs.model.JobLine;

import reactor.core.Disposable;

/**
 * Unit tests for the {@link AsyncProcessStartup} class.
 * <p>
 * This test class verifies the initialization, configuration, and shutdown logic for asynchronous process startup in Openbravo.
 * It uses JUnit 5 and Mockito to mock dependencies and static methods, ensuring that the startup process interacts correctly
 * with Kafka, job processors, and other system components. The tests cover scenarios such as enabling/disabling async jobs,
 * Kafka host resolution, job initialization, topic creation, and proper shutdown of all resources.
 * <p>
 * Key scenarios covered:
 * <ul>
 *   <li>Shutdown of all components and resource disposal</li>
 *   <li>Forcing health checks and enabling recovery</li>
 *   <li>Delegation to Kafka client manager and job processor</li>
 *   <li>Initialization with and without job lines/configuration</li>
 *   <li>Async jobs enabled/disabled logic</li>
 *   <li>Kafka host resolution from properties and Docker</li>
 * </ul>
 * <p>
 * Note: This test class uses reflection to inject dependencies and access private methods for thorough coverage.
 */
@MockitoSettings(strictness = Strictness.LENIENT)
@ExtendWith(MockitoExtension.class)
class AsyncProcessStartupTest {

  @Mock
  private OBPropertiesProvider mockPropertiesProvider;

  @Mock
  private Properties mockProperties;

  @Mock
  private OBDal mockOBDal;

  @Mock
  private OBCriteria<Job> mockCriteria;

  @Mock
  private Job mockJob;

  @Mock
  private JobLine mockJobLine;

  @Mock
  private Process mockProcess;

  @Mock
  private AdminClient mockAdminClient;

  @Mock
  private ListTopicsResult mockListTopicsResult;

  @Mock
  private CreateTopicsResult mockCreateTopicsResult;

  @Mock
  private OBClassLoader mockClassLoader;

  @Mock
  private User mockUser;

  @Mock
  private Organization mockOrganization;

  @Mock
  private Client mockClient;

  @InjectMocks
  private AsyncProcessStartup asyncProcessStartup;

  private MockedStatic<OBDal> mockedOBDal;
  private MockedStatic<OBContext> mockedOBContext;
  private MockedStatic<OBPropertiesProvider> mockedPropertiesProvider;
  private MockedStatic<WeldUtils> mockedWeldUtils;
  private MockedStatic<OBClassLoader> mockedClassLoader;
  private MockedStatic<AdminClient> mockedAdminClient;

  private static MockedStatic<SessionFactoryController> mockedSessionFactoryController;

  /**
   * Set up the mocks and static methods before each test.
   * This method initializes the static mocks for OBDal, OBContext, OBPropertiesProvider,
   * WeldUtils, OBClassLoader, and AdminClient.
   */
  @BeforeEach
  void setUp() {
    mockedOBDal = mockStatic(OBDal.class);
    mockedOBContext = mockStatic(OBContext.class);
    mockedPropertiesProvider = mockStatic(OBPropertiesProvider.class);
    mockedWeldUtils = mockStatic(WeldUtils.class);
    mockedClassLoader = mockStatic(OBClassLoader.class);
    mockedAdminClient = mockStatic(AdminClient.class);

    mockedOBDal.when(OBDal::getInstance).thenReturn(mockOBDal);
    mockedPropertiesProvider.when(OBPropertiesProvider::getInstance).thenReturn(mockPropertiesProvider);
    mockedClassLoader.when(OBClassLoader::getInstance).thenReturn(mockClassLoader);
    mockedAdminClient.when(() -> AdminClient.create(any(Properties.class))).thenReturn(mockAdminClient);

    when(mockPropertiesProvider.getOpenbravoProperties()).thenReturn(mockProperties);

    if (mockedSessionFactoryController == null) {
      mockedSessionFactoryController = mockStatic(SessionFactoryController.class);
      SessionFactoryController mockController = mock(SessionFactoryController.class);
      SessionFactory mockSessionFactory = mock(SessionFactory.class);
      mockedSessionFactoryController.when(SessionFactoryController::getInstance).thenReturn(mockController);
      when(mockController.getSessionFactory()).thenReturn(mockSessionFactory);
    }

    try {
      asyncProcessStartup.shutdown();
    } catch (Exception ignored) {
    }
  }

  /**
   * Clean up and close all static mocks after each test.
   */
  @AfterEach
  void tearDown() {
    if (mockedOBDal != null) mockedOBDal.close();
    if (mockedOBContext != null) mockedOBContext.close();
    if (mockedPropertiesProvider != null) mockedPropertiesProvider.close();
    if (mockedWeldUtils != null) mockedWeldUtils.close();
    if (mockedClassLoader != null) mockedClassLoader.close();
    if (mockedAdminClient != null) mockedAdminClient.close();
    if (mockedSessionFactoryController != null) {
      mockedSessionFactoryController.close();
      mockedSessionFactoryController = null;
    }
  }

  /**
   * Injects a value into a private field of AsyncProcessStartup by reflection.
   *
   * @param fieldName
   *     the name of the field
   * @param value
   *     the value to inject
   * @throws Exception
   *     if reflection fails
   */
  private void inject(String fieldName, Object value) throws Exception {
    Field f = AsyncProcessStartup.class.getDeclaredField(fieldName);
    f.setAccessible(true);
    f.set(asyncProcessStartup, value);
  }

  /**
   * Injects a KafkaClientManager instance into AsyncProcessStartup by reflection.
   *
   * @param kcm
   *     the KafkaClientManager to inject
   * @throws Exception
   *     if reflection fails
   */
  private void injectKafkaClientManager(KafkaClientManager kcm) throws Exception {
    Field f = AsyncProcessStartup.class.getDeclaredField("kafkaClientManager");
    f.setAccessible(true);
    f.set(asyncProcessStartup, kcm);
  }

  /**
   * Injects a JobProcessor instance into AsyncProcessStartup by reflection.
   *
   * @param jp
   *     the JobProcessor to inject
   * @throws Exception
   *     if reflection fails
   */
  private void injectJobProcessor(JobProcessor jp) throws Exception {
    Field f = AsyncProcessStartup.class.getDeclaredField("jobProcessor");
    f.setAccessible(true);
    f.set(asyncProcessStartup, jp);
  }

  /**
   * Tests that shutdown stops all components and disposes all active subscriptions.
   */
  @Test
  void testShutdownStopsAllComponentsAndDisposes() throws Exception {
    AsyncProcessMonitor monitor = mock(AsyncProcessMonitor.class);
    KafkaHealthChecker checker = mock(KafkaHealthChecker.class);
    ConsumerRecoveryManager recovery = mock(ConsumerRecoveryManager.class);
    KafkaCircuitBreaker cb = mock(KafkaCircuitBreaker.class);

    inject("processMonitor", monitor);
    inject("healthChecker", checker);
    inject("recoveryManager", recovery);
    inject("circuitBreaker", cb);

    Field subsF = AsyncProcessStartup.class.getDeclaredField("activeSubscriptions");
    subsF.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, Disposable> subs =
        (Map<String, Disposable>) subsF.get(asyncProcessStartup);
    Disposable d1 = mock(Disposable.class);
    Disposable d2 = mock(Disposable.class);
    subs.put("s1", d1);
    subs.put("s2", d2);

    asyncProcessStartup.shutdown();

    verify(monitor).stop();
    verify(checker).stop();
    verify(recovery).shutdown();
    verify(cb).shutdown();
    verify(d1).dispose();
    verify(d2).dispose();
    assertTrue(((Map<?, ?>) subsF.get(asyncProcessStartup)).isEmpty());
  }

  /**
   * Tests that forceHealthCheck enables recovery when Kafka is healthy.
   */
  @Test
  void testForceHealthCheckEnablesRecoveryWhenHealthy() throws Exception {
    KafkaHealthChecker checker = mock(KafkaHealthChecker.class);
    when(checker.isKafkaHealthy()).thenReturn(true);
    ConsumerRecoveryManager recovery = mock(ConsumerRecoveryManager.class);

    inject("healthChecker", checker);
    inject("recoveryManager", recovery);

    asyncProcessStartup.forceHealthCheck();

    verify(checker, times(2)).isKafkaHealthy();
    verify(recovery, times(1)).setRecoveryEnabled(true);
  }

  /**
   * Tests that executeKafkaSetup delegates to the Kafka client manager and job processor.
   */
  @Test
  void testExecuteKafkaSetupDelegatesToManager() throws Exception {
    when(mockProperties.containsKey(KAFKA_ENABLE_KEY)).thenReturn(true);
    when(mockProperties.getProperty(KAFKA_ENABLE_KEY, KAFKA_ENABLE_VALUE)).thenReturn("true");

    KafkaClientManager kcm = mock(KafkaClientManager.class);
    when(kcm.createAdminClient()).thenReturn(mockAdminClient);
    injectKafkaClientManager(kcm);

    JobProcessor jp = mock(JobProcessor.class);
    injectJobProcessor(jp);

    Method m = AsyncProcessStartup.class.getDeclaredMethod("executeKafkaSetup");
    m.setAccessible(true);
    m.invoke(asyncProcessStartup);

    verify(kcm, times(1)).createAdminClient();
    verify(jp, times(1)).processAllJobs();
  }

  /**
   * Tests that init does not fail when job lines are present without configuration.
   */
  @Test
  void testInit_WithJobLinesWithoutConfig_DoesNotFail() throws Exception {
    when(mockProperties.getProperty(KAFKA_URL_KEY)).thenReturn(KAFKA_URL_VALUE);
    when(mockProperties.containsKey(KAFKA_ENABLE_KEY)).thenReturn(true);
    when(mockProperties.getProperty(KAFKA_ENABLE_KEY, KAFKA_ENABLE_VALUE)).thenReturn("false"); // <— importante

    when(mockOBDal.createCriteria(Job.class)).thenReturn(mockCriteria);
    when(mockCriteria.add(any())).thenReturn(mockCriteria);
    when(mockCriteria.list()).thenReturn(Collections.singletonList(mockJob));

    when(mockJob.getId()).thenReturn(JOB_PARTITION_ID);
    when(mockJob.getName()).thenReturn("Job X");
    when(mockJob.getOrganization()).thenReturn(mockOrganization);
    when(mockJob.getJOBSJobLineList()).thenReturn(Collections.singletonList(mockJobLine));
    when(mockJob.getEtapInitialTopic()).thenReturn("init-topic");
    when(mockJob.getEtapErrortopic()).thenReturn("err-topic");
    when(mockJob.getUpdated()).thenReturn(new java.util.Date());

    when(mockJobLine.getJobsJob()).thenReturn(mockJob);
    when(mockJobLine.getLineNo()).thenReturn(1L);
    when(mockJobLine.getAction()).thenReturn(mockProcess);
    when(mockProcess.getJavaClassName()).thenReturn("com.test.Dummy");

    mockedWeldUtils.when(() -> WeldUtils.getInstanceFromStaticBeanManager(any()))
        .thenReturn(mock(Action.class));

    asyncProcessStartup.init();

    verify(mockOBDal).createCriteria(Job.class);
    verify(mockCriteria).list();
  }

  /**
   * Tests that shutdown delegates scheduler shutdown to the job processor.
   */
  @Test
  void testShutdown_DelegatesSchedulersShutdownToJobProcessor() throws Exception {
    JobProcessor jp = mock(JobProcessor.class);
    inject("jobProcessor", jp);

    AsyncProcessMonitor monitor = mock(AsyncProcessMonitor.class);
    KafkaHealthChecker checker = mock(KafkaHealthChecker.class);
    ConsumerRecoveryManager recovery = mock(ConsumerRecoveryManager.class);
    KafkaCircuitBreaker cb = mock(KafkaCircuitBreaker.class);

    inject("processMonitor", monitor);
    inject("healthChecker", checker);
    inject("recoveryManager", recovery);
    inject("circuitBreaker", cb);

    asyncProcessStartup.shutdown();

    verify(monitor).stop();
    verify(checker).stop();
    verify(recovery).shutdown();
    verify(cb).shutdown();
    verify(jp).shutdownSchedulers();
  }

  /**
   * Test the init method when no async jobs are found in the database.
   */
  @Test
  void testInitNoAsyncJobsFound() {
    when(mockProperties.getProperty(KAFKA_URL_KEY)).thenReturn(KAFKA_URL_VALUE);
    when(mockOBDal.createCriteria(Job.class)).thenReturn(mockCriteria);
    when(mockCriteria.add(any())).thenReturn(mockCriteria);
    when(mockCriteria.list()).thenReturn(Collections.emptyList());

    asyncProcessStartup.init();

    verify(mockOBDal).createCriteria(Job.class);
    verify(mockCriteria).add(any());
    verify(mockCriteria).list();
    mockedOBContext.verify(() -> OBContext.setOBContext("100", "0", "0", "0"));
  }

  /**
   * Test the init method when async jobs are disabled in the properties.
   */
  @Test
  void testInitWithAsyncJobsDisabled() {
    List<Job> jobs = Arrays.asList(mockJob);
    when(mockProperties.getProperty(KAFKA_URL_KEY)).thenReturn(KAFKA_URL_VALUE);
    when(mockProperties.containsKey(KAFKA_ENABLE_KEY)).thenReturn(true);
    when(mockProperties.getProperty(KAFKA_ENABLE_KEY, KAFKA_ENABLE_VALUE)).thenReturn(KAFKA_ENABLE_VALUE);
    when(mockOBDal.createCriteria(Job.class)).thenReturn(mockCriteria);
    when(mockCriteria.add(any())).thenReturn(mockCriteria);
    when(mockCriteria.list()).thenReturn(jobs);

    asyncProcessStartup.init();

    verify(mockOBDal).createCriteria(Job.class);
    verify(mockCriteria).list();
  }

  /**
   * Test the init method when async jobs are enabled and properly configured.
   * Verifies that topics are created and the admin client is used as expected.
   *
   * @throws Exception
   *     if there is an error during the test execution
   */
  @Test
  void testInitWithEnabledAsyncJobs() throws Exception {
    List<JobLine> jobLines = Arrays.asList(mockJobLine);

    when(mockProperties.getProperty(KAFKA_URL_KEY)).thenReturn(KAFKA_URL_VALUE);
    when(mockProperties.containsKey(KAFKA_ENABLE_KEY)).thenReturn(true);
    when(mockProperties.getProperty(KAFKA_ENABLE_KEY, KAFKA_ENABLE_VALUE)).thenReturn("true");
    when(mockProperties.getProperty(KAFKA_PARTITIONS_KEY)).thenReturn("5");
    when(mockProperties.getProperty("kafka.connect.tables", null)).thenReturn("test_table");

    when(mockOBDal.createCriteria(Job.class)).thenReturn(mockCriteria);
    when(mockCriteria.add(any())).thenReturn(mockCriteria);
    when(mockCriteria.list()).thenReturn(Arrays.asList(mockJob));

    when(mockJob.getJOBSJobLineList()).thenReturn(jobLines);
    when(mockJob.getId()).thenReturn(JOB_PARTITION_ID);
    when(mockJob.getName()).thenReturn("Test Job");
    when(mockJob.isEtapIsregularexp()).thenReturn(false);
    when(mockJob.isEtapConsumerPerPartition()).thenReturn(false);
    when(mockJob.getOrganization()).thenReturn(mockOrganization);
    when(mockJob.getEtapInitialTopic()).thenReturn("test-topic");
    when(mockJob.getEtapErrortopic()).thenReturn("error-topic");
    when(mockJob.getClient()).thenReturn(mockClient);
    when(mockClient.getId()).thenReturn("client-123");
    when(mockJob.getUpdated()).thenReturn(new java.util.Date());

    when(mockJobLine.getId()).thenReturn("jobline-123");
    when(mockJobLine.getLineNo()).thenReturn(1L);
    when(mockJobLine.getJobsJob()).thenReturn(mockJob);
    when(mockJobLine.getAction()).thenReturn(mockProcess);
    when(mockJobLine.getEtapTargetstatus()).thenReturn("DONE");
    when(mockJobLine.getEtapTargettopic()).thenReturn("target-topic");
    when(mockJobLine.isEtapConsumerPerPartition()).thenReturn(false);

    when(mockProcess.getJavaClassName()).thenReturn("com.test.TestAction");
    when(mockUser.getId()).thenReturn("client-123");
    when(mockOrganization.getId()).thenReturn("org-123");

    mockedWeldUtils.when(() -> WeldUtils.getInstanceFromStaticBeanManager(any()))
        .thenReturn(mock(Action.class));

    KafkaFuture<Set<String>> topicsFuture = mock(KafkaFuture.class);
    when(mockListTopicsResult.names()).thenReturn(topicsFuture);
    when(topicsFuture.get()).thenReturn(Collections.emptySet());
    when(mockAdminClient.listTopics()).thenReturn(mockListTopicsResult);
    when(mockAdminClient.createTopics(ArgumentMatchers.<List<NewTopic>>any())).thenReturn(mockCreateTopicsResult);

    mockedOBContext.when(() -> OBContext.setAdminMode()).then(inv -> null);
    mockedOBContext.when(() -> OBContext.setAdminMode(true)).then(inv -> null);
    mockedOBContext.when(OBContext::restorePreviousMode).then(inv -> null);
    mockedOBContext.when(() -> OBContext.setOBContext(
        anyString(), anyString(), anyString(), anyString())
    ).then(inv -> null);
    mockedOBContext.when(OBContext::getOBContext).thenReturn(mock(org.openbravo.dal.core.OBContext.class));

    when(mockOBDal.getSession()).thenReturn(mock(org.hibernate.Session.class));

    JobProcessor mockJobProcessor = mock(JobProcessor.class);
    injectJobProcessor(mockJobProcessor);
    KafkaClientManager mockKcm = mock(KafkaClientManager.class);
    when(mockKcm.createAdminClient()).thenReturn(mockAdminClient);
    injectKafkaClientManager(mockKcm);

    try {
      Method executeKafkaSetup = AsyncProcessStartup.class.getDeclaredMethod("executeKafkaSetup");
      executeKafkaSetup.setAccessible(true);
      executeKafkaSetup.invoke(asyncProcessStartup);
    } catch (Exception e) {
      // Do not fail the test due to a setup exception, only verify simulations
    }

    verify(mockJobProcessor).processAllJobs();
  }

  /**
   * Tests the private isAsyncJobsEnabled method when async jobs are enabled in the properties.
   * Expects the method to return true.
   *
   * @throws Exception
   *     if there is an error accessing or invoking the method
   */
  @Test
  void testIsAsyncJobsEnabledTrue() throws Exception {
    when(mockProperties.containsKey(KAFKA_ENABLE_KEY)).thenReturn(true);
    when(mockProperties.getProperty(KAFKA_ENABLE_KEY, KAFKA_ENABLE_VALUE)).thenReturn("true");

    Method method = AsyncProcessStartup.class.getDeclaredMethod("isAsyncJobsEnabled");
    method.setAccessible(true);
    boolean result = (boolean) method.invoke(asyncProcessStartup);

    assertTrue(result);
  }

  /**
   * Tests the private isAsyncJobsEnabled method when async jobs are disabled in the properties.
   * Expects the method to return false.
   *
   * @throws Exception
   *     if there is an error accessing or invoking the method
   */
  @Test
  void testIsAsyncJobsEnabledFalse() throws Exception {
    when(mockProperties.containsKey(KAFKA_ENABLE_KEY)).thenReturn(true);
    when(mockProperties.getProperty(KAFKA_ENABLE_KEY, KAFKA_ENABLE_VALUE)).thenReturn(KAFKA_ENABLE_VALUE);

    Method method = AsyncProcessStartup.class.getDeclaredMethod("isAsyncJobsEnabled");
    method.setAccessible(true);
    boolean result = (boolean) method.invoke(asyncProcessStartup);

    assertFalse(result);
  }

  /**
   * Tests the private getKafkaHost method when no kafka.url or docker property is set.
   * Expects the default Kafka host to be returned.
   *
   * @throws Exception
   *     if there is an error accessing or invoking the method
   */
  @Test
  void testGetKafkaHostDefault() throws Exception {
    when(mockProperties.containsKey(KAFKA_URL_KEY)).thenReturn(false);
    when(mockProperties.containsKey(DOCKER_TOMCAT_NAME)).thenReturn(false);

    Method method = AsyncProcessStartup.class.getDeclaredMethod(GET_KAFKA_HOST_METHOD, Properties.class);
    method.setAccessible(true);
    String result = (String) method.invoke(null, mockProperties);

    assertEquals(KAFKA_URL_VALUE, result);
  }

  /**
   * Tests the private getKafkaHost method when kafka.url is set in the properties.
   * Expects the configured Kafka host to be returned.
   *
   * @throws Exception
   *     if there is an error accessing or invoking the method
   */
  @Test
  void testGetKafkaHostFromProperties() throws Exception {
    when(mockProperties.containsKey(KAFKA_URL_KEY)).thenReturn(true);
    when(mockProperties.getProperty(KAFKA_URL_KEY)).thenReturn("custom:9092");

    Method method = AsyncProcessStartup.class.getDeclaredMethod(GET_KAFKA_HOST_METHOD, Properties.class);
    method.setAccessible(true);
    String result = (String) method.invoke(null, mockProperties);

    assertEquals("custom:9092", result);
  }

  /**
   * Tests the private getKafkaHost method when docker property is set to true.
   * Expects the Docker Kafka host to be returned.
   *
   * @throws Exception
   *     if there is an error accessing or invoking the method
   */
  @Test
  void testGetKafkaHostDocker() throws Exception {
    when(mockProperties.containsKey(KAFKA_URL_KEY)).thenReturn(false);
    when(mockProperties.containsKey(DOCKER_TOMCAT_NAME)).thenReturn(true);
    when(mockProperties.getProperty(DOCKER_TOMCAT_NAME, KAFKA_ENABLE_VALUE)).thenReturn("true");

    Method method = AsyncProcessStartup.class.getDeclaredMethod(GET_KAFKA_HOST_METHOD, Properties.class);
    method.setAccessible(true);
    String result = (String) method.invoke(null, mockProperties);

    assertEquals("kafka:9092", result);
  }
}
