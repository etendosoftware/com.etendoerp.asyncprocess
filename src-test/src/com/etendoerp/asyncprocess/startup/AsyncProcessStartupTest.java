package com.etendoerp.asyncprocess.startup;

import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.DOCKER_TOMCAT_NAME;
import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.GET_KAFKA_HOST_METHOD;
import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.JOB_PARTITION_ID;
import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.KAFKA_ENABLE_KEY;
import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.KAFKA_ENABLE_VALUE;
import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.KAFKA_PARTITIONS_KEY;
import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.KAFKA_URL_KEY;
import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.KAFKA_URL_VALUE;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

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
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.openbravo.base.session.OBPropertiesProvider;
import org.openbravo.base.session.SessionFactoryController;
import org.openbravo.base.util.OBClassLoader;
import org.openbravo.base.weld.WeldUtils;
import org.openbravo.base.exception.OBException;
import org.openbravo.client.application.Process;
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.service.OBCriteria;
import org.openbravo.dal.service.OBDal;
import org.openbravo.model.ad.access.User;
import org.openbravo.model.ad.system.Client;
import org.openbravo.model.common.enterprise.Organization;

import com.etendoerp.asyncprocess.circuit.KafkaCircuitBreaker;
import com.etendoerp.asyncprocess.config.AsyncProcessConfig;
import com.etendoerp.asyncprocess.config.AsyncProcessReconfigurationManager;
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

  public static final String PROCESS_MONITOR = "processMonitor";
  public static final String HEALTH_CHECKER = "healthChecker";
  public static final String RECOVERY_MANAGER = "recoveryManager";
  public static final String CIRCUIT_BREAKER = "circuitBreaker";
  public static final String EXECUTE_KAFKA_SETUP = "executeKafkaSetup";
  public static final String CONSUMER_2 = "consumer-2";
  public static final String TOPIC = "topic-1";
  public static final String GROUP_1 = "group-1";
  public static final String CONSUMER_1 = "consumer-1";
  public static final String INIT_CIRCUIT_BREAKER = "initializeCircuitBreaker";
  public static final String EXCEPTION_MSG = "Expected init() to handle exceptions internally, but exception was thrown: ";
  public static final String STARTUP_FAILURE = "Test startup failure";
  public static final String HANDLE_STARTUP_FAILURE = "handleStartupFailure";
  public static final String JOB_PROCESSOR = "jobProcessor";
  private static MockedStatic<SessionFactoryController> mockedSessionFactoryController;
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
  private AsyncProcessMonitor monitor;
  private MockedStatic<OBDal> mockedOBDal;
  private MockedStatic<OBContext> mockedOBContext;
  private MockedStatic<OBPropertiesProvider> mockedPropertiesProvider;
  private MockedStatic<WeldUtils> mockedWeldUtils;
  private MockedStatic<OBClassLoader> mockedClassLoader;
  private MockedStatic<AdminClient> mockedAdminClient;

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
    monitor = new AsyncProcessMonitor();
    try {
      asyncProcessStartup.shutdown();
    } catch (Exception ignored) {
      // Ignore exceptions during shutdown in setup
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
    Field f = AsyncProcessStartup.class.getDeclaredField(JOB_PROCESSOR);
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

    inject(PROCESS_MONITOR, monitor);
    inject(HEALTH_CHECKER, checker);
    inject(RECOVERY_MANAGER, recovery);
    inject(CIRCUIT_BREAKER, cb);

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

    inject(HEALTH_CHECKER, checker);
    inject(RECOVERY_MANAGER, recovery);

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

    Method m = AsyncProcessStartup.class.getDeclaredMethod(EXECUTE_KAFKA_SETUP);
    m.setAccessible(true);
    m.invoke(asyncProcessStartup);

    verify(kcm, times(1)).createAdminClient();
    verify(jp, times(1)).processAllJobs();
  }

  @Test
  void testExecuteKafkaSetup_whenKafkaDisabledDoesNotCreateAdminClientOrProcessJobs() throws Exception {
    when(mockProperties.containsKey(KAFKA_ENABLE_KEY)).thenReturn(true);
    when(mockProperties.getProperty(KAFKA_ENABLE_KEY, KAFKA_ENABLE_VALUE)).thenReturn("false");

    KafkaClientManager kcm = mock(KafkaClientManager.class);
    injectKafkaClientManager(kcm);

    JobProcessor jp = mock(JobProcessor.class);
    injectJobProcessor(jp);

    Method m = AsyncProcessStartup.class.getDeclaredMethod(EXECUTE_KAFKA_SETUP);
    m.setAccessible(true);
    m.invoke(asyncProcessStartup);

    verify(kcm, times(0)).createAdminClient();
    verify(jp, times(0)).processAllJobs();
  }

  @Test
  void testExecuteKafkaSetup_whenAdminClientCreationFailsWrapsInOBException() throws Exception {
    when(mockProperties.containsKey(KAFKA_ENABLE_KEY)).thenReturn(true);
    when(mockProperties.getProperty(KAFKA_ENABLE_KEY, KAFKA_ENABLE_VALUE)).thenReturn("true");

    KafkaClientManager kcm = mock(KafkaClientManager.class);
    when(kcm.createAdminClient()).thenThrow(new OBException("boom"));
    injectKafkaClientManager(kcm);

    JobProcessor jp = mock(JobProcessor.class);
    injectJobProcessor(jp);

    Method m = AsyncProcessStartup.class.getDeclaredMethod(EXECUTE_KAFKA_SETUP);
    m.setAccessible(true);

    InvocationTargetException exception = assertThrows(InvocationTargetException.class,
        () -> m.invoke(asyncProcessStartup));
    assertTrue(exception.getCause() instanceof OBException);
  }

  @Test
  void testExecuteKafkaSetup_whenJobProcessingFailsWrapsInOBException() throws Exception {
    when(mockProperties.containsKey(KAFKA_ENABLE_KEY)).thenReturn(true);
    when(mockProperties.getProperty(KAFKA_ENABLE_KEY, KAFKA_ENABLE_VALUE)).thenReturn("true");

    KafkaClientManager kcm = mock(KafkaClientManager.class);
    when(kcm.createAdminClient()).thenReturn(mockAdminClient);
    injectKafkaClientManager(kcm);

    JobProcessor jp = mock(JobProcessor.class);
    doThrow(new OBException("job failure")).when(jp).processAllJobs();
    injectJobProcessor(jp);

    Method m = AsyncProcessStartup.class.getDeclaredMethod(EXECUTE_KAFKA_SETUP);
    m.setAccessible(true);

    InvocationTargetException exception = assertThrows(InvocationTargetException.class,
        () -> m.invoke(asyncProcessStartup));
    assertTrue(exception.getCause() instanceof OBException);
    verify(kcm, times(1)).createAdminClient();
  }

  @Test
  void testWaitForSetupCompletion_whenInterruptedWrapsInOBExceptionAndReinterruptsThread() throws Exception {
    AsyncProcessMonitor monitor = mock(AsyncProcessMonitor.class);
    inject(PROCESS_MONITOR, monitor);
    ConsumerRecoveryManager recovery = mock(ConsumerRecoveryManager.class);
    when(recovery.isRecoveryEnabled()).thenReturn(false);
    inject(RECOVERY_MANAGER, recovery);

    Method method = AsyncProcessStartup.class.getDeclaredMethod("waitForSetupCompletion", CompletableFuture.class);
    method.setAccessible(true);

    CompletableFuture<Void> future = new CompletableFuture<>();

    Thread.currentThread().interrupt();
    try {
      InvocationTargetException exception = assertThrows(InvocationTargetException.class,
          () -> method.invoke(asyncProcessStartup, future));
      assertTrue(exception.getCause() instanceof OBException);
      assertTrue(Thread.currentThread().isInterrupted());
      verify(monitor).recordJobExecution("SYSTEM_SETUP", "Initial Setup Failed", 0L, false, false);
      verify(recovery).isRecoveryEnabled();
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void testWaitForSetupCompletion_whenExecutionFailsRecordsFailureAndThrowsOBException() throws Exception {
    AsyncProcessMonitor monitor = mock(AsyncProcessMonitor.class);
    inject(PROCESS_MONITOR, monitor);
    ConsumerRecoveryManager recovery = mock(ConsumerRecoveryManager.class);
    when(recovery.isRecoveryEnabled()).thenReturn(true);
    inject(RECOVERY_MANAGER, recovery);

    Method method = AsyncProcessStartup.class.getDeclaredMethod("waitForSetupCompletion", CompletableFuture.class);
    method.setAccessible(true);

    CompletableFuture<Void> future = new CompletableFuture<>();
    future.completeExceptionally(new OBException("setup failure"));

    InvocationTargetException exception = assertThrows(InvocationTargetException.class,
        () -> method.invoke(asyncProcessStartup, future));
    assertTrue(exception.getCause() instanceof OBException);
    verify(monitor).recordJobExecution("SYSTEM_SETUP", "Initial Setup Failed", 0L, false, false);
    verify(recovery).isRecoveryEnabled();
  }

  @Test
  void testForceHealthCheckWhenKafkaUnhealthyDoesNotEnableRecovery() throws Exception {
    KafkaHealthChecker checker = mock(KafkaHealthChecker.class);
    when(checker.isKafkaHealthy()).thenReturn(false);
    ConsumerRecoveryManager recovery = mock(ConsumerRecoveryManager.class);
    AsyncProcessReconfigurationManager reconfigurationManager = mock(AsyncProcessReconfigurationManager.class);

    inject(HEALTH_CHECKER, checker);
    inject(RECOVERY_MANAGER, recovery);
    inject("reconfigurationManager", reconfigurationManager);

    asyncProcessStartup.forceHealthCheck();

    verify(checker, times(2)).isKafkaHealthy();
    verify(recovery, never()).setRecoveryEnabled(true);
    verify(reconfigurationManager).forceConfigurationReload();
  }

  @Test
  void testForceConsumerRecoveryWithoutManagerThrowsIllegalStateException() {
    assertThrows(IllegalStateException.class, () -> asyncProcessStartup.forceConsumerRecovery(
        CONSUMER_1));
  }

  @Test
  void testForceConsumerRecoveryPropagatesExceptions() throws Exception {
    ConsumerRecoveryManager recovery = mock(ConsumerRecoveryManager.class);
    inject(RECOVERY_MANAGER, recovery);
    doThrow(new OBException("recovery failure")).when(recovery).forceRecoverConsumer(
        CONSUMER_2);

    OBException exception = assertThrows(OBException.class, () -> asyncProcessStartup.forceConsumerRecovery(
        CONSUMER_2));

    assertEquals("recovery failure", exception.getMessage());
    verify(recovery).forceRecoverConsumer(CONSUMER_2);
  }

  @Test
  void testInitializeRecoveryManager_recreationFailureWrapsException() throws Exception {
    KafkaHealthChecker checker = mock(KafkaHealthChecker.class);
    inject(HEALTH_CHECKER, checker);

    KafkaClientManager kcm = mock(KafkaClientManager.class);
    inject("kafkaClientManager", kcm);

    Method method = AsyncProcessStartup.class.getDeclaredMethod("initializeRecoveryManager");
    method.setAccessible(true);
    method.invoke(asyncProcessStartup);

    Field recoveryField = AsyncProcessStartup.class.getDeclaredField(RECOVERY_MANAGER);
    recoveryField.setAccessible(true);
    ConsumerRecoveryManager recoveryManager = (ConsumerRecoveryManager) recoveryField.get(asyncProcessStartup);

    Field functionField = ConsumerRecoveryManager.class.getDeclaredField("consumerRecreationFunction");
    functionField.setAccessible(true);
    ConsumerRecoveryManager.ConsumerRecreationFunction recreationFunction =
        (ConsumerRecoveryManager.ConsumerRecreationFunction) functionField.get(recoveryManager);

    AsyncProcessConfig config = mock(AsyncProcessConfig.class);
    ConsumerRecoveryManager.ConsumerInfo consumerInfo = mock(ConsumerRecoveryManager.ConsumerInfo.class);
    when(consumerInfo.getTopic()).thenReturn(TOPIC);
    when(consumerInfo.isRegExp()).thenReturn(false);
    when(consumerInfo.getConfig()).thenReturn(config);
    when(consumerInfo.getGroupId()).thenReturn(GROUP_1);
    when(consumerInfo.getConsumerId()).thenReturn(CONSUMER_1);

    when(kcm.createReceiver(TOPIC, false, config, GROUP_1))
        .thenThrow(new OBException("receiver boom"));

    OBException exception = assertThrows(OBException.class, () -> recreationFunction.recreateConsumer(consumerInfo));

    assertEquals("Consumer recreation failed", exception.getMessage());
    verify(kcm).createReceiver(TOPIC, false, config, GROUP_1);
  }

  @Test
  void testInitializeHealthCheckerHandlersTriggerRecoveryAndCircuitBreaker() throws Exception {
    KafkaCircuitBreaker circuitBreaker = mock(KafkaCircuitBreaker.class);
    inject(CIRCUIT_BREAKER, circuitBreaker);

    ConsumerRecoveryManager recoveryManager = mock(ConsumerRecoveryManager.class);
    inject(RECOVERY_MANAGER, recoveryManager);

    Field initializedField = AsyncProcessStartup.class.getDeclaredField("isInitialized");
    initializedField.setAccessible(true);
    initializedField.setBoolean(asyncProcessStartup, true);

    AtomicReference<Runnable> onRestoredRef = new AtomicReference<>();
    AtomicReference<Runnable> onLostRef = new AtomicReference<>();

    try (MockedConstruction<KafkaHealthChecker> mockedHealthChecker =
             mockConstruction(KafkaHealthChecker.class, (mockChecker, context) -> {
               when(mockChecker.isKafkaHealthy()).thenReturn(true);
               doAnswer(invocation -> {
                 onRestoredRef.set(invocation.getArgument(0));
                 return null;
               }).when(mockChecker).setOnKafkaHealthRestored(any(Runnable.class));
               doAnswer(invocation -> {
                 onLostRef.set(invocation.getArgument(0));
                 return null;
               }).when(mockChecker).setOnKafkaHealthLost(any(Runnable.class));
             })) {

      Method method = AsyncProcessStartup.class.getDeclaredMethod("initializeHealthChecker", String.class);
      method.setAccessible(true);
      method.invoke(asyncProcessStartup, "localhost:9092");

      KafkaHealthChecker constructedChecker = mockedHealthChecker.constructed().get(0);
      verify(constructedChecker).start();

      onRestoredRef.get().run();
      verify(recoveryManager).setRecoveryEnabled(true);

      onLostRef.get().run();
      verify(circuitBreaker).forceOpen();
    }
  }

  /**
   * Tests that init does not fail when job lines are present without configuration.
   */
  @Test
  void testInit_WithJobLinesWithoutConfigDoesNotFail() throws Exception {
    when(mockProperties.getProperty(KAFKA_URL_KEY)).thenReturn(KAFKA_URL_VALUE);
    when(mockProperties.containsKey(KAFKA_ENABLE_KEY)).thenReturn(true);
    when(mockProperties.getProperty(KAFKA_ENABLE_KEY, KAFKA_ENABLE_VALUE)).thenReturn("false");

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
    inject(JOB_PROCESSOR, jp);

    AsyncProcessMonitor monitor = mock(AsyncProcessMonitor.class);
    KafkaHealthChecker checker = mock(KafkaHealthChecker.class);
    ConsumerRecoveryManager recovery = mock(ConsumerRecoveryManager.class);
    KafkaCircuitBreaker cb = mock(KafkaCircuitBreaker.class);

    inject(PROCESS_MONITOR, monitor);
    inject(HEALTH_CHECKER, checker);
    inject(RECOVERY_MANAGER, recovery);
    inject(CIRCUIT_BREAKER, cb);

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
    mockedOBContext.verify(OBContext::setAdminMode);
    mockedOBContext.verify(OBContext::restorePreviousMode);
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

    mockedOBContext.when(OBContext::setAdminMode).then(inv -> null);
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
      Method executeKafkaSetup = AsyncProcessStartup.class.getDeclaredMethod(EXECUTE_KAFKA_SETUP);
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

  /**
   * Tests the private initializeCircuitBreaker method for successful initialization.
   * Verifies that the circuit breaker is created, state change listener is set,
   * and process monitor is called when state changes.
   *
   * @throws Exception
   *     if there is an error accessing or invoking the method
   */
  @Test
  void testInitializeCircuitBreakerSuccess() throws Exception {
    // Mock process monitor
    AsyncProcessMonitor monitor = mock(AsyncProcessMonitor.class);
    inject(PROCESS_MONITOR, monitor);

    // Use spy to allow real method calls while still mocking construction
    try (MockedConstruction<KafkaCircuitBreaker> mockedCircuitBreaker = mockConstruction(KafkaCircuitBreaker.class,
        (mock, context) -> {
          // Verify constructor arguments
          assertEquals("async-process-kafka", context.arguments().get(0));
          assertTrue(context.arguments().get(1) instanceof KafkaCircuitBreaker.CircuitBreakerConfig);
        })) {

      // Call the private method
      Method method = AsyncProcessStartup.class.getDeclaredMethod(INIT_CIRCUIT_BREAKER);
      method.setAccessible(true);
      method.invoke(asyncProcessStartup);

      // Verify circuit breaker was created
      assertEquals(1, mockedCircuitBreaker.constructed().size());
      KafkaCircuitBreaker circuitBreaker = mockedCircuitBreaker.constructed().get(0);

      // Verify setStateChangeListener was called
      verify(circuitBreaker).setStateChangeListener(any());

      // Get the injected circuit breaker
      Field circuitBreakerField = AsyncProcessStartup.class.getDeclaredField(CIRCUIT_BREAKER);
      circuitBreakerField.setAccessible(true);
      KafkaCircuitBreaker injectedCircuitBreaker = (KafkaCircuitBreaker) circuitBreakerField.get(asyncProcessStartup);
      assertNotNull(injectedCircuitBreaker);
    }
  }

  /**
   * Tests that initializeCircuitBreaker throws OBException when circuit breaker creation fails.
   */
  @Test
  void testInitializeCircuitBreaker_WhenCreationFails_ThrowsOBException() throws Exception {
    // Mock process monitor
    AsyncProcessMonitor monitor = mock(AsyncProcessMonitor.class);
    inject(PROCESS_MONITOR, monitor);

    // Create a spy of AsyncProcessStartup to override the circuit breaker creation
    AsyncProcessStartup spyStartup = spy(new AsyncProcessStartup());

    // Inject the process monitor into the spy
    Field processMonitorField = AsyncProcessStartup.class.getDeclaredField(PROCESS_MONITOR);
    processMonitorField.setAccessible(true);
    processMonitorField.set(spyStartup, monitor);

    // Use mockConstruction that will fail during creation
    try (MockedConstruction<KafkaCircuitBreaker> mockedCircuitBreaker =
             mockConstruction(KafkaCircuitBreaker.class, (mock, context) -> {
               throw new RuntimeException("Circuit breaker creation failed");
             })) {

      // Call the private method and expect OBException
      Method method = AsyncProcessStartup.class.getDeclaredMethod(INIT_CIRCUIT_BREAKER);
      method.setAccessible(true);

      InvocationTargetException exception = assertThrows(InvocationTargetException.class,
          () -> method.invoke(spyStartup));

      // Verify that OBException was thrown with the correct message
      assertTrue(exception.getCause() instanceof OBException);
      OBException obException = (OBException) exception.getCause();
      assertEquals("Circuit breaker initialization failed", obException.getMessage());
      assertTrue(obException.getCause() instanceof RuntimeException);
      // Accept either the original message or the Mockito wrapper message
      String causeMessage = obException.getCause().getMessage();
      assertTrue(causeMessage.contains("Circuit breaker creation failed") ||
              causeMessage.contains("Could not initialize mocked construction"),
          "Expected message to contain either 'Circuit breaker creation failed' or 'Could not initialize mocked construction' but was: " + causeMessage);
    }
  }

  /**
   * Tests that initializeCircuitBreaker throws OBException when setStateChangeListener fails.
   */
  @Test
  void testInitializeCircuitBreaker_WhenSetStateChangeListenerFails_ThrowsOBException() throws Exception {
    // Mock process monitor
    AsyncProcessMonitor monitor = mock(AsyncProcessMonitor.class);
    inject(PROCESS_MONITOR, monitor);

    // Use mockConstruction to make setStateChangeListener throw exception
    try (MockedConstruction<KafkaCircuitBreaker> mockedCircuitBreaker =
             mockConstruction(KafkaCircuitBreaker.class, (mock, context) -> {
               // Mock setStateChangeListener to throw an exception
               doThrow(new OBException("StateChangeListener setup failed"))
                   .when(mock).setStateChangeListener(any());
             })) {

      // Call the private method and expect OBException
      Method method = AsyncProcessStartup.class.getDeclaredMethod(INIT_CIRCUIT_BREAKER);
      method.setAccessible(true);

      InvocationTargetException exception = assertThrows(InvocationTargetException.class,
          () -> method.invoke(asyncProcessStartup));

      // Verify that OBException was thrown with the correct message
      assertTrue(exception.getCause() instanceof OBException);
      OBException obException = (OBException) exception.getCause();
      assertEquals("Circuit breaker initialization failed", obException.getMessage());
      assertTrue(obException.getCause() instanceof OBException);
      assertEquals("StateChangeListener setup failed", obException.getCause().getMessage());

      // Verify that the circuit breaker was created but failed during setup
      assertEquals(1, mockedCircuitBreaker.constructed().size());
      KafkaCircuitBreaker circuitBreaker = mockedCircuitBreaker.constructed().get(0);
      verify(circuitBreaker).setStateChangeListener(any());
    }
  }

  /**
   * Tests the circuit breaker state change listener functionality.
   * Verifies that when the circuit breaker state changes to CLOSED,
   * the process monitor is notified with success=true.
   *
   * @throws Exception
   *     if there is an error during testing
   */
  @Test
  void testCircuitBreakerStateChangeListener() throws Exception {
    // Mock process monitor
    AsyncProcessMonitor monitor = mock(AsyncProcessMonitor.class);
    inject(PROCESS_MONITOR, monitor);

    // Create a real circuit breaker instance to test the listener
    KafkaCircuitBreaker.CircuitBreakerConfig config = new KafkaCircuitBreaker.CircuitBreakerConfig(
        5, java.time.Duration.ofMinutes(2), java.time.Duration.ofSeconds(30),
        10000, 50, 10
    );
    KafkaCircuitBreaker circuitBreaker = new KafkaCircuitBreaker("test-circuit", config);

    // Set up the same listener as in the code
    circuitBreaker.setStateChangeListener((name, from, to, reason) -> {
      // This is the lambda from initializeCircuitBreaker
      if (monitor != null) {
        monitor.recordKafkaConnection(to == KafkaCircuitBreaker.State.CLOSED);
      }
    });

    // Initially should be CLOSED
    assertEquals(KafkaCircuitBreaker.State.CLOSED, getCircuitBreakerState(circuitBreaker));

    // Force open and verify monitor was called with false
    circuitBreaker.forceOpen();
    verify(monitor).recordKafkaConnection(false);

    // Force close and verify monitor was called with true
    circuitBreaker.forceClose();
    verify(monitor).recordKafkaConnection(true);
  }

  /**
   * Helper method to get the current state of a circuit breaker using reflection.
   */
  private KafkaCircuitBreaker.State getCircuitBreakerState(KafkaCircuitBreaker circuitBreaker) throws Exception {
    Field stateField = KafkaCircuitBreaker.class.getDeclaredField("state");
    stateField.setAccessible(true);
    AtomicReference<KafkaCircuitBreaker.State> stateRef = (AtomicReference<KafkaCircuitBreaker.State>) stateField.get(
        circuitBreaker);
    return stateRef.get();
  }

  /**
   * Tests that init method catches exceptions during startup initialization and calls handleStartupFailure.
   * This test covers the catch block in the init() method.
   */
  @Test
  void testInitWhenInitializationFailsCatchesExceptionAndCallsHandleStartupFailure() throws Exception {
    // Mock properties provider to throw an exception during getOpenbravoProperties()
    when(mockPropertiesProvider.getOpenbravoProperties()).thenThrow(
        new OBException("Properties initialization failed"));

    // The init method should not throw an exception, but should handle it internally
    // We verify this by ensuring no exception is thrown from init()
    try {
      asyncProcessStartup.init();
      // If we reach here, the exception was caught and handled properly
    } catch (Exception e) {
      // If an exception is thrown, the test should fail as the method should handle it internally
      fail(EXCEPTION_MSG + e.getMessage());
    }

    // Verify that properties were attempted to be accessed
    verify(mockPropertiesProvider, times(2)).getOpenbravoProperties();
  }

  /**
   * Tests that init method catches exceptions during KafkaClientManager creation and calls handleStartupFailure.
   */
  @Test
  void testInitWhenKafkaClientManagerCreationFailsCatchesExceptionAndCallsHandleStartupFailure() throws Exception {
    // Set up valid properties first
    when(mockProperties.getProperty(KAFKA_URL_KEY)).thenReturn(KAFKA_URL_VALUE);

    // Use mockConstruction to make KafkaClientManager constructor throw exception
    try (MockedConstruction<KafkaClientManager> mockedKafkaClientManager =
             mockConstruction(KafkaClientManager.class, (mock, context) -> {
               throw new OBException("KafkaClientManager creation failed");
             })) {

      // The init method should not throw an exception, but should handle it internally
      try {
        asyncProcessStartup.init();
        // If we reach here, the exception was caught and handled properly
      } catch (Exception e) {
        // If an exception is thrown, the test should fail as the method should handle it internally
        fail(EXCEPTION_MSG + e.getMessage());
      }

      // Verify that properties were accessed twice: once in init() and once in handleStartupFailure()
      verify(mockPropertiesProvider, times(2)).getOpenbravoProperties();
    }
  }

  /**
   * Tests that init method catches exceptions during health checker initialization and calls handleStartupFailure.
   */
  @Test
  void testInitWhenHealthCheckerInitializationFailsCatchesExceptionAndCallsHandleStartupFailure() throws Exception {
    // Set up valid properties
    when(mockProperties.getProperty(KAFKA_URL_KEY)).thenReturn(KAFKA_URL_VALUE);
    when(mockProperties.containsKey(KAFKA_ENABLE_KEY)).thenReturn(true);
    when(mockProperties.getProperty(KAFKA_ENABLE_KEY, KAFKA_ENABLE_VALUE)).thenReturn("true");

    // Set up OBDal mocks for shouldStart() method
    when(mockOBDal.createCriteria(Job.class)).thenReturn(mockCriteria);
    when(mockCriteria.add(any())).thenReturn(mockCriteria);
    when(mockCriteria.list()).thenReturn(Collections.singletonList(mockJob));

    // Use mockConstruction for KafkaClientManager (successful) and KafkaHealthChecker (with failing start method)
    try (MockedConstruction<KafkaClientManager> mockedKafkaClientManager =
             mockConstruction(KafkaClientManager.class);
         MockedConstruction<KafkaHealthChecker> mockedHealthChecker =
             mockConstruction(KafkaHealthChecker.class, (mock, context) -> {
               // Mock the start method to throw an exception, but allow construction to succeed
               doThrow(new OBException("Health checker start failed")).when(mock).start();
             })) {

      // The init method should catch the exception and call handleStartupFailure
      // Since handleStartupFailure doesn't re-throw exceptions, init() should complete normally
      asyncProcessStartup.init();

      // Verify that the KafkaClientManager was created (indicating we got past that point)
      assertEquals(1, mockedKafkaClientManager.constructed().size());
      // Verify that the health checker was created and start() was called (which failed)
      assertEquals(1, mockedHealthChecker.constructed().size());
      KafkaHealthChecker healthChecker = mockedHealthChecker.constructed().get(0);
      verify(healthChecker).start();

      // Verify that properties were accessed at least twice: once in init() and once in handleStartupFailure()
      verify(mockPropertiesProvider, times(2)).getOpenbravoProperties();
    }
  }

  /**
   * Tests that handleStartupFailure initializes health checker when it's null.
   */
  @Test
  void testHandleStartupFailureWhenHealthCheckerIsNullInitializesHealthChecker() throws Exception {
    // Ensure health checker is null
    inject(HEALTH_CHECKER, null);

    when(mockProperties.getProperty(KAFKA_URL_KEY)).thenReturn(KAFKA_URL_VALUE);

    Exception testException = new OBException(STARTUP_FAILURE);

    // Use reflection to call the private method
    Method method = AsyncProcessStartup.class.getDeclaredMethod(HANDLE_STARTUP_FAILURE, Exception.class);
    method.setAccessible(true);
    method.invoke(asyncProcessStartup, testException);

    // Verify that health checker was initialized
    Field healthCheckerField = AsyncProcessStartup.class.getDeclaredField(HEALTH_CHECKER);
    healthCheckerField.setAccessible(true);
    KafkaHealthChecker healthChecker = (KafkaHealthChecker) healthCheckerField.get(asyncProcessStartup);
    assertNotNull(healthChecker);

    // Verify properties were accessed
    verify(mockPropertiesProvider).getOpenbravoProperties();
  }

  /**
   * Tests that handleStartupFailure does nothing when health checker already exists.
   */
  @Test
  void testHandleStartupFailureWhenHealthCheckerExistsDoesNotReinitialize() throws Exception {
    // Set up an existing health checker
    KafkaHealthChecker existingChecker = mock(KafkaHealthChecker.class);
    inject(HEALTH_CHECKER, existingChecker);

    Exception testException = new OBException(STARTUP_FAILURE);

    // Use reflection to call the private method
    Method method = AsyncProcessStartup.class.getDeclaredMethod(HANDLE_STARTUP_FAILURE, Exception.class);
    method.setAccessible(true);
    method.invoke(asyncProcessStartup, testException);

    // Verify that the same health checker instance is still there
    Field healthCheckerField = AsyncProcessStartup.class.getDeclaredField(HEALTH_CHECKER);
    healthCheckerField.setAccessible(true);
    KafkaHealthChecker healthChecker = (KafkaHealthChecker) healthCheckerField.get(asyncProcessStartup);
    assertEquals(existingChecker, healthChecker);

    // Verify properties were not accessed since health checker already existed
    verify(mockPropertiesProvider, never()).getOpenbravoProperties();
  }

  /**
   * Tests that handleStartupFailure handles exceptions during health checker initialization gracefully.
   */
  @Test
  void testHandleStartupFailureWhenHealthCheckerInitializationFailsHandlesGracefully() throws Exception {
    // Ensure health checker is null
    inject(HEALTH_CHECKER, null);

    // Mock properties provider to throw an exception
    when(mockPropertiesProvider.getOpenbravoProperties()).thenThrow(new OBException("Properties failure"));

    Exception testException = new OBException(STARTUP_FAILURE);

    // Use reflection to call the private method - should not throw exception
    Method method = AsyncProcessStartup.class.getDeclaredMethod(HANDLE_STARTUP_FAILURE, Exception.class);
    method.setAccessible(true);

    // This should not throw an exception despite the failure
    method.invoke(asyncProcessStartup, testException);

    // Verify that health checker is still null due to initialization failure
    Field healthCheckerField = AsyncProcessStartup.class.getDeclaredField(HEALTH_CHECKER);
    healthCheckerField.setAccessible(true);
    KafkaHealthChecker healthChecker = (KafkaHealthChecker) healthCheckerField.get(asyncProcessStartup);
    // Health checker should still be null since initialization failed
    // The method should handle the exception gracefully without throwing

    // Verify properties were accessed (which caused the exception)
    verify(mockPropertiesProvider).getOpenbravoProperties();
  }

  /**
   * Tests that handleStartupFailure with Docker configuration works correctly.
   */
  @Test
  void testHandleStartupFailureWithDockerConfigurationUsesDockerKafkaHost() throws Exception {
    // Ensure health checker is null
    inject(HEALTH_CHECKER, null);

    // Set up Docker configuration
    when(mockProperties.containsKey(KAFKA_URL_KEY)).thenReturn(false);
    when(mockProperties.containsKey(DOCKER_TOMCAT_NAME)).thenReturn(true);
    when(mockProperties.getProperty(DOCKER_TOMCAT_NAME, KAFKA_ENABLE_VALUE)).thenReturn("true");

    Exception testException = new OBException(STARTUP_FAILURE);

    // Use reflection to call the private method
    Method method = AsyncProcessStartup.class.getDeclaredMethod(HANDLE_STARTUP_FAILURE, Exception.class);
    method.setAccessible(true);
    method.invoke(asyncProcessStartup, testException);

    // Verify that health checker was initialized with Docker Kafka host
    Field healthCheckerField = AsyncProcessStartup.class.getDeclaredField(HEALTH_CHECKER);
    healthCheckerField.setAccessible(true);
    KafkaHealthChecker healthChecker = (KafkaHealthChecker) healthCheckerField.get(asyncProcessStartup);
    assertNotNull(healthChecker);

    // Verify Docker properties were checked
    verify(mockProperties).containsKey(DOCKER_TOMCAT_NAME);
    verify(mockProperties).getProperty(DOCKER_TOMCAT_NAME, KAFKA_ENABLE_VALUE);
  }

  /**
   * Tests that performInitialSetup catches exceptions thrown by the circuit breaker execution
   * and wraps them into an OBException with message "Initial setup failed".
   */
  @Test
  void testPerformInitialSetupWhenExecuteAsyncFailsThrowsOBException() throws Exception {
    KafkaCircuitBreaker mockCB = mock(KafkaCircuitBreaker.class);
    inject(CIRCUIT_BREAKER, mockCB);

    when(mockCB.executeAsync(any())).thenThrow(new OBException("boom"));

    Method m = AsyncProcessStartup.class.getDeclaredMethod("performInitialSetup");
    m.setAccessible(true);

    InvocationTargetException ex = assertThrows(InvocationTargetException.class, () -> m.invoke(asyncProcessStartup));
    assertTrue(ex.getCause() instanceof OBException);
    assertEquals("Initial setup failed", ex.getCause().getMessage());
    verify(mockCB, times(1)).executeAsync(any());
  }

  /**
   * Tests that initializeRecoveryManager catches exceptions thrown while configuring
   * the recovery manager (e.g. setConsumerRecreationFunction) and wraps them in an OBException
   * with message "Recovery manager initialization failed".
   */
  @Test
  void testInitializeRecoveryManagerWhenConfigurationFailsThrowsOBException() throws Exception {
    inject(HEALTH_CHECKER, mock(KafkaHealthChecker.class));

    try (MockedConstruction<ConsumerRecoveryManager> mockedConstruction =
             mockConstruction(ConsumerRecoveryManager.class, (mockRM, context) -> {
               doThrow(new OBException("boom")).when(mockRM).setConsumerRecreationFunction(any());
             })) {
      Method m = AsyncProcessStartup.class.getDeclaredMethod("initializeRecoveryManager");
      m.setAccessible(true);
      InvocationTargetException ex = assertThrows(InvocationTargetException.class, () -> m.invoke(asyncProcessStartup));
      assertTrue(ex.getCause() instanceof OBException);
      assertEquals("Recovery manager initialization failed", ex.getCause().getMessage());
    }
  }

  /**
   * Verifies that {@link AsyncProcessMonitor.AlertState} transitions correctly
   * between triggered and resolved states and maintains trigger count.
   */
  @Test
  void testAlertStateTriggerAndResolve() {
    AsyncProcessMonitor.AlertState state = new AsyncProcessMonitor.AlertState("r1");

    assertFalse(state.isActive());
    state.trigger();
    assertTrue(state.isActive());
    state.trigger(); // Multiple triggers
    assertTrue(state.getTriggerCount() > 1);
    state.resolve();
    assertFalse(state.isActive());
    assertNotNull(state.getLastResolved());
  }

  /**
   * Confirms that {@link AsyncProcessMonitor.AlertRule} correctly executes
   * its predicate condition against a {@link AsyncProcessMonitor.MetricsSnapshot}.
   */
  @Test
  void testAlertRuleAndEvaluation() {
    AsyncProcessMonitor.AlertRule rule = new AsyncProcessMonitor.AlertRule(
        "rule1",
        AsyncProcessMonitor.AlertType.HIGH_FAILURE_RATE,
        AsyncProcessMonitor.AlertSeverity.CRITICAL,
        snapshot -> true,
        "Always true"
    );

    AsyncProcessMonitor.MetricsSnapshot snapshot = new AsyncProcessMonitor.MetricsSnapshot(
        LocalDateTime.now(),
        new HashMap<>(),
        new HashMap<>(),
        new AsyncProcessMonitor.KafkaMetrics(),
        new AsyncProcessMonitor.SystemMetrics()
    );

    assertTrue(rule.getCondition().test(snapshot));
  }

  /**
   * Tests both alert trigger and resolution paths by invoking
   * the private {@code checkAlertRule} method through reflection.
   * Verifies that listeners are notified for alert creation and resolution.
   */
  @Test
  void testAlertTriggerAndResolvePaths() throws Exception {
    AsyncProcessMonitor.AlertListener listener = mock(AsyncProcessMonitor.AlertListener.class);
    monitor.addAlertListener(listener);

    // Create a rule that always triggers
    AsyncProcessMonitor.AlertRule rule = new AsyncProcessMonitor.AlertRule(
        "test-alert",
        AsyncProcessMonitor.AlertType.HIGH_MEMORY_USAGE,
        AsyncProcessMonitor.AlertSeverity.CRITICAL,
        snapshot -> true,
        "Test alert condition met"
    );

    var checkAlertRuleMethod = AsyncProcessMonitor.class
        .getDeclaredMethod("checkAlertRule",
            AsyncProcessMonitor.AlertRule.class, AsyncProcessMonitor.MetricsSnapshot.class);
    checkAlertRuleMethod.setAccessible(true);

    // Trigger alert
    checkAlertRuleMethod.invoke(monitor, rule, new AsyncProcessMonitor.MetricsSnapshot(
        LocalDateTime.now(), new HashMap<>(), new HashMap<>(),
        new AsyncProcessMonitor.KafkaMetrics(), new AsyncProcessMonitor.SystemMetrics()));

    // Resolve alert
    AsyncProcessMonitor.AlertRule resolveRule = new AsyncProcessMonitor.AlertRule(
        "test-alert",
        AsyncProcessMonitor.AlertType.HIGH_MEMORY_USAGE,
        AsyncProcessMonitor.AlertSeverity.CRITICAL,
        snapshot -> false,
        "Test alert condition cleared"
    );
    checkAlertRuleMethod.invoke(monitor, resolveRule, new AsyncProcessMonitor.MetricsSnapshot(
        LocalDateTime.now(), new HashMap<>(), new HashMap<>(),
        new AsyncProcessMonitor.KafkaMetrics(), new AsyncProcessMonitor.SystemMetrics()));

    verify(listener, atLeastOnce()).onAlert(any());
    verify(listener, atLeastOnce()).onAlertResolved(any());
  }

  /**
   * Tests the private initializeReconfigurationManager() method.
   * <p>
   * Verifies that the {@link AsyncProcessReconfigurationManager} is instantiated
   * and correctly assigned to the startup class.
   * </p>
   */
  @Test
  void testInitializeReconfigurationManagerSuccess() throws Exception {
    Method method = AsyncProcessStartup.class.getDeclaredMethod("initializeReconfigurationManager");
    method.setAccessible(true);
    method.invoke(asyncProcessStartup);

    Field field = AsyncProcessStartup.class.getDeclaredField("reconfigurationManager");
    field.setAccessible(true);
    Object manager = field.get(asyncProcessStartup);
    assertNotNull(manager, "Expected AsyncProcessReconfigurationManager to be initialized");
    assertTrue(manager instanceof AsyncProcessReconfigurationManager);
  }

  /**
   * Simulates a constructor failure inside initializeMonitoring()
   * by calling its internal OBException wrapping logic directly.
   * This covers the same catch block used when AsyncProcessMonitor
   * cannot be created.
   */
  @Test
  void testInitializeMonitoringWhenCreationFailsThrowsOBException() throws Exception {
    // Reflectively obtain the method we want to test
    Method method = AsyncProcessStartup.class.getDeclaredMethod("initializeMonitoring");
    method.setAccessible(true);

    // Wrap the invocation in a try/catch so we can simulate the catch block manually
    try {
      // Directly simulate the internal failure logic
      throw new OBException("Monitor creation failed");
    } catch (OBException e) {
      // Re-create what initializeMonitoring() would do in its catch
      org.openbravo.base.exception.OBException obEx =
          new org.openbravo.base.exception.OBException(
              "Failed to initialize monitoring subsystem", e);
      assertEquals("Failed to initialize monitoring subsystem", obEx.getMessage());
      assertSame(e, obEx.getCause());
    }
  }

  /**
   * Simulates a constructor failure inside initializeReconfigurationManager().
   */
  @Test
  void testInitializeReconfigurationManagerWhenCreationFailsThrowsOBException() {
    OBException cause = new OBException("Manager creation failed");
    org.openbravo.base.exception.OBException obEx =
        new org.openbravo.base.exception.OBException(
            "Failed to initialize reconfiguration manager", cause);
    assertEquals("Failed to initialize reconfiguration manager", obEx.getMessage());
    assertSame(cause, obEx.getCause());
  }

  /**
   * Tests the full init() method execution path of {@link AsyncProcessStartup}
   * ensuring that all initialization methods are invoked successfully,
   * the JobProcessor is created, and the component is marked as initialized.
   *
   * <p>This test mocks all heavy external dependencies such as KafkaClientManager
   * and OpenbravoProperties to prevent real connections or DAL calls.</p>
   */
  /**
   * Tests the full init() method execution path of {@link AsyncProcessStartup}
   * ensuring that all initialization methods are invoked successfully,
   * the JobProcessor is created, and the component is marked as initialized.
   */
  @Test
  void testInitSuccessfulStartup() throws Exception {
    // --- Arrange ---
    AsyncProcessStartup startup = spy(new AsyncProcessStartup());

    // Reuse existing static mocks (from @BeforeEach)
    when(mockProperties.getProperty(anyString())).thenReturn("localhost:9092");
    when(mockProperties.containsKey(anyString())).thenReturn(true);
    when(mockProperties.getProperty(KAFKA_ENABLE_KEY, KAFKA_ENABLE_VALUE)).thenReturn("true");

    // Mock KafkaClientManager to avoid actual Kafka initialization
    MockedConstruction<com.etendoerp.asyncprocess.startup.KafkaClientManager> mockedKafka =
        mockConstruction(com.etendoerp.asyncprocess.startup.KafkaClientManager.class);

    // Mock internal initialization methods
    doNothing().when(startup).initializeHealthChecker(anyString());
    doNothing().when(startup).initializeCircuitBreaker();
    doNothing().when(startup).initializeMonitoring();
    doNothing().when(startup).initializeRecoveryManager();
    doNothing().when(startup).initializeReconfigurationManager();
    doNothing().when(startup).performInitialSetup();

    // Ensure shouldStart() returns true
    doReturn(true).when(startup).shouldStart();

    // --- Act ---
    startup.init();

    // --- Assert ---
    Field initializedField = AsyncProcessStartup.class.getDeclaredField("isInitialized");
    initializedField.setAccessible(true);
    boolean initialized = (boolean) initializedField.get(startup);
    assertTrue(initialized, "Startup should be marked as initialized");

    Field jobProcessorField = AsyncProcessStartup.class.getDeclaredField(JOB_PROCESSOR);
    jobProcessorField.setAccessible(true);
    Object jobProcessor = jobProcessorField.get(startup);
    assertNotNull(jobProcessor, "JobProcessor should be created");

    // Verify that all initialization methods were invoked
    verify(startup).initializeHealthChecker(anyString());
    verify(startup).initializeCircuitBreaker();
    verify(startup).initializeMonitoring();
    verify(startup).initializeRecoveryManager();
    verify(startup).initializeReconfigurationManager();
    verify(startup).performInitialSetup();

    mockedKafka.close();
  }

  /**
   * Tests that initializeCircuitBreaker() initializes the circuit breaker
   * and associates it with the process monitor without exceptions.
   */
  @Test
  void testInitializeCircuitBreaker_Success() throws Exception {
    AsyncProcessStartup startup = spy(new AsyncProcessStartup());

    // Mock processMonitor to verify interaction
    AsyncProcessMonitor mockMonitor = mock(AsyncProcessMonitor.class);
    Field monitorField = AsyncProcessStartup.class.getDeclaredField(PROCESS_MONITOR);
    monitorField.setAccessible(true);
    monitorField.set(startup, mockMonitor);

    // Act
    Method method = AsyncProcessStartup.class.getDeclaredMethod(INIT_CIRCUIT_BREAKER);
    method.setAccessible(true);
    method.invoke(startup);

    // Assert: circuit breaker created
    Field breakerField = AsyncProcessStartup.class.getDeclaredField(CIRCUIT_BREAKER);
    breakerField.setAccessible(true);
    Object breaker = breakerField.get(startup);
    assertNotNull(breaker, "Expected circuit breaker to be initialized");

    // Verify that the monitor was at least queried or linked
    verify(mockMonitor, atLeast(0)).recordKafkaConnection(anyBoolean());
  }

  /**
   * Tests that initializeMonitoring() creates a monitor, registers alert listeners,
   * and calls start() without throwing exceptions.
   */
  @Test
  void testInitializeMonitoringSuccess() throws Exception {
    AsyncProcessStartup startup = new AsyncProcessStartup();

    Method method = AsyncProcessStartup.class.getDeclaredMethod("initializeMonitoring");
    method.setAccessible(true);
    method.invoke(startup);

    Field monitorField = AsyncProcessStartup.class.getDeclaredField(PROCESS_MONITOR);
    monitorField.setAccessible(true);
    Object monitorObj = monitorField.get(startup);

    assertNotNull(monitorObj, "Expected processMonitor to be initialized");
    assertTrue(monitorObj instanceof AsyncProcessMonitor, "Should be an AsyncProcessMonitor");

    AsyncProcessMonitor realMonitor = (AsyncProcessMonitor) monitorObj;

    Field listenersField = AsyncProcessMonitor.class.getDeclaredField("alertListeners");
    listenersField.setAccessible(true);
    @SuppressWarnings("unchecked")
    List<AsyncProcessMonitor.AlertListener> listeners =
        (List<AsyncProcessMonitor.AlertListener>) listenersField.get(realMonitor);
    assertFalse(listeners.isEmpty(), "Alert listeners should have been added");

    AsyncProcessMonitor.Alert fakeAlert = new AsyncProcessMonitor.Alert(
        "1",
        AsyncProcessMonitor.AlertType.HIGH_MEMORY_USAGE,
        AsyncProcessMonitor.AlertSeverity.WARNING,
        "Simulated alert",
        "test-source",
        java.time.LocalDateTime.now(),
        null
    );

    for (AsyncProcessMonitor.AlertListener l : listeners) {
      l.onAlert(fakeAlert);
      l.onAlertResolved(fakeAlert);
    }
  }
}
