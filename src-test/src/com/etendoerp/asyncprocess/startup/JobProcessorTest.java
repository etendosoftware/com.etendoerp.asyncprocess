package com.etendoerp.asyncprocess.startup;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.openbravo.base.session.SessionFactoryController;
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.core.SessionHandler;
import org.openbravo.dal.service.OBCriteria;
import org.openbravo.dal.service.OBDal;

import com.etendoerp.asyncprocess.config.AsyncProcessConfig;
import com.etendoerp.asyncprocess.health.KafkaHealthChecker;
import com.etendoerp.asyncprocess.model.AsyncProcessExecution;
import com.etendoerp.asyncprocess.monitoring.AsyncProcessMonitor;
import com.etendoerp.asyncprocess.recovery.ConsumerRecoveryManager;
import com.smf.jobs.Action;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;

@MockitoSettings(strictness = Strictness.LENIENT)
@ExtendWith(MockitoExtension.class)
class JobProcessorTest {
  private static final Logger log = LogManager.getLogger();
  public static final String JOB_SCHEDULERS = "jobSchedulers";
  public static final String ETAP_PARALLEL_THREADS = "etapParallelThreads";
  public static final String CONFIGURE_JOB_SCHEDULER = "configureJobScheduler";
  public static final String NOTANUMBER = "notanumber";
  public static final String ETAP_MAX_RETRIES = "etapMaxRetries";
  public static final String ETAP_RETRY_DELAY_MS = "etapRetryDelayMs";
  public static final String ETAP_PREFETCH_COUNT = "etapPrefetchCount";
  public static final String GET_JOB_LINE_CONFIG = "getJobLineConfig";
  public static final String TEST_JOB = "TestJob";
  public static final String CONSUMER_CREATION_CONFIG = "com.etendoerp.asyncprocess.startup.JobProcessor$ConsumerCreationConfig";
  public static final String TEST_JOB_ID = "test-job-id";
  public static final String TEST_CLIENT_ID = "test-client-id";
  public static final String TEST_ORG_ID = "test-org-id";
  public static final String TEST_JOBLINE_ID = "test-jobline-id";
  public static final String TEST_TOPIC = "test-topic";
  public static final String TEST_JOBLINE_ID_0 = "test-jobline-id-0";
  public static final String CREATE_ACTION_FACTORY = "createActionFactory";
  public static final String ACTION_CLASS = "com.smf.jobs.Action";
  public static final String GET_JOB_PARALLEL_THREADS = "getJobParallelThreads";
  public static final String CONVERT_STATE = "convertState";

  @Mock private AsyncProcessMonitor processMonitor;
  @Mock private ConsumerRecoveryManager recoveryManager;
  @Mock private KafkaHealthChecker healthChecker;
  @Mock private KafkaClientManager kafkaClientManager;
  @Mock private Disposable disposable;

  private Map<String, Disposable> activeSubscriptions;
  private JobProcessor jobProcessor;
  private SessionFactoryController originalSessionFactoryController;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    originalSessionFactoryController = SessionFactoryController.getInstance();
    try {
      SessionFactoryController.setInstance(new TestSessionFactoryController());
    } catch (Exception setupException) {
      throw new RuntimeException("Failed to setup SessionFactoryController for test", setupException);
    }
    activeSubscriptions = new HashMap<>();
    jobProcessor = new JobProcessor(processMonitor, recoveryManager, healthChecker, activeSubscriptions, kafkaClientManager);
  }

  @AfterEach
  void tearDown() {
    try {
      if (SessionHandler.isSessionHandlerPresent() && SessionHandler.getInstance().isCurrentTransactionActive()) {
        SessionHandler.getInstance().commitAndClose();
      }
    } catch (Exception e) {
      log.error("Warning: Error closing session during test teardown: {}", e.getMessage());
    }
    SessionFactoryController.setInstance(originalSessionFactoryController);
  }

  @Test
  void testConstructorInitializesFields() {
    assertNotNull(jobProcessor);
  }

  @Test
  void testShutdownSchedulers() throws Exception {
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    when(scheduler.awaitTermination(anyLong(), any())).thenReturn(true);
    Field schedulersField = JobProcessor.class.getDeclaredField(JOB_SCHEDULERS);
    schedulersField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, ScheduledExecutorService> schedulers = (Map<String, ScheduledExecutorService>) schedulersField.get(jobProcessor);
    schedulers.put("testJob", scheduler);

    assertDoesNotThrow(() -> jobProcessor.shutdownSchedulers());
    assertTrue(schedulers.isEmpty());

    // Test with InterruptedException
    when(scheduler.awaitTermination(anyLong(), any())).thenThrow(new InterruptedException());
    schedulers.put("testJob", scheduler);
    assertDoesNotThrow(() -> jobProcessor.shutdownSchedulers());
    assertTrue(schedulers.isEmpty());
  }

  @Test
  void testConfigureJobScheduler() throws Exception {
    com.smf.jobs.model.Job job = mock(com.smf.jobs.model.Job.class);
    when(job.getId()).thenReturn("job1");
    when(job.get(ETAP_PARALLEL_THREADS)).thenReturn("2");
    invokePrivateMethod(jobProcessor, CONFIGURE_JOB_SCHEDULER, new Class<?>[]{com.smf.jobs.model.Job.class}, job);

    Field schedulersField = JobProcessor.class.getDeclaredField(JOB_SCHEDULERS);
    schedulersField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, ScheduledExecutorService> schedulers = (Map<String, ScheduledExecutorService>) schedulersField.get(jobProcessor);
    assertTrue(schedulers.containsKey("job1"));
  }

  @Test
  void testGetJobParallelThreads() {
    com.smf.jobs.model.Job job = mock(com.smf.jobs.model.Job.class);
    when(job.getId()).thenReturn("job1");

    // Test case 1: Valid number
    when(job.get(ETAP_PARALLEL_THREADS)).thenReturn("4");
    assertEquals(4, (int) invokePrivateMethod(jobProcessor, GET_JOB_PARALLEL_THREADS, new Class<?>[]{com.smf.jobs.model.Job.class}, job));

    // Test case 2: Null value
    when(job.get(ETAP_PARALLEL_THREADS)).thenReturn(null);
    assertEquals(8, (int) invokePrivateMethod(jobProcessor, GET_JOB_PARALLEL_THREADS, new Class<?>[]{com.smf.jobs.model.Job.class}, job));

    // Test case 3: Not a number
    when(job.get(ETAP_PARALLEL_THREADS)).thenReturn(NOTANUMBER);
    assertEquals(8, (int) invokePrivateMethod(jobProcessor, GET_JOB_PARALLEL_THREADS, new Class<?>[]{com.smf.jobs.model.Job.class}, job));
  }

  @Test
  void testGetJobLineConfigWithValidValues() {
    com.smf.jobs.model.JobLine jobLine = mock(com.smf.jobs.model.JobLine.class);
    when(jobLine.get(ETAP_MAX_RETRIES)).thenReturn("5");
    when(jobLine.get(ETAP_RETRY_DELAY_MS)).thenReturn("2000");
    when(jobLine.get(ETAP_PREFETCH_COUNT)).thenReturn("3");
    AsyncProcessConfig config = (AsyncProcessConfig) invokePrivateMethod(jobProcessor, GET_JOB_LINE_CONFIG, new Class<?>[]{com.smf.jobs.model.JobLine.class}, jobLine);
    assertNotNull(config);
    assertEquals(5, config.getMaxRetries());
    assertEquals(2000L, config.getRetryDelayMs());
    assertEquals(3, config.getPrefetchCount());
  }

  @Test
  void testGetJobLineConfigWithDefaults() {
    com.smf.jobs.model.JobLine jobLine = mock(com.smf.jobs.model.JobLine.class);
    when(jobLine.get(ETAP_MAX_RETRIES)).thenReturn(null);
    when(jobLine.get(ETAP_RETRY_DELAY_MS)).thenReturn(NOTANUMBER);
    when(jobLine.get(ETAP_PREFETCH_COUNT)).thenReturn(null);
    AsyncProcessConfig defaultConfig = (AsyncProcessConfig) invokePrivateMethod(jobProcessor, GET_JOB_LINE_CONFIG, new Class<?>[]{com.smf.jobs.model.JobLine.class}, jobLine);
    assertNotNull(defaultConfig);
    // Use the actual default values from JobProcessor instead of creating a new AsyncProcessConfig
    assertEquals(3, defaultConfig.getMaxRetries()); // DEFAULT_MAX_RETRIES = 3
    assertEquals(1000L, defaultConfig.getRetryDelayMs()); // DEFAULT_RETRY_DELAY = 1000
    assertEquals(1, defaultConfig.getPrefetchCount()); // DEFAULT_PREFETCH_COUNT = 1
  }


  @Test
  void testCalculateTopics() {
    com.smf.jobs.model.Job job = mock(com.smf.jobs.model.Job.class);
    com.smf.jobs.model.JobLine jobLine = mock(com.smf.jobs.model.JobLine.class);
    when(job.getName()).thenReturn(TEST_JOB);
    when(jobLine.getJobsJob()).thenReturn(job);
    List<com.smf.jobs.model.JobLine> jobLines = List.of(jobLine);

    // Error Topic
    when(job.getEtapErrortopic()).thenReturn(null, "customTopic");
    assertNotNull(invokePrivateMethod(jobProcessor, "calculateErrorTopic", new Class<?>[]{com.smf.jobs.model.Job.class}, job));
    assertEquals("customTopic", invokePrivateMethod(jobProcessor, "calculateErrorTopic", new Class<?>[]{com.smf.jobs.model.Job.class}, job));

    // Next Topic
    when(jobLine.getEtapTargettopic()).thenReturn(null, "nextTopic");
    assertNotNull(invokePrivateMethod(jobProcessor, "calculateNextTopic", new Class<?>[]{com.smf.jobs.model.JobLine.class, List.class}, jobLine, jobLines));
    assertEquals("nextTopic", invokePrivateMethod(jobProcessor, "calculateNextTopic", new Class<?>[]{com.smf.jobs.model.JobLine.class, List.class}, jobLine, jobLines));

    // Current Topic
    when(job.getEtapInitialTopic()).thenReturn(null, "initTopic");
    assertNotNull(invokePrivateMethod(jobProcessor, "calculateCurrentTopic", new Class<?>[]{com.smf.jobs.model.JobLine.class, List.class}, jobLine, jobLines));
    assertEquals("initTopic", invokePrivateMethod(jobProcessor, "calculateCurrentTopic", new Class<?>[]{com.smf.jobs.model.JobLine.class, List.class}, jobLine, jobLines));
  }

  @Test
  void testConvertState() {
    assertNotNull(invokePrivateMethod(jobProcessor, CONVERT_STATE, new Class<?>[]{String.class}, "STARTED"));
    assertNotNull(invokePrivateMethod(jobProcessor, CONVERT_STATE, new Class<?>[]{String.class}, "INVALID"));
    assertNotNull(invokePrivateMethod(jobProcessor, CONVERT_STATE, new Class<?>[]{String.class}, new Object[]{null}));
  }

  @Test
  void testHandleJobProcessingError() {
    assertDoesNotThrow(() -> invokePrivateMethod(jobProcessor, "handleJobProcessingError", new Class<?>[]{Throwable.class}, new RuntimeException("test error")));
  }

  @Test
  void testGetGroupId() {
    com.smf.jobs.model.Job job = mock(com.smf.jobs.model.Job.class);
    when(job.getName()).thenReturn("Test.Job_1");
    com.smf.jobs.model.JobLine jobLine = mock(com.smf.jobs.model.JobLine.class);
    when(jobLine.getJobsJob()).thenReturn(job);
    String groupId = (String) invokePrivateMethod(null, "getGroupId", new Class<?>[]{com.smf.jobs.model.JobLine.class}, jobLine);
    assertTrue(groupId.contains("test-job-1"));
  }

  @Test
  void testSetupTopicAndPartitionsAndCalculateConsumerCount() {
    com.smf.jobs.model.Job job = mock(com.smf.jobs.model.Job.class);
    com.smf.jobs.model.JobLine jobLine = mock(com.smf.jobs.model.JobLine.class);
    when(job.isEtapConsumerPerPartition()).thenReturn(false, true);
    when(kafkaClientManager.getNumPartitions()).thenReturn(3);

    assertEquals(1, (int) invokePrivateMethod(jobProcessor, "calculateConsumerCount", new Class<?>[]{com.smf.jobs.model.Job.class, com.smf.jobs.model.JobLine.class}, job, jobLine));
    assertEquals(3, (int) invokePrivateMethod(jobProcessor, "calculateConsumerCount", new Class<?>[]{com.smf.jobs.model.Job.class, com.smf.jobs.model.JobLine.class}, job, jobLine));

    AdminClient adminClient = mock(AdminClient.class);
    assertDoesNotThrow(() -> invokePrivateMethod(jobProcessor, "setupTopicAndPartitions", new Class<?>[]{AdminClient.class, String.class}, adminClient, "topic1"));
  }

  @Test
  void testNewInstanceGetThrowsExceptionForInterface() throws Exception {
    Class<?> newInstanceClass = Class.forName("com.etendoerp.asyncprocess.startup.JobProcessor$NewInstance");
    Constructor<?> ctor = newInstanceClass.getDeclaredConstructor(Class.class);
    ctor.setAccessible(true);
    Object newInstance = ctor.newInstance(Action.class);

    assertThrows(RuntimeException.class, () -> {
      invokePrivateMethod(newInstance, "get", new Class<?>[0]);
    }, "Expected get() to throw an exception when trying to instantiate an interface without a Weld context.");
  }

  @Test
  void testCreateAndConfigureConsumerSuccess() throws Exception {
    var setup = setupConsumerTest(true);

    when(setup.mockReceiver.doOnNext(any(Consumer.class))).thenAnswer(inv -> {
      Consumer<ReceiverRecord<String, AsyncProcessExecution>> callback = inv.getArgument(0);
      callback.accept(setup.mockRecord);
      return setup.mockReceiver;
    });

    when(kafkaClientManager.createReceiver(anyString(), anyBoolean(), any(AsyncProcessConfig.class), anyString())).thenReturn(setup.mockReceiver);
    Flux<ReceiverRecord<String, AsyncProcessExecution>> result = invokeCreateAndConfigureConsumer(setup.creationConfig, 0);

    assertNotNull(result, "Consumer should be created successfully");
    assertTrue(activeSubscriptions.containsKey(TEST_JOBLINE_ID_0), "Active subscription should be registered");
    verify(processMonitor, times(1)).recordConsumerActivity(anyString(), anyString(), anyString());
    verify(recoveryManager, times(1)).registerConsumer(any());
    verify(healthChecker, times(1)).registerConsumerGroup(anyString());
  }

  @Test
  void testCreateAndConfigureConsumerNullActionFactory() throws Exception {
    var setup = setupConsumerTest(false);
    Flux<ReceiverRecord<String, AsyncProcessExecution>> result = invokeCreateAndConfigureConsumer(setup.creationConfig, 0);

    assertNull(result, "Should return null when action factory is not found");
    assertTrue(activeSubscriptions.isEmpty(), "No active subscriptions should be registered");
    verify(processMonitor, never()).recordConsumerActivity(anyString(), anyString(), anyString());
  }

  @Test
  void testConsumerActivityMonitoring() throws Exception {
    var setup = setupConsumerTest(true);
    when(setup.mockReceiver.doOnNext(any(Consumer.class))).thenAnswer(inv -> {
      Consumer<ReceiverRecord<String, AsyncProcessExecution>> callback = inv.getArgument(0);
      callback.accept(setup.mockRecord);
      return setup.mockReceiver;
    });
    when(kafkaClientManager.createReceiver(anyString(), anyBoolean(), any(AsyncProcessConfig.class), anyString())).thenReturn(setup.mockReceiver);
    invokeCreateAndConfigureConsumer(setup.creationConfig, 0);

    verify(processMonitor, times(1)).recordConsumerActivity(TEST_JOBLINE_ID_0, "etendo-ap-group-testjob", TEST_TOPIC);
  }

  @Test
  void testConsumerErrorHandling() throws Exception {
    var setup = setupConsumerTest(true);
    RuntimeException testError = new RuntimeException("Connection lost");
    when(setup.mockReceiver.doOnError(any(Consumer.class))).thenAnswer(inv -> {
      Consumer<Throwable> callback = inv.getArgument(0);
      callback.accept(testError);
      return setup.mockReceiver;
    });
    when(kafkaClientManager.createReceiver(anyString(), anyBoolean(), any(AsyncProcessConfig.class), anyString())).thenReturn(setup.mockReceiver);
    invokeCreateAndConfigureConsumer(setup.creationConfig, 0);

    verify(processMonitor, times(1)).recordConsumerConnectionLost(TEST_JOBLINE_ID_0);
  }

  @Test
  void testCreateSingleConsumerSafely_whenCreateReceiverThrows_returnsNull() throws Exception {
    // Prepare job and jobline
    com.smf.jobs.model.Job job = mock(com.smf.jobs.model.Job.class);
    com.smf.jobs.model.JobLine jobLine = mock(com.smf.jobs.model.JobLine.class);
    org.openbravo.model.ad.system.Client client = mock(org.openbravo.model.ad.system.Client.class);
    org.openbravo.model.common.enterprise.Organization org = mock(org.openbravo.model.common.enterprise.Organization.class);

    when(job.getId()).thenReturn("job-ex");
    when(job.getName()).thenReturn("JobEx");
    when(job.getClient()).thenReturn(client);
    when(job.getOrganization()).thenReturn(org);
    when(client.getId()).thenReturn("client-ex");
    when(org.getId()).thenReturn("org-ex");
    when(jobLine.getId()).thenReturn("jobline-ex");
    when(jobLine.getJobsJob()).thenReturn(job);
    when(jobLine.getLineNo()).thenReturn(10L);

    // Action supplier present so failure happens later (at receiver creation)
    Map<String, Supplier<Action>> actionSuppliers = new HashMap<>();
    actionSuppliers.put("jobline-ex", () -> mock(Action.class));

    AsyncProcessConfig cfg = new AsyncProcessConfig();
    KafkaSender<String, AsyncProcessExecution> kafkaSender = mock(KafkaSender.class);

    Object creationConfig = createConsumerCreationConfig(job, jobLine, List.of(jobLine), "topic-ex", cfg, kafkaSender, actionSuppliers);

    // Force createAndConfigureConsumer to throw inside by making createReceiver throw
    when(kafkaClientManager.createReceiver(anyString(), anyBoolean(), any(AsyncProcessConfig.class), anyString()))
        .thenThrow(new RuntimeException("receiver boom"));

    Object result = invokePrivateMethod(jobProcessor, "createSingleConsumerSafely", new Class<?>[]{getConsumerConfigClass(), int.class}, creationConfig, 0);

    assertNull(result, "Expected null receiver when exception occurs");
    assertTrue(activeSubscriptions.isEmpty(), "No subscriptions should be registered after failure");
  }

  private ConsumerTestSetup setupConsumerTest(boolean withActionFactory) throws Exception {
    AsyncProcessConfig config = new AsyncProcessConfig();
    com.smf.jobs.model.Job job = mock(com.smf.jobs.model.Job.class);
    com.smf.jobs.model.JobLine jobLine = mock(com.smf.jobs.model.JobLine.class);
    org.openbravo.model.ad.system.Client mockClient = mock(org.openbravo.model.ad.system.Client.class);
    org.openbravo.model.common.enterprise.Organization mockOrg = mock(org.openbravo.model.common.enterprise.Organization.class);

    when(job.getId()).thenReturn(TEST_JOB_ID);
    when(job.getName()).thenReturn(TEST_JOB);
    when(job.getClient()).thenReturn(mockClient);
    when(job.getOrganization()).thenReturn(mockOrg);
    when(mockClient.getId()).thenReturn(TEST_CLIENT_ID);
    when(mockOrg.getId()).thenReturn(TEST_ORG_ID);
    when(jobLine.getId()).thenReturn(TEST_JOBLINE_ID);
    when(jobLine.getJobsJob()).thenReturn(job);

    Map<String, Supplier<Action>> actionSuppliers = new HashMap<>();
    if (withActionFactory) {
      actionSuppliers.put(TEST_JOBLINE_ID, () -> mock(Action.class));
    }

    KafkaSender<String, AsyncProcessExecution> kafkaSender = mock(KafkaSender.class);
    Flux<ReceiverRecord<String, AsyncProcessExecution>> mockReceiver = mock(Flux.class);
    when(mockReceiver.doOnNext(any(Consumer.class))).thenReturn(mockReceiver);
    when(mockReceiver.doOnError(any(Consumer.class))).thenReturn(mockReceiver);
    when(mockReceiver.subscribe(any(Consumer.class))).thenReturn(disposable);

    Object creationConfig = createConsumerCreationConfig(job, jobLine, List.of(jobLine), TEST_TOPIC, config, kafkaSender, actionSuppliers);
    return new ConsumerTestSetup(creationConfig, mockReceiver, mock(ReceiverRecord.class));
  }

  private static class ConsumerTestSetup {
    final Object creationConfig;
    final Flux<ReceiverRecord<String, AsyncProcessExecution>> mockReceiver;
    final ReceiverRecord<String, AsyncProcessExecution> mockRecord;

    ConsumerTestSetup(Object creationConfig, Flux<ReceiverRecord<String, AsyncProcessExecution>> mockReceiver, ReceiverRecord<String, AsyncProcessExecution> mockRecord) {
      this.creationConfig = creationConfig;
      this.mockReceiver = mockReceiver;
      this.mockRecord = mockRecord;
    }
  }

  // HELPER METHODS
  private Object invokePrivateMethod(Object obj, String methodName, Class<?>[] paramTypes, Object... args) {
    try {
      Method method = (obj == null ? JobProcessor.class : obj.getClass()).getDeclaredMethod(methodName, paramTypes);
      method.setAccessible(true);
      return method.invoke(obj, args);
    } catch (Exception e) {
      if (e.getCause() != null) {
        throw new RuntimeException(e.getCause());
      }
      throw new RuntimeException(e);
    }
  }

  private Object createConsumerCreationConfig(
      com.smf.jobs.model.Job job, com.smf.jobs.model.JobLine jobLine, List<com.smf.jobs.model.JobLine> jobLines,
      String topic, AsyncProcessConfig config, KafkaSender<String, AsyncProcessExecution> kafkaSender,
      Map<String, Supplier<Action>> actionSuppliers) throws Exception {
    Class<?> configClass = Class.forName(CONSUMER_CREATION_CONFIG);
    Constructor<?> ctor = configClass.getDeclaredConstructors()[0];
    ctor.setAccessible(true);
    return ctor.newInstance(job, jobLine, jobLines, topic, config, kafkaSender, actionSuppliers);
  }

  private Flux<ReceiverRecord<String, AsyncProcessExecution>> invokeCreateAndConfigureConsumer(
      Object creationConfig, int consumerIndex) {
    return (Flux<ReceiverRecord<String, AsyncProcessExecution>>) invokePrivateMethod(jobProcessor,
        "createAndConfigureConsumer", new Class<?>[]{getConsumerConfigClass(), int.class}, creationConfig, consumerIndex);
  }

  private Class<?> getConsumerConfigClass() {
    try {
      return Class.forName(CONSUMER_CREATION_CONFIG);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private static class TestSessionFactoryController extends SessionFactoryController {
    @Override public void initialize() {
      // No-op for testing
    }
    @Override public org.hibernate.SessionFactory getSessionFactory() { return mock(org.hibernate.SessionFactory.class); }
    @Override public boolean isInitialized() { return true; }
    @Override protected void mapModel(org.hibernate.cfg.Configuration configuration) {
      // No-op for testing
    }
  }

  @Test
  void testProcessAllJobs_noJobsDoesNotCreateAdminClient() {
    JobProcessor jp = spy(jobProcessor);
    doReturn(Collections.emptyMap()).when(jp).preloadActionSuppliers();
    doReturn(new ArrayList<>()).when(jp).loadAsyncJobs();
    jp.processAllJobs();
    verify(kafkaClientManager, never()).createAdminClient();
  }

  @Test
  void testProcessAllJobs_whenAdminClientCreationFailsRecordsKafkaConnectionFalse() {
    JobProcessor jp = spy(jobProcessor);
    doReturn(Collections.emptyMap()).when(jp).preloadActionSuppliers();
    com.smf.jobs.model.Job mockJob = mock(com.smf.jobs.model.Job.class);
    when(mockJob.getJOBSJobLineList()).thenReturn(new ArrayList<>());
    doReturn(List.of(mockJob)).when(jp).loadAsyncJobs();
    when(kafkaClientManager.createAdminClient()).thenThrow(new RuntimeException("boom"));
    jp.processAllJobs();
    verify(processMonitor, times(1)).recordKafkaConnection(false);
  }

  @Test
  void testPreloadActionSuppliers_success() {
    try (MockedStatic<OBContext> mockedOBContext = org.mockito.Mockito.mockStatic(OBContext.class)) {
      mockedOBContext.when(() -> OBContext.setOBContext(anyString(), anyString(), anyString(), anyString())).then(inv -> null);
      JobProcessor jp = spy(new JobProcessor(processMonitor, recoveryManager, healthChecker, new HashMap<>(), kafkaClientManager));
      com.smf.jobs.model.Job mockJob = mock(com.smf.jobs.model.Job.class);
      com.smf.jobs.model.JobLine mockJobLine = mock(com.smf.jobs.model.JobLine.class);
      org.openbravo.client.application.Process mockProcess = mock(org.openbravo.client.application.Process.class);
      when(mockJob.getJOBSJobLineList()).thenReturn(List.of(mockJobLine));
      when(mockJobLine.getAction()).thenReturn(mockProcess);
      when(mockJobLine.getId()).thenReturn("jobLine1");
      when(mockProcess.getJavaClassName()).thenReturn("java.lang.String");
      doReturn(List.of(mockJob)).when(jp).loadAsyncJobs();
      Map<String, Supplier<Action>> suppliers = jp.preloadActionSuppliers();
      assertEquals(1, suppliers.size());
      assertTrue(suppliers.containsKey("jobLine1"));
      assertNotNull(suppliers.get("jobLine1"));
    }
  }

  @Test
  void testPreloadActionSuppliers_classNotFoundThrowsOBException() {
    try (MockedStatic<OBContext> mockedOBContext = org.mockito.Mockito.mockStatic(OBContext.class)) {
      mockedOBContext.when(() -> OBContext.setOBContext(anyString(), anyString(), anyString(), anyString())).then(inv -> null);
      JobProcessor jp = spy(new JobProcessor(processMonitor, recoveryManager, healthChecker, new HashMap<>(), kafkaClientManager));
      com.smf.jobs.model.Job mockJob = mock(com.smf.jobs.model.Job.class);
      com.smf.jobs.model.JobLine mockJobLine = mock(com.smf.jobs.model.JobLine.class);
      org.openbravo.client.application.Process mockProcess = mock(org.openbravo.client.application.Process.class);
      when(mockJob.getJOBSJobLineList()).thenReturn(List.of(mockJobLine));
      when(mockJobLine.getAction()).thenReturn(mockProcess);
      when(mockJobLine.getId()).thenReturn("jobLine2");
      when(mockProcess.getJavaClassName()).thenReturn("non.existing.Clazz12345");
      doReturn(List.of(mockJob)).when(jp).loadAsyncJobs();
      assertThrows(org.openbravo.base.exception.OBException.class, jp::preloadActionSuppliers);
    }
  }

  @Test
  void testLoadAsyncJobs_returnsAsyncJobs() {
    try (MockedStatic<OBContext> mockedOBContext = org.mockito.Mockito.mockStatic(OBContext.class);
         MockedStatic<OBDal> mockedOBDal = org.mockito.Mockito.mockStatic(OBDal.class)) {
      mockedOBContext.when(() -> OBContext.setOBContext(anyString(), anyString(), anyString(), anyString())).then(inv -> null);
      JobProcessor jp = new JobProcessor(processMonitor, recoveryManager, healthChecker, new HashMap<>(), kafkaClientManager);
      OBDal mockDal = mock(OBDal.class);
      mockedOBDal.when(OBDal::getInstance).thenReturn(mockDal);
      @SuppressWarnings("unchecked")
      OBCriteria<com.smf.jobs.model.Job> mockCriteria = (OBCriteria<com.smf.jobs.model.Job>) mock(OBCriteria.class);
      when(mockDal.createCriteria(com.smf.jobs.model.Job.class)).thenReturn(mockCriteria);
      when(mockCriteria.add(any())).thenReturn(mockCriteria);
      List<com.smf.jobs.model.Job> expected = List.of(mock(com.smf.jobs.model.Job.class), mock(com.smf.jobs.model.Job.class));
      when(mockCriteria.list()).thenReturn(expected);
      List<com.smf.jobs.model.Job> result = jp.loadAsyncJobs();
      assertEquals(expected, result);
      verify(mockCriteria, times(1)).add(any());
    }
  }

  @Test
  void testLoadAsyncJobs_returnsEmptyListWhenNoAsyncJobs() {
    try (MockedStatic<OBContext> mockedOBContext = org.mockito.Mockito.mockStatic(OBContext.class);
         MockedStatic<OBDal> mockedOBDal = org.mockito.Mockito.mockStatic(OBDal.class)) {
      mockedOBContext.when(() -> OBContext.setOBContext(anyString(), anyString(), anyString(), anyString())).then(inv -> null);
      JobProcessor jp = new JobProcessor(processMonitor, recoveryManager, healthChecker, new HashMap<>(), kafkaClientManager);
      OBDal mockDal = mock(OBDal.class);
      mockedOBDal.when(OBDal::getInstance).thenReturn(mockDal);
      @SuppressWarnings("unchecked")
      OBCriteria<com.smf.jobs.model.Job> mockCriteria = (OBCriteria<com.smf.jobs.model.Job>) mock(OBCriteria.class);
      when(mockDal.createCriteria(com.smf.jobs.model.Job.class)).thenReturn(mockCriteria);
      when(mockCriteria.add(any())).thenReturn(mockCriteria);
      when(mockCriteria.list()).thenReturn(List.of());
      List<com.smf.jobs.model.Job> result = jp.loadAsyncJobs();
      assertNotNull(result);
      assertTrue(result.isEmpty());
      verify(mockCriteria, times(1)).add(any());
    }
  }
}

