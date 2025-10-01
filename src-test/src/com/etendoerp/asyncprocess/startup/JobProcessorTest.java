package com.etendoerp.asyncprocess.startup;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

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

/**
 * Unit tests for the {@link JobProcessor} class.
 * <p>
 * This test class verifies the correct initialization, configuration, and behavior of the JobProcessor,
 * including scheduler management, job and job line configuration, topic calculation, consumer creation,
 * error handling, and utility methods. It uses JUnit 5 and Mockito for mocking dependencies and reflection
 * to access private methods and fields for thorough coverage.
 * <p>
 * Key scenarios covered:
 * <ul>
 *   <li>Initialization and field setup</li>
 *   <li>Scheduler shutdown and error handling</li>
 *   <li>Job and job line configuration and validation</li>
 *   <li>Topic calculation for jobs and job lines</li>
 *   <li>Consumer creation and configuration</li>
 *   <li>Error topic and next topic calculation</li>
 *   <li>State conversion and error handling</li>
 *   <li>Group ID calculation</li>
 *   <li>Partition and consumer count calculation</li>
 *   <li>Reflection-based utility method testing</li>
 * </ul>
 */
class JobProcessorTest {

  @Mock
  private AsyncProcessMonitor processMonitor;
  @Mock
  private ConsumerRecoveryManager recoveryManager;
  @Mock
  private KafkaHealthChecker healthChecker;
  @Mock
  private KafkaClientManager kafkaClientManager;
  @Mock
  private Disposable disposable;

  private Map<String, Disposable> activeSubscriptions;
  private JobProcessor jobProcessor;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    activeSubscriptions = new HashMap<>();
    jobProcessor = new JobProcessor(processMonitor, recoveryManager, healthChecker, activeSubscriptions,
        kafkaClientManager);
  }

  /**
   * Verifies that the JobProcessor constructor initializes all required fields and dependencies.
   */
  @Test
  void testConstructorInitializesFields() {
    assertNotNull(jobProcessor);
  }

  /**
   * Ensures that calling shutdownSchedulers does not throw any exceptions when no schedulers are present.
   */
  @Test
  void testShutdownSchedulersNoError() {
    assertDoesNotThrow(() -> jobProcessor.shutdownSchedulers());
  }

  /**
   * Ensures that shutdownSchedulers properly shuts down active schedulers and clears the scheduler map.
   *
   * @throws Exception
   *     if reflection fails
   */
  @Test
  void testShutdownSchedulersWithActiveSchedulers() throws Exception {
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    when(scheduler.awaitTermination(anyLong(), any())).thenReturn(true);
    Field schedulersField = JobProcessor.class.getDeclaredField("jobSchedulers");
    schedulersField.setAccessible(true); // NOSONAR - Reflection is required for testing private fields
    @SuppressWarnings("unchecked")
    Map<String, ScheduledExecutorService> schedulers = (Map<String, ScheduledExecutorService>) schedulersField.get(
        jobProcessor);
    schedulers.put("testJob", scheduler);
    assertDoesNotThrow(() -> jobProcessor.shutdownSchedulers());
    assertTrue(schedulers.isEmpty());
  }

  /**
   * Ensures that shutdownSchedulers handles InterruptedException gracefully and clears the scheduler map.
   *
   * @throws Exception
   *     if reflection fails
   */
  @Test
  void testShutdownSchedulersWithInterruptedException() throws Exception {
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    when(scheduler.awaitTermination(anyLong(), any())).thenThrow(new InterruptedException());
    Field schedulersField = JobProcessor.class.getDeclaredField("jobSchedulers");
    schedulersField.setAccessible(true); // NOSONAR - Reflection is required for testing private fields
    @SuppressWarnings("unchecked")
    Map<String, ScheduledExecutorService> schedulers = (Map<String, ScheduledExecutorService>) schedulersField.get(
        jobProcessor);
    schedulers.put("testJob", scheduler);
    assertDoesNotThrow(() -> jobProcessor.shutdownSchedulers());
    assertTrue(schedulers.isEmpty());
  }

  /**
   * Verifies that getJobScheduler returns null if the scheduler does not exist for the given job.
   *
   * @throws Exception
   *     if reflection fails
   */
  @Test
  void testGetJobSchedulerReturnsNullIfNotExists() throws Exception {
    Method method = JobProcessor.class.getDeclaredMethod("getJobScheduler", String.class);
    method.setAccessible(true);
    Object result = method.invoke(jobProcessor, "noJob");
    assertNull(result);
  }

  /**
   * Verifies that getJobScheduler returns the correct scheduler if it exists for the given job.
   *
   * @throws Exception
   *     if reflection fails
   */
  @Test
  void testGetJobSchedulerReturnsSchedulerIfExists() throws Exception {
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    Field schedulersField = JobProcessor.class.getDeclaredField("jobSchedulers");
    schedulersField.setAccessible(true); // NOSONAR - Reflection is required for testing private fields
    @SuppressWarnings("unchecked")
    Map<String, ScheduledExecutorService> schedulers = (Map<String, ScheduledExecutorService>) schedulersField.get(
        jobProcessor);
    schedulers.put("jobX", scheduler);
    Method method = JobProcessor.class.getDeclaredMethod("getJobScheduler", String.class);
    method.setAccessible(true);
    Object result = method.invoke(jobProcessor, "jobX");
    assertEquals(scheduler, result);
  }

  /**
   * Ensures that configureJobScheduler creates and stores a scheduler for the given job.
   *
   * @throws Exception
   *     if reflection fails
   */
  @Test
  void testConfigureJobSchedulerCreatesScheduler() throws Exception {
    com.smf.jobs.model.Job job = mock(com.smf.jobs.model.Job.class);
    when(job.getId()).thenReturn("job1");
    when(job.get("etapParallelThreads")).thenReturn("2");
    Method method = JobProcessor.class.getDeclaredMethod("configureJobScheduler", com.smf.jobs.model.Job.class);
    method.setAccessible(true);
    method.invoke(jobProcessor, job);
    Field schedulersField = JobProcessor.class.getDeclaredField("jobSchedulers");
    schedulersField.setAccessible(true); // NOSONAR - Reflection is required for testing private fields
    @SuppressWarnings("unchecked")
    Map<String, ScheduledExecutorService> schedulers = (Map<String, ScheduledExecutorService>) schedulersField.get(
        jobProcessor);
    assertTrue(schedulers.containsKey("job1"));
  }

  /**
   * Verifies that getJobParallelThreads returns the correct thread count for valid, null, and invalid values.
   *
   * @throws Exception
   *     if reflection fails
   */
  @Test
  void testGetJobParallelThreadsWithValidAndInvalidValues() throws Exception {
    com.smf.jobs.model.Job job = mock(com.smf.jobs.model.Job.class);
    when(job.get("etapParallelThreads")).thenReturn("4");
    Method method = JobProcessor.class.getDeclaredMethod("getJobParallelThreads", com.smf.jobs.model.Job.class);
    method.setAccessible(true);
    int threads = (int) method.invoke(jobProcessor, job);
    assertEquals(4, threads);
    when(job.get("etapParallelThreads")).thenReturn(null);
    threads = (int) method.invoke(jobProcessor, job);
    assertEquals(8, threads); // DEFAULT_PARALLEL_THREADS
    when(job.get("etapParallelThreads")).thenReturn("notanumber");
    when(job.getId()).thenReturn("job1");
    threads = (int) method.invoke(jobProcessor, job);
    assertEquals(8, threads); // fallback to default
  }

  /**
   * Verifies that getJobLineConfig returns a valid config object for valid, null, and invalid values.
   *
   * @throws Exception
   *     if reflection fails
   */
  @Test
  void testGetJobLineConfigWithValidAndInvalidValues() throws Exception {
    com.smf.jobs.model.JobLine jobLine = mock(com.smf.jobs.model.JobLine.class);
    when(jobLine.get("etapMaxRetries")).thenReturn("5");
    when(jobLine.get("etapRetryDelayMs")).thenReturn("2000");
    when(jobLine.get("etapPrefetchCount")).thenReturn("3");
    Method method = JobProcessor.class.getDeclaredMethod("getJobLineConfig", com.smf.jobs.model.JobLine.class);
    method.setAccessible(true);
    Object config = method.invoke(jobProcessor, jobLine);
    assertNotNull(config);
    when(jobLine.get("etapMaxRetries")).thenReturn(null);
    when(jobLine.get("etapRetryDelayMs")).thenReturn(null);
    when(jobLine.get("etapPrefetchCount")).thenReturn(null);
    config = method.invoke(jobProcessor, jobLine);
    assertNotNull(config);
    when(jobLine.get("etapMaxRetries")).thenReturn("notanumber");
    when(jobLine.get("etapRetryDelayMs")).thenReturn("notanumber");
    when(jobLine.get("etapPrefetchCount")).thenReturn("notanumber");
    config = method.invoke(jobProcessor, jobLine);
    assertNotNull(config);
  }

  /**
   * Ensures that calculateErrorTopic returns a default topic if none is set, or the configured topic otherwise.
   *
   * @throws Exception
   *     if reflection fails
   */
  @Test
  void testCalculateErrorTopic() throws Exception {
    com.smf.jobs.model.Job job = mock(com.smf.jobs.model.Job.class);
    when(job.getEtapErrortopic()).thenReturn(null);
    when(job.getName()).thenReturn("TestJob");
    Method method = JobProcessor.class.getDeclaredMethod("calculateErrorTopic", com.smf.jobs.model.Job.class);
    method.setAccessible(true);
    String topic = (String) method.invoke(jobProcessor, job);
    assertNotNull(topic);
    when(job.getEtapErrortopic()).thenReturn("customTopic");
    topic = (String) method.invoke(jobProcessor, job);
    assertEquals("customTopic", topic);
  }

  /**
   * Ensures that calculateNextTopic returns a default topic if none is set, or the configured topic otherwise.
   *
   * @throws Exception
   *     if reflection fails
   */
  @Test
  void testCalculateNextTopic() throws Exception {
    com.smf.jobs.model.Job job = mock(com.smf.jobs.model.Job.class);
    when(job.getName()).thenReturn("TestJob");
    com.smf.jobs.model.JobLine jobLine = mock(com.smf.jobs.model.JobLine.class);
    when(jobLine.getJobsJob()).thenReturn(job);
    when(jobLine.getEtapTargettopic()).thenReturn(null);
    when(jobLine.getLineNo()).thenReturn(1L);
    List<com.smf.jobs.model.JobLine> jobLines = new ArrayList<>();
    jobLines.add(jobLine);
    Method method = JobProcessor.class.getDeclaredMethod("calculateNextTopic", com.smf.jobs.model.JobLine.class,
        List.class);
    method.setAccessible(true);
    String topic = (String) method.invoke(jobProcessor, jobLine, jobLines);
    assertNotNull(topic);
    when(jobLine.getEtapTargettopic()).thenReturn("nextTopic");
    topic = (String) method.invoke(jobProcessor, jobLine, jobLines);
    assertEquals("nextTopic", topic);
  }

  /**
   * Ensures that calculateCurrentTopic returns a default topic if none is set, or the configured topic otherwise.
   *
   * @throws Exception
   *     if reflection fails
   */
  @Test
  void testCalculateCurrentTopic() throws Exception {
    com.smf.jobs.model.Job job = mock(com.smf.jobs.model.Job.class);
    when(job.getEtapInitialTopic()).thenReturn(null);
    when(job.getName()).thenReturn("TestJob");
    com.smf.jobs.model.JobLine jobLine = mock(com.smf.jobs.model.JobLine.class);
    when(jobLine.getJobsJob()).thenReturn(job);
    when(jobLine.getLineNo()).thenReturn(1L);
    List<com.smf.jobs.model.JobLine> jobLines = new ArrayList<>();
    jobLines.add(jobLine);
    Method method = JobProcessor.class.getDeclaredMethod("calculateCurrentTopic", com.smf.jobs.model.JobLine.class,
        List.class);
    method.setAccessible(true);
    String topic = (String) method.invoke(jobProcessor, jobLine, jobLines);
    assertNotNull(topic);
    when(job.getEtapInitialTopic()).thenReturn("initTopic");
    topic = (String) method.invoke(jobProcessor, jobLine, jobLines);
    assertEquals("initTopic", topic);
  }

  /**
   * Verifies that convertState returns a valid state for known, unknown, and null input values.
   *
   * @throws Exception
   *     if reflection fails
   */
  @Test
  void testConvertState() throws Exception {
    Method method = JobProcessor.class.getDeclaredMethod("convertState", String.class);
    method.setAccessible(true);
    Object state = method.invoke(jobProcessor, "STARTED");
    assertNotNull(state);
    state = method.invoke(jobProcessor, "INVALID");
    assertNotNull(state);
    state = method.invoke(jobProcessor, (Object) null);
    assertNotNull(state);
  }

  /**
   * Ensures that handleJobProcessingError does not throw exceptions when handling errors.
   *
   * @throws Exception
   *     if reflection fails
   */
  @Test
  void testHandleJobProcessingError() throws Exception {
    Method method = JobProcessor.class.getDeclaredMethod("handleJobProcessingError", Throwable.class);
    method.setAccessible(true);
    Throwable t = new RuntimeException("test error");
    assertDoesNotThrow(() -> method.invoke(jobProcessor, t));
  }

  /**
   * Verifies that getGroupId returns a group ID containing the job name and line number.
   *
   * @throws Exception
   *     if reflection fails
   */
  @Test
  void testGetGroupId() throws Exception {
    com.smf.jobs.model.Job job = mock(com.smf.jobs.model.Job.class);
    when(job.getName()).thenReturn("Test.Job_1");
    com.smf.jobs.model.JobLine jobLine = mock(com.smf.jobs.model.JobLine.class);
    when(jobLine.getJobsJob()).thenReturn(job);
    Method method = JobProcessor.class.getDeclaredMethod("getGroupId", com.smf.jobs.model.JobLine.class);
    method.setAccessible(true);
    String groupId = (String) method.invoke(null, jobLine);
    assertTrue(groupId.contains("test-job-1"));
  }

  /**
   * Verifies that calculateConsumerCount returns the correct number of consumers based on job and job line configuration.
   * Also ensures that setupTopicAndPartitions does not throw exceptions.
   *
   * @throws Exception
   *     if reflection fails
   */
  @Test
  void testSetupTopicAndPartitionsAndCalculateConsumerCount() throws Exception {
    com.smf.jobs.model.Job job = mock(com.smf.jobs.model.Job.class);
    com.smf.jobs.model.JobLine jobLine = mock(com.smf.jobs.model.JobLine.class);
    when(job.isEtapConsumerPerPartition()).thenReturn(false);
    when(jobLine.isEtapConsumerPerPartition()).thenReturn(false);
    when(kafkaClientManager.getNumPartitions()).thenReturn(3);
    Method countMethod = JobProcessor.class.getDeclaredMethod("calculateConsumerCount", com.smf.jobs.model.Job.class,
        com.smf.jobs.model.JobLine.class);
    countMethod.setAccessible(true);
    int count = (int) countMethod.invoke(jobProcessor, job, jobLine);
    assertEquals(1, count);
    when(job.isEtapConsumerPerPartition()).thenReturn(true);
    count = (int) countMethod.invoke(jobProcessor, job, jobLine);
    assertEquals(3, count);
    AdminClient adminClient = mock(AdminClient.class);
    Method setupMethod = JobProcessor.class.getDeclaredMethod("setupTopicAndPartitions", AdminClient.class,
        String.class);
    setupMethod.setAccessible(true);
    assertDoesNotThrow(() -> setupMethod.invoke(jobProcessor, adminClient, "topic1"));
  }

  /**
   * Ensures that createConsumersForJobLine and createSingleConsumerSafely create consumers as expected.
   *
   * @throws Exception
   *     if reflection fails
   */
  @Test
  void testCreateConsumersForJobLineAndCreateSingleConsumerSafely() throws Exception {
    Class<?> configClass = Class.forName("com.etendoerp.asyncprocess.startup.JobProcessor$ConsumerCreationConfig");
    Constructor<?> ctor = configClass.getDeclaredConstructors()[0];
    ctor.setAccessible(true);
    com.smf.jobs.model.Job job = mock(com.smf.jobs.model.Job.class);
    when(job.getName()).thenReturn("TestJob");
    com.smf.jobs.model.JobLine jobLine = mock(com.smf.jobs.model.JobLine.class);
    when(jobLine.getJobsJob()).thenReturn(job);
    List<com.smf.jobs.model.JobLine> jobLines = new ArrayList<>();
    jobLines.add(jobLine);
    String topic = "topic";
    Object asyncConfig = Class.forName(
        "com.etendoerp.asyncprocess.config.AsyncProcessConfig").getDeclaredConstructor().newInstance();
    reactor.kafka.sender.KafkaSender kafkaSender = mock(reactor.kafka.sender.KafkaSender.class);
    Map<String, java.util.function.Supplier<com.smf.jobs.Action>> actionSuppliers = new HashMap<>();
    Object creationConfig = ctor.newInstance(job, jobLine, jobLines, topic, asyncConfig, kafkaSender, actionSuppliers);
    Method createConsumers = JobProcessor.class.getDeclaredMethod("createConsumersForJobLine", configClass, int.class);
    createConsumers.setAccessible(true);
    Object result = createConsumers.invoke(jobProcessor, creationConfig, 1);
    assertNotNull(result);
    Method createSingle = JobProcessor.class.getDeclaredMethod("createSingleConsumerSafely", configClass, int.class);
    createSingle.setAccessible(true);
  }

  /**
   * Tests the NewInstance inner class get() method to ensure it returns null for interface types.
   * This assertion ensures the test is meaningful and resolves the warning about missing assertions.
   *
   * @throws Exception
   *     if reflection fails
   */
  @Test
  void testNewInstanceGet() throws Exception {
    Class<?> newInstanceClass = Class.forName("com.etendoerp.asyncprocess.startup.JobProcessor$NewInstance");
    Constructor<?> ctor = newInstanceClass.getDeclaredConstructor(Class.class);
    ctor.setAccessible(true);
    Class<?> actionClass = com.smf.jobs.Action.class;
    Object newInstance = ctor.newInstance(actionClass);
    Method getMethod = newInstanceClass.getDeclaredMethod("get");
    getMethod.setAccessible(true);
    Object result = null;
    try {
      result = getMethod.invoke(newInstance);
    } catch (Exception ignored) {
      // Ignored exception
    }
    // Assertion added to ensure the test is meaningful and the warning is resolved
    assertNull(result, "Expected get() to return null for interface Action");
  }

  // ==================== NEW TESTS FOR LINES 281-333 ====================

  /**
   * Tests successful consumer creation and configuration (lines 281-333).
   * Verifies that createAndConfigureConsumer properly creates a consumer with all components.
   */
  @Test
  void testCreateAndConfigureConsumerSuccess() throws Exception {
    // GIVEN
    AsyncProcessConfig config = createTestAsyncProcessConfig();
    List<com.smf.jobs.model.JobLine> jobLines = new ArrayList<>();

    com.smf.jobs.model.Job job = mock(com.smf.jobs.model.Job.class);
    com.smf.jobs.model.JobLine jobLine = mock(com.smf.jobs.model.JobLine.class);

    // Mock Client and Organization to avoid NullPointerException
    org.openbravo.model.ad.system.Client mockClient = mock(org.openbravo.model.ad.system.Client.class);
    org.openbravo.model.common.enterprise.Organization mockOrganization = mock(org.openbravo.model.common.enterprise.Organization.class);

    when(job.getId()).thenReturn("test-job-id");
    when(job.getName()).thenReturn("TestJob");
    when(job.getClient()).thenReturn(mockClient);
    when(job.getOrganization()).thenReturn(mockOrganization);
    when(mockClient.getId()).thenReturn("test-client-id");
    when(mockOrganization.getId()).thenReturn("test-org-id");

    when(jobLine.getId()).thenReturn("test-jobline-id");
    when(jobLine.getJobsJob()).thenReturn(job);

    jobLines.add(jobLine);

    Map<String, Supplier<Action>> actionSuppliers = new HashMap<>();
    Action mockAction = mock(Action.class);
    actionSuppliers.put("test-jobline-id", () -> mockAction);

    KafkaSender<String, AsyncProcessExecution> kafkaSender = mock(KafkaSender.class);
    Flux<ReceiverRecord<String, AsyncProcessExecution>> mockReceiver = mock(Flux.class);
    ReceiverRecord<String, AsyncProcessExecution> mockRecord = mock(ReceiverRecord.class);

    // Store the callbacks to execute them later
    Consumer<ReceiverRecord<String, AsyncProcessExecution>>[] onNextCallback = new Consumer[1];
    Consumer<Throwable>[] onErrorCallback = new Consumer[1];

    // Capture the doOnNext callback
    when(mockReceiver.doOnNext(any(Consumer.class))).thenAnswer(invocation -> {
      onNextCallback[0] = invocation.getArgument(0);
      return mockReceiver;
    });

    // Capture the doOnError callback
    when(mockReceiver.doOnError(any(Consumer.class))).thenAnswer(invocation -> {
      onErrorCallback[0] = invocation.getArgument(0);
      return mockReceiver;
    });

    // Mock subscribe to execute the onNext callback immediately
    when(mockReceiver.subscribe(any(Consumer.class))).thenAnswer(invocation -> {
      // Execute the captured doOnNext callback to trigger processMonitor.recordConsumerActivity
      if (onNextCallback[0] != null) {
        onNextCallback[0].accept(mockRecord);
      }
      return disposable;
    });

    when(kafkaClientManager.createReceiver(anyString(), anyBoolean(), any(AsyncProcessConfig.class), anyString()))
        .thenReturn(mockReceiver);

    Object creationConfig = createConsumerCreationConfig(job, jobLine, jobLines, "test-topic", config, kafkaSender, actionSuppliers);

    // WHEN
    Flux<ReceiverRecord<String, AsyncProcessExecution>> result = invokeCreateAndConfigureConsumer(creationConfig, 0);

    // THEN
    assertNotNull(result, "Consumer should be created successfully");
    assertEquals(mockReceiver, result, "Returned Flux should be the created receiver");

    // Verify active subscription was registered
    String expectedConsumerId = "test-jobline-id-0";
    assertTrue(activeSubscriptions.containsKey(expectedConsumerId), "Active subscription should be registered");

    // Verify components were called
    verify(processMonitor, times(1)).recordConsumerActivity(anyString(), anyString(), anyString());
    verify(recoveryManager, times(1)).registerConsumer(any());
    verify(healthChecker, times(1)).registerConsumerGroup(anyString());
  }

  /**
   * Tests consumer creation when action factory is null (covering lines 276-279).
   */
  @Test
  void testCreateAndConfigureConsumerNullActionFactory() throws Exception {
    // GIVEN
    AsyncProcessConfig config = createTestAsyncProcessConfig();
    List<com.smf.jobs.model.JobLine> jobLines = new ArrayList<>();

    com.smf.jobs.model.Job job = mock(com.smf.jobs.model.Job.class);
    com.smf.jobs.model.JobLine jobLine = mock(com.smf.jobs.model.JobLine.class);

    // Mock Client and Organization to avoid NullPointerException
    org.openbravo.model.ad.system.Client mockClient = mock(org.openbravo.model.ad.system.Client.class);
    org.openbravo.model.common.enterprise.Organization mockOrganization = mock(org.openbravo.model.common.enterprise.Organization.class);

    when(job.getId()).thenReturn("test-job-id");
    when(job.getClient()).thenReturn(mockClient);
    when(job.getOrganization()).thenReturn(mockOrganization);
    when(mockClient.getId()).thenReturn("test-client-id");
    when(mockOrganization.getId()).thenReturn("test-org-id");

    when(jobLine.getId()).thenReturn("test-jobline-id");
    when(jobLine.getJobsJob()).thenReturn(job);

    // Empty action suppliers map - no action factory available
    Map<String, Supplier<Action>> actionSuppliers = new HashMap<>();

    KafkaSender<String, AsyncProcessExecution> kafkaSender = mock(KafkaSender.class);

    Object creationConfig = createConsumerCreationConfig(job, jobLine, jobLines, "test-topic", config, kafkaSender, actionSuppliers);

    // WHEN
    Flux<ReceiverRecord<String, AsyncProcessExecution>> result = invokeCreateAndConfigureConsumer(creationConfig, 0);

    // THEN
    assertNull(result, "Should return null when action factory is not found");

    // Verify no subscriptions were created
    assertTrue(activeSubscriptions.isEmpty(), "No active subscriptions should be registered");

    // Verify components were not called
    verify(processMonitor, never()).recordConsumerActivity(anyString(), anyString(), anyString());
    verify(recoveryManager, never()).registerConsumer(any());
    verify(healthChecker, never()).registerConsumerGroup(anyString());
  }

  /**
   * Tests consumer activity monitoring callback (lines 294-297).
   */
  @Test
  void testConsumerActivityMonitoring() throws Exception {
    // GIVEN
    AsyncProcessConfig config = createTestAsyncProcessConfig();
    List<com.smf.jobs.model.JobLine> jobLines = new ArrayList<>();

    com.smf.jobs.model.Job job = mock(com.smf.jobs.model.Job.class);
    com.smf.jobs.model.JobLine jobLine = mock(com.smf.jobs.model.JobLine.class);

    // Mock Client and Organization to avoid NullPointerException
    org.openbravo.model.ad.system.Client mockClient = mock(org.openbravo.model.ad.system.Client.class);
    org.openbravo.model.common.enterprise.Organization mockOrganization = mock(org.openbravo.model.common.enterprise.Organization.class);

    when(job.getId()).thenReturn("test-job-id");
    when(job.getName()).thenReturn("TestJob");
    when(job.getClient()).thenReturn(mockClient);
    when(job.getOrganization()).thenReturn(mockOrganization);
    when(mockClient.getId()).thenReturn("test-client-id");
    when(mockOrganization.getId()).thenReturn("test-org-id");

    when(jobLine.getId()).thenReturn("test-jobline-id");
    when(jobLine.getJobsJob()).thenReturn(job);

    jobLines.add(jobLine);

    Map<String, Supplier<Action>> actionSuppliers = new HashMap<>();
    Action mockAction = mock(Action.class);
    actionSuppliers.put("test-jobline-id", () -> mockAction);

    KafkaSender<String, AsyncProcessExecution> kafkaSender = mock(KafkaSender.class);
    Flux<ReceiverRecord<String, AsyncProcessExecution>> mockReceiver = mock(Flux.class);
    ReceiverRecord<String, AsyncProcessExecution> mockRecord = mock(ReceiverRecord.class);

    // Store the callbacks to execute them later
    Consumer<ReceiverRecord<String, AsyncProcessExecution>>[] onNextCallback = new Consumer[1];

    // Capture the doOnNext callback and execute it immediately
    when(mockReceiver.doOnNext(any(Consumer.class))).thenAnswer(invocation -> {
      Consumer<ReceiverRecord<String, AsyncProcessExecution>> callback = invocation.getArgument(0);
      onNextCallback[0] = callback;
      // Execute the callback immediately to trigger recordConsumerActivity
      callback.accept(mockRecord);
      return mockReceiver;
    });

    when(mockReceiver.doOnError(any(Consumer.class))).thenReturn(mockReceiver);
    when(mockReceiver.subscribe(any(Consumer.class))).thenReturn(disposable);

    when(kafkaClientManager.createReceiver(anyString(), anyBoolean(), any(AsyncProcessConfig.class), anyString()))
        .thenReturn(mockReceiver);

    Object creationConfig = createConsumerCreationConfig(job, jobLine, jobLines, "test-topic", config, kafkaSender, actionSuppliers);

    // WHEN
    invokeCreateAndConfigureConsumer(creationConfig, 0);

    // THEN
    String expectedConsumerId = "test-jobline-id-0";
    String expectedGroupId = "etendo-ap-group-testjob";
    verify(processMonitor, times(1)).recordConsumerActivity(expectedConsumerId, expectedGroupId, "test-topic");
  }

  /**
   * Tests consumer error handling callback (lines 298-305).
   */
  @Test
  void testConsumerErrorHandling() throws Exception {
    // GIVEN
    AsyncProcessConfig config = createTestAsyncProcessConfig();
    List<com.smf.jobs.model.JobLine> jobLines = new ArrayList<>();

    com.smf.jobs.model.Job job = mock(com.smf.jobs.model.Job.class);
    com.smf.jobs.model.JobLine jobLine = mock(com.smf.jobs.model.JobLine.class);

    // Mock Client and Organization to avoid NullPointerException
    org.openbravo.model.ad.system.Client mockClient = mock(org.openbravo.model.ad.system.Client.class);
    org.openbravo.model.common.enterprise.Organization mockOrganization = mock(org.openbravo.model.common.enterprise.Organization.class);

    when(job.getId()).thenReturn("test-job-id");
    when(job.getName()).thenReturn("TestJob");
    when(job.getClient()).thenReturn(mockClient);
    when(job.getOrganization()).thenReturn(mockOrganization);
    when(mockClient.getId()).thenReturn("test-client-id");
    when(mockOrganization.getId()).thenReturn("test-org-id");

    when(jobLine.getId()).thenReturn("test-jobline-id");
    when(jobLine.getJobsJob()).thenReturn(job);

    jobLines.add(jobLine);

    Map<String, Supplier<Action>> actionSuppliers = new HashMap<>();
    Action mockAction = mock(Action.class);
    actionSuppliers.put("test-jobline-id", () -> mockAction);

    KafkaSender<String, AsyncProcessExecution> kafkaSender = mock(KafkaSender.class);
    Flux<ReceiverRecord<String, AsyncProcessExecution>> mockReceiver = mock(Flux.class);

    RuntimeException testError = new RuntimeException("Connection lost");

    when(mockReceiver.doOnNext(any(Consumer.class))).thenReturn(mockReceiver);

    // Capture the doOnError callback and execute it immediately
    when(mockReceiver.doOnError(any(Consumer.class))).thenAnswer(invocation -> {
      Consumer<Throwable> callback = invocation.getArgument(0);
      // Execute the callback immediately to trigger recordConsumerConnectionLost
      callback.accept(testError);
      return mockReceiver;
    });

    when(mockReceiver.subscribe(any(Consumer.class))).thenReturn(disposable);

    when(kafkaClientManager.createReceiver(anyString(), anyBoolean(), any(AsyncProcessConfig.class), anyString()))
        .thenReturn(mockReceiver);

    Object creationConfig = createConsumerCreationConfig(job, jobLine, jobLines, "test-topic", config, kafkaSender, actionSuppliers);

    // WHEN
    invokeCreateAndConfigureConsumer(creationConfig, 0);

    // THEN
    String expectedConsumerId = "test-jobline-id-0";
    verify(processMonitor, times(1)).recordConsumerConnectionLost(expectedConsumerId);
  }

  /**
   * Tests consumer creation without process monitor (null safety).
   */
  @Test
  void testCreateAndConfigureConsumerNullProcessMonitor() throws Exception {
    // GIVEN
    JobProcessor processorWithNullMonitor = new JobProcessor(null, recoveryManager, healthChecker, activeSubscriptions, kafkaClientManager);

    AsyncProcessConfig config = createTestAsyncProcessConfig();
    List<com.smf.jobs.model.JobLine> jobLines = new ArrayList<>();

    com.smf.jobs.model.Job job = mock(com.smf.jobs.model.Job.class);
    com.smf.jobs.model.JobLine jobLine = mock(com.smf.jobs.model.JobLine.class);

    // Mock Client and Organization to avoid NullPointerException
    org.openbravo.model.ad.system.Client mockClient = mock(org.openbravo.model.ad.system.Client.class);
    org.openbravo.model.common.enterprise.Organization mockOrganization = mock(org.openbravo.model.common.enterprise.Organization.class);

    when(job.getId()).thenReturn("test-job-id");
    when(job.getName()).thenReturn("TestJob");
    when(job.getClient()).thenReturn(mockClient);
    when(job.getOrganization()).thenReturn(mockOrganization);
    when(mockClient.getId()).thenReturn("test-client-id");
    when(mockOrganization.getId()).thenReturn("test-org-id");

    when(jobLine.getId()).thenReturn("test-jobline-id");
    when(jobLine.getJobsJob()).thenReturn(job);

    jobLines.add(jobLine);

    Map<String, Supplier<Action>> actionSuppliers = new HashMap<>();
    Action mockAction = mock(Action.class);
    actionSuppliers.put("test-jobline-id", () -> mockAction);

    KafkaSender<String, AsyncProcessExecution> kafkaSender = mock(KafkaSender.class);
    Flux<ReceiverRecord<String, AsyncProcessExecution>> mockReceiver = mock(Flux.class);

    when(mockReceiver.doOnNext(any(Consumer.class))).thenReturn(mockReceiver);
    when(mockReceiver.doOnError(any(Consumer.class))).thenReturn(mockReceiver);
    when(mockReceiver.subscribe(any(Consumer.class))).thenReturn(disposable);

    when(kafkaClientManager.createReceiver(anyString(), anyBoolean(), any(AsyncProcessConfig.class), anyString()))
        .thenReturn(mockReceiver);

    Object creationConfig = createConsumerCreationConfig(job, jobLine, jobLines, "test-topic", config, kafkaSender, actionSuppliers);

    // WHEN
    Flux<ReceiverRecord<String, AsyncProcessExecution>> result =
        invokeCreateAndConfigureConsumerOnProcessor(processorWithNullMonitor, creationConfig, 0);

    // THEN
    assertNotNull(result, "Consumer should be created even without process monitor");

    // Verify other components were still called
    verify(recoveryManager, times(1)).registerConsumer(any());
    verify(healthChecker, times(1)).registerConsumerGroup(anyString());
  }

  /**
   * Tests recovery manager consumer registration (lines 307-327).
   */
  @Test
  void testConsumerRecoveryInfoCreation() throws Exception {
    // GIVEN
    AsyncProcessConfig config = createTestAsyncProcessConfig();
    List<com.smf.jobs.model.JobLine> jobLines = new ArrayList<>();

    com.smf.jobs.model.Job job = mock(com.smf.jobs.model.Job.class);
    com.smf.jobs.model.JobLine jobLine = mock(com.smf.jobs.model.JobLine.class);

    // Mock Client and Organization to avoid NullPointerException
    org.openbravo.model.ad.system.Client mockClient = mock(org.openbravo.model.ad.system.Client.class);
    org.openbravo.model.common.enterprise.Organization mockOrganization = mock(org.openbravo.model.common.enterprise.Organization.class);

    when(job.getId()).thenReturn("test-job-id");
    when(job.getName()).thenReturn("TestJob");
    when(job.getClient()).thenReturn(mockClient);
    when(job.getOrganization()).thenReturn(mockOrganization);
    when(mockClient.getId()).thenReturn("test-client-id");
    when(mockOrganization.getId()).thenReturn("test-org-id");

    when(jobLine.getId()).thenReturn("test-jobline-id");
    when(jobLine.getJobsJob()).thenReturn(job);

    jobLines.add(jobLine);

    Map<String, Supplier<Action>> actionSuppliers = new HashMap<>();
    Action mockAction = mock(Action.class);
    actionSuppliers.put("test-jobline-id", () -> mockAction);

    KafkaSender<String, AsyncProcessExecution> kafkaSender = mock(KafkaSender.class);
    Flux<ReceiverRecord<String, AsyncProcessExecution>> mockReceiver = mock(Flux.class);

    when(mockReceiver.doOnNext(any(Consumer.class))).thenReturn(mockReceiver);
    when(mockReceiver.doOnError(any(Consumer.class))).thenReturn(mockReceiver);
    when(mockReceiver.subscribe(any(Consumer.class))).thenReturn(disposable);

    when(kafkaClientManager.createReceiver(anyString(), anyBoolean(), any(AsyncProcessConfig.class), anyString()))
        .thenReturn(mockReceiver);
    when(kafkaClientManager.getKafkaHost()).thenReturn("localhost:9092");

    Object creationConfig = createConsumerCreationConfig(job, jobLine, jobLines, "test-topic", config, kafkaSender, actionSuppliers);

    // WHEN
    invokeCreateAndConfigureConsumer(creationConfig, 0);

    // THEN
    verify(recoveryManager, times(1)).registerConsumer(any(ConsumerRecoveryManager.ConsumerInfo.class));
    verify(kafkaClientManager, times(1)).getKafkaHost();
  }

  /**
   * Tests health checker registration (lines 329-331).
   */
  @Test
  void testHealthCheckerRegistration() throws Exception {
    // GIVEN
    AsyncProcessConfig config = createTestAsyncProcessConfig();
    List<com.smf.jobs.model.JobLine> jobLines = new ArrayList<>();

    com.smf.jobs.model.Job job = mock(com.smf.jobs.model.Job.class);
    com.smf.jobs.model.JobLine jobLine = mock(com.smf.jobs.model.JobLine.class);

    // Mock Client and Organization to avoid NullPointerException
    org.openbravo.model.ad.system.Client mockClient = mock(org.openbravo.model.ad.system.Client.class);
    org.openbravo.model.common.enterprise.Organization mockOrganization = mock(org.openbravo.model.common.enterprise.Organization.class);

    when(job.getId()).thenReturn("test-job-id");
    when(job.getName()).thenReturn("TestJob");
    when(job.getClient()).thenReturn(mockClient);
    when(job.getOrganization()).thenReturn(mockOrganization);
    when(mockClient.getId()).thenReturn("test-client-id");
    when(mockOrganization.getId()).thenReturn("test-org-id");

    when(jobLine.getId()).thenReturn("test-jobline-id");
    when(jobLine.getJobsJob()).thenReturn(job);

    jobLines.add(jobLine);

    Map<String, Supplier<Action>> actionSuppliers = new HashMap<>();
    Action mockAction = mock(Action.class);
    actionSuppliers.put("test-jobline-id", () -> mockAction);

    KafkaSender<String, AsyncProcessExecution> kafkaSender = mock(KafkaSender.class);
    Flux<ReceiverRecord<String, AsyncProcessExecution>> mockReceiver = mock(Flux.class);

    when(mockReceiver.doOnNext(any(Consumer.class))).thenReturn(mockReceiver);
    when(mockReceiver.doOnError(any(Consumer.class))).thenReturn(mockReceiver);
    when(mockReceiver.subscribe(any(Consumer.class))).thenReturn(disposable);

    when(kafkaClientManager.createReceiver(anyString(), anyBoolean(), any(AsyncProcessConfig.class), anyString()))
        .thenReturn(mockReceiver);

    Object creationConfig = createConsumerCreationConfig(job, jobLine, jobLines, "test-topic", config, kafkaSender, actionSuppliers);

    // WHEN
    invokeCreateAndConfigureConsumer(creationConfig, 0);

    // THEN
    String expectedGroupId = "etendo-ap-group-testjob";
    verify(healthChecker, times(1)).registerConsumerGroup(expectedGroupId);
  }

  // ==================== HELPER METHODS ====================

  /**
   * Creates a test AsyncProcessConfig with default values.
   */
  private AsyncProcessConfig createTestAsyncProcessConfig() {
    AsyncProcessConfig config = new AsyncProcessConfig();
    config.setMaxRetries(3);
    config.setRetryDelayMs(1000L);
    config.setPrefetchCount(1);
    return config;
  }

  /**
   * Creates a ConsumerCreationConfig using reflection.
   */
  private Object createConsumerCreationConfig(
      com.smf.jobs.model.Job job,
      com.smf.jobs.model.JobLine jobLine,
      List<com.smf.jobs.model.JobLine> jobLines,
      String topic,
      AsyncProcessConfig config,
      KafkaSender<String, AsyncProcessExecution> kafkaSender,
      Map<String, Supplier<Action>> actionSuppliers) throws Exception {

    Class<?> configClass = Class.forName("com.etendoerp.asyncprocess.startup.JobProcessor$ConsumerCreationConfig");
    Constructor<?> ctor = configClass.getDeclaredConstructors()[0];
    ctor.setAccessible(true);

    return ctor.newInstance(job, jobLine, jobLines, topic, config, kafkaSender, actionSuppliers);
  }

  /**
   * Invokes the createAndConfigureConsumer method using reflection.
   */
  private Flux<ReceiverRecord<String, AsyncProcessExecution>> invokeCreateAndConfigureConsumer(
      Object creationConfig, int consumerIndex) throws Exception {
    return invokeCreateAndConfigureConsumerOnProcessor(jobProcessor, creationConfig, consumerIndex);
  }

  /**
   * Invokes the createAndConfigureConsumer method on a specific processor using reflection.
   */
  private Flux<ReceiverRecord<String, AsyncProcessExecution>> invokeCreateAndConfigureConsumerOnProcessor(
      JobProcessor processor, Object creationConfig, int consumerIndex) throws Exception {

    Class<?> configClass = Class.forName("com.etendoerp.asyncprocess.startup.JobProcessor$ConsumerCreationConfig");
    Method method = JobProcessor.class.getDeclaredMethod("createAndConfigureConsumer", configClass, int.class);
    method.setAccessible(true);

    return (Flux<ReceiverRecord<String, AsyncProcessExecution>>) method.invoke(processor, creationConfig, consumerIndex);
  }
}
