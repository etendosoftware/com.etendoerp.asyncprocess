package com.etendoerp.asyncprocess.startup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.openbravo.base.session.OBPropertiesProvider;
import org.openbravo.base.util.OBClassLoader;
import org.openbravo.base.weld.WeldUtils;
import org.openbravo.client.application.Process;
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.service.OBCriteria;
import org.openbravo.dal.service.OBDal;
import org.openbravo.model.ad.access.User;
import org.openbravo.model.ad.system.Client;
import org.openbravo.model.common.enterprise.Organization;

import com.etendoerp.asyncprocess.config.AsyncProcessConfig;
import com.etendoerp.asyncprocess.model.AsyncProcessState;
import com.smf.jobs.Action;
import com.smf.jobs.model.Job;
import com.smf.jobs.model.JobLine;

/**
 * Unit tests for the AsyncProcessStartup class.
 * This class tests the initialization and configuration of asynchronous processes in Openbravo.
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
  private DescribeTopicsResult mockDescribeTopicsResult;

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
  }

  /**
   * Set up the OBContext with a default user and organization.
   */
  @AfterEach
  void tearDown() {
    if (mockedOBDal != null) mockedOBDal.close();
    if (mockedOBContext != null) mockedOBContext.close();
    if (mockedPropertiesProvider != null) mockedPropertiesProvider.close();
    if (mockedWeldUtils != null) mockedWeldUtils.close();
    if (mockedClassLoader != null) mockedClassLoader.close();
    if (mockedAdminClient != null) mockedAdminClient.close();
  }

  /**
   * Test the init method when no async jobs are found in the database.
   */
  @Test
  void testInitNoAsyncJobsFound() {
    when(mockProperties.getProperty("kafka.url")).thenReturn("localhost:29092");
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
    when(mockProperties.getProperty("kafka.url")).thenReturn("localhost:29092");
    when(mockProperties.containsKey("kafka.enable")).thenReturn(true);
    when(mockProperties.getProperty("kafka.enable", "false")).thenReturn("false");
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
   * @throws Exception if there is an error during the test execution
   */
  @Test
  void testInitWithEnabledAsyncJobs() throws Exception {
    List<JobLine> jobLines = Arrays.asList(mockJobLine);
    when(mockProperties.getProperty("kafka.url")).thenReturn("localhost:29092");
    when(mockProperties.containsKey("kafka.enable")).thenReturn(true);
    when(mockProperties.getProperty("kafka.enable", "false")).thenReturn("true");
    when(mockProperties.getProperty("kafka.topic.partitions")).thenReturn("5");
    when(mockOBDal.createCriteria(Job.class)).thenReturn(mockCriteria);
    when(mockCriteria.add(any())).thenReturn(mockCriteria);
    when(mockCriteria.list()).thenReturn(Arrays.asList(mockJob));

    when(mockJob.getJOBSJobLineList()).thenReturn(jobLines);
    when(mockJob.getId()).thenReturn("job-123");
    when(mockJob.getName()).thenReturn("Test Job");
    when(mockJob.isEtapIsregularexp()).thenReturn(false);
    when(mockJob.isEtapConsumerPerPartition()).thenReturn(false);
    when(mockJob.getOrganization()).thenReturn(mockOrganization);
    when(mockJob.getEtapInitialTopic()).thenReturn("test-topic");
    when(mockJob.getEtapErrortopic()).thenReturn("error-topic");

    when(mockJob.getClient()).thenReturn(mockClient);
    when(mockClient.getId()).thenReturn("client-123");

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

    mockedWeldUtils.when(() -> WeldUtils.getInstanceFromStaticBeanManager(any())).thenReturn(mock(Action.class));

    KafkaFuture<Set<String>> topicsFuture = mock(KafkaFuture.class);
    when(mockListTopicsResult.names()).thenReturn(topicsFuture);
    when(topicsFuture.get()).thenReturn(Collections.emptySet());
    when(mockAdminClient.listTopics()).thenReturn(mockListTopicsResult);
    when(mockAdminClient.createTopics(anyList())).thenReturn(mockCreateTopicsResult);

    asyncProcessStartup.init();

    verify(mockAdminClient).listTopics();
    verify(mockAdminClient).createTopics(anyList());
  }

  /**
   * Tests the private createKafkaConnectTopics method using reflection.
   * Verifies that topics are created for each table specified in the properties.
   *
   * @throws Exception if there is an error accessing or invoking the method
   */
  @Test
  void testCreateKafkaConnectTopics() throws Exception {
    when(mockProperties.getProperty("kafka.connect.tables", null)).thenReturn("table1,public.table2,table3");
    when(mockProperties.getProperty("kafka.topic.partitions")).thenReturn("3");

    KafkaFuture<Set<String>> topicsFuture = mock(KafkaFuture.class);
    when(mockListTopicsResult.names()).thenReturn(topicsFuture);
    when(topicsFuture.get()).thenReturn(Collections.emptySet());
    when(mockAdminClient.listTopics()).thenReturn(mockListTopicsResult);
    when(mockAdminClient.createTopics(anyList())).thenReturn(mockCreateTopicsResult);

    Method method = AsyncProcessStartup.class.getDeclaredMethod("createKafkaConnectTopics", Properties.class, AdminClient.class);
    method.setAccessible(true);
    method.invoke(asyncProcessStartup, mockProperties, mockAdminClient);

    verify(mockAdminClient, times(3)).createTopics(anyList());
  }

  /**
   * Tests the private getGroupId method using reflection.
   * Verifies that the group ID is generated correctly from the job line.
   *
   * @throws Exception if there is an error accessing or invoking the method
   */
  @Test
  void testGetGroupId() throws Exception {
    when(mockJob.getName()).thenReturn("Test Job");
    when(mockJobLine.getJobsJob()).thenReturn(mockJob);

    Method method = AsyncProcessStartup.class.getDeclaredMethod("getGroupId", JobLine.class);
    method.setAccessible(true);
    String result = (String) method.invoke(asyncProcessStartup, mockJobLine);

    assertEquals("etendo-ap-group-test-job", result);
  }

  /**
   * Tests the private getNumPartitions method when no partition property is set.
   * Expects the default value to be returned.
   *
   * @throws Exception if there is an error accessing or invoking the method
   */
  @Test
  void testGetNumPartitionsDefault() throws Exception {
    when(mockProperties.containsKey("kafka.topic.partitions")).thenReturn(false);

    Method method = AsyncProcessStartup.class.getDeclaredMethod("getNumPartitions");
    method.setAccessible(true);
    int result = (int) method.invoke(null);

    assertEquals(5, result);
  }

  /**
   * Tests the private getNumPartitions method when a valid partition property is set.
   * Expects the configured value to be returned.
   *
   * @throws Exception if there is an error accessing or invoking the method
   */
  @Test
  void testGetNumPartitionsConfigured() throws Exception {
    when(mockProperties.containsKey("kafka.topic.partitions")).thenReturn(true);
    when(mockProperties.getProperty("kafka.topic.partitions")).thenReturn("10");

    Method method = AsyncProcessStartup.class.getDeclaredMethod("getNumPartitions");
    method.setAccessible(true);
    int result = (int) method.invoke(null);

    assertEquals(10, result);
  }

  /**
   * Tests the private getNumPartitions method when an invalid partition property is set.
   * Expects the default value to be returned if parsing fails.
   *
   * @throws Exception if there is an error accessing or invoking the method
   */
  @Test
  void testGetNumPartitionsInvalidNumber() throws Exception {
    when(mockProperties.containsKey("kafka.topic.partitions")).thenReturn(true);
    when(mockProperties.getProperty("kafka.topic.partitions")).thenReturn("invalid");

    Method method = AsyncProcessStartup.class.getDeclaredMethod("getNumPartitions");
    method.setAccessible(true);
    int result = (int) method.invoke(null);

    assertEquals(5, result);
  }

  /**
   * Tests the private existsOrCreateTopic method for a new topic using reflection.
   * Verifies that the topic is created if it does not exist.
   *
   * @throws Exception if there is an error accessing or invoking the method
   */
  @Test
  void testExistsOrCreateTopicNew() throws Exception {
    String topicName = "new-topic";
    int partitions = 3;

    KafkaFuture<Set<String>> topicsFuture = mock(KafkaFuture.class);
    when(mockListTopicsResult.names()).thenReturn(topicsFuture);
    when(topicsFuture.get()).thenReturn(Collections.emptySet());
    when(mockAdminClient.listTopics()).thenReturn(mockListTopicsResult);
    when(mockAdminClient.createTopics(anyList())).thenReturn(mockCreateTopicsResult);

    Method method = AsyncProcessStartup.class.getDeclaredMethod("existsOrCreateTopic", AdminClient.class, String.class, int.class);
    method.setAccessible(true);
    method.invoke(asyncProcessStartup, mockAdminClient, topicName, partitions);

    verify(mockAdminClient).listTopics();

  }

  /**
   * Tests the private existsOrCreateTopic method for an existing topic using reflection.
   * Verifies that the topic is not created if it already exists.
   *
   * @throws Exception if there is an error accessing or invoking the method
   */
  @Test
  void testExistsOrCreateTopicExists() throws Exception {
    String topicName = "existing-topic";
    int partitions = 3;

    KafkaFuture<Set<String>> topicsFuture = mock(KafkaFuture.class);
    when(mockListTopicsResult.names()).thenReturn(topicsFuture);
    when(topicsFuture.get()).thenReturn(Collections.singleton(topicName));
    when(mockAdminClient.listTopics()).thenReturn(mockListTopicsResult);

    KafkaFuture<Map<String, TopicDescription>> descriptionFuture = mock(KafkaFuture.class);
    TopicDescription topicDescription = mock(TopicDescription.class);
    TopicPartitionInfo partitionInfo = mock(TopicPartitionInfo.class);
    when(mockDescribeTopicsResult.all()).thenReturn(descriptionFuture);
    when(descriptionFuture.get()).thenReturn(Collections.singletonMap(topicName, topicDescription));
    when(topicDescription.partitions()).thenReturn(Arrays.asList(partitionInfo, partitionInfo, partitionInfo));
    when(mockAdminClient.describeTopics(anyList())).thenReturn(mockDescribeTopicsResult);

    Method method = AsyncProcessStartup.class.getDeclaredMethod("existsOrCreateTopic", AdminClient.class, String.class, int.class);
    method.setAccessible(true);
    method.invoke(asyncProcessStartup, mockAdminClient, topicName, partitions);

    verify(mockAdminClient).listTopics();
    verify(mockAdminClient).describeTopics(Collections.singletonList(topicName));
    verify(mockAdminClient, never()).createTopics(anyList());
  }

  /**
   * Tests the private configureJobScheduler method using reflection.
   * Verifies that a scheduler is created and stored for the given job.
   *
   * @throws Exception if there is an error accessing or invoking the method
   */
  @Test
  void testConfigureJobScheduler() throws Exception {
    when(mockJob.getId()).thenReturn("job-123");
    when(mockJob.get("etapParallelThreads")).thenReturn("4");

    Method method = AsyncProcessStartup.class.getDeclaredMethod("configureJobScheduler", Job.class);
    method.setAccessible(true);
    method.invoke(asyncProcessStartup, mockJob);

    Field field = AsyncProcessStartup.class.getDeclaredField("jobSchedulers");
    field.setAccessible(true);
    Map<String, ScheduledExecutorService> schedulers = (Map<String, ScheduledExecutorService>) field.get(asyncProcessStartup);
    assertTrue(schedulers.containsKey("job-123"));
    assertNotNull(schedulers.get("job-123"));
  }

  /**
   * Tests the private getJobScheduler method using reflection.
   * Verifies that the correct scheduler is returned for a given job ID.
   *
   * @throws Exception if there is an error accessing or invoking the method
   */
  @Test
  void testGetJobScheduler() throws Exception {
    String jobId = "job-123";
    ScheduledExecutorService mockScheduler = mock(ScheduledExecutorService.class);

    Field field = AsyncProcessStartup.class.getDeclaredField("jobSchedulers");
    field.setAccessible(true);
    Map<String, ScheduledExecutorService> schedulers = new HashMap<>();
    schedulers.put(jobId, mockScheduler);
    field.set(asyncProcessStartup, schedulers);

    Method method = AsyncProcessStartup.class.getDeclaredMethod("getJobScheduler", String.class);
    method.setAccessible(true);
    ScheduledExecutorService result = (ScheduledExecutorService) method.invoke(asyncProcessStartup, jobId);

    assertEquals(mockScheduler, result);
  }

  /**
   * Tests the private getJobParallelThreads method when no parallel threads are configured.
   * Expects the default value to be returned.
   *
   * @throws Exception if there is an error accessing or invoking the method
   */
  @Test
  void testGetJobParallelThreadsDefault() throws Exception {
    when(mockJob.get("etapParallelThreads")).thenReturn(null);

    Method method = AsyncProcessStartup.class.getDeclaredMethod("getJobParallelThreads", Job.class);
    method.setAccessible(true);
    int result = (int) method.invoke(asyncProcessStartup, mockJob);

    assertEquals(8, result);
  }

  /**
   * Tests the private getJobParallelThreads method when a value is configured.
   * Expects the configured value to be returned.
   *
   * @throws Exception if there is an error accessing or invoking the method
   */
  @Test
  void testGetJobParallelThreadsConfigured() throws Exception {
    when(mockJob.get("etapParallelThreads")).thenReturn("12");

    Method method = AsyncProcessStartup.class.getDeclaredMethod("getJobParallelThreads", Job.class);
    method.setAccessible(true);
    int result = (int) method.invoke(asyncProcessStartup, mockJob);

    assertEquals(12, result);
  }

  /**
   * Tests the private getJobLineConfig method using reflection.
   * Verifies that the configuration is correctly extracted from the job line.
   *
   * @throws Exception if there is an error accessing or invoking the method
   */
  @Test
  void testGetJobLineConfig() throws Exception {
    when(mockJobLine.get("etapMaxRetries")).thenReturn("5");
    when(mockJobLine.get("etapRetryDelayMs")).thenReturn("2000");
    when(mockJobLine.get("etapPrefetchCount")).thenReturn("10");

    Method method = AsyncProcessStartup.class.getDeclaredMethod("getJobLineConfig", JobLine.class);
    method.setAccessible(true);
    AsyncProcessConfig result = (AsyncProcessConfig) method.invoke(asyncProcessStartup, mockJobLine);

    assertEquals(5, result.getMaxRetries());
    assertEquals(2000, result.getRetryDelayMs());
    assertEquals(10, result.getPrefetchCount());
  }

  /**
   * Tests the private getJobLineConfig method when no configuration is set on the job line.
   * Expects default values to be used.
   *
   * @throws Exception if there is an error accessing or invoking the method
   */
  @Test
  void testGetJobLineConfigDefaults() throws Exception {
    when(mockJobLine.get("etapMaxRetries")).thenReturn(null);
    when(mockJobLine.get("etapRetryDelayMs")).thenReturn(null);
    when(mockJobLine.get("etapPrefetchCount")).thenReturn(null);

    Method method = AsyncProcessStartup.class.getDeclaredMethod("getJobLineConfig", JobLine.class);
    method.setAccessible(true);
    AsyncProcessConfig result = (AsyncProcessConfig) method.invoke(asyncProcessStartup, mockJobLine);

    assertEquals(3, result.getMaxRetries());
    assertEquals(1000, result.getRetryDelayMs());
    assertEquals(1, result.getPrefetchCount());
  }

  /**
   * Tests the private isAsyncJobsEnabled method when async jobs are enabled in the properties.
   * Expects the method to return true.
   *
   * @throws Exception if there is an error accessing or invoking the method
   */
  @Test
  void testIsAsyncJobsEnabledTrue() throws Exception {
    when(mockProperties.containsKey("kafka.enable")).thenReturn(true);
    when(mockProperties.getProperty("kafka.enable", "false")).thenReturn("true");

    Method method = AsyncProcessStartup.class.getDeclaredMethod("isAsyncJobsEnabled");
    method.setAccessible(true);
    boolean result = (boolean) method.invoke(asyncProcessStartup);

    assertTrue(result);
  }

  /**
   * Tests the private isAsyncJobsEnabled method when async jobs are disabled in the properties.
   * Expects the method to return false.
   *
   * @throws Exception if there is an error accessing or invoking the method
   */
  @Test
  void testIsAsyncJobsEnabledFalse() throws Exception {
    when(mockProperties.containsKey("kafka.enable")).thenReturn(true);
    when(mockProperties.getProperty("kafka.enable", "false")).thenReturn("false");

    Method method = AsyncProcessStartup.class.getDeclaredMethod("isAsyncJobsEnabled");
    method.setAccessible(true);
    boolean result = (boolean) method.invoke(asyncProcessStartup);

    assertFalse(result);
  }

  /**
   * Tests the private convertState method using reflection.
   * Verifies that the correct AsyncProcessState is returned for each input string.
   *
   * @throws Exception if there is an error accessing or invoking the method
   */
  @Test
  void testConvertState() throws Exception {
    Method method = AsyncProcessStartup.class.getDeclaredMethod("convertState", String.class);
    method.setAccessible(true);

    assertEquals(AsyncProcessState.WAITING, method.invoke(asyncProcessStartup, "WAITING"));
    assertEquals(AsyncProcessState.ACCEPTED, method.invoke(asyncProcessStartup, "ACCEPTED"));
    assertEquals(AsyncProcessState.DONE, method.invoke(asyncProcessStartup, "DONE"));
    assertEquals(AsyncProcessState.REJECTED, method.invoke(asyncProcessStartup, "REJECTED"));
    assertEquals(AsyncProcessState.ERROR, method.invoke(asyncProcessStartup, "ERROR"));
    assertEquals(AsyncProcessState.STARTED, method.invoke(asyncProcessStartup, "UNKNOWN"));
    assertEquals(AsyncProcessState.STARTED, method.invoke(asyncProcessStartup, (String) null));
  }

  /**
   * Tests the private calculateErrorTopic method using reflection.
   * Verifies that the error topic is calculated correctly from the job.
   *
   * @throws Exception if there is an error accessing or invoking the method
   */
  @Test
  void testCalculateErrorTopic() throws Exception {
    when(mockJob.getEtapErrortopic()).thenReturn("custom-error-topic");

    Method method = AsyncProcessStartup.class.getDeclaredMethod("calculateErrorTopic", Job.class);
    method.setAccessible(true);
    String result = (String) method.invoke(asyncProcessStartup, mockJob);

    assertEquals("custom-error-topic", result);
  }

  /**
   * Tests the private calculateErrorTopic method when no error topic is set on the job.
   * Expects a default error topic to be generated.
   *
   * @throws Exception if there is an error accessing or invoking the method
   */
  @Test
  void testCalculateErrorTopicDefault() throws Exception {
    when(mockJob.getEtapErrortopic()).thenReturn("");
    when(mockJob.getName()).thenReturn("TestJob");

    Method method = AsyncProcessStartup.class.getDeclaredMethod("calculateErrorTopic", Job.class);
    method.setAccessible(true);
    String result = (String) method.invoke(asyncProcessStartup, mockJob);

    assertTrue(result.contains("error"));
  }

  /**
   * Tests the private calculateNextTopic method using reflection.
   * Verifies that the next topic is calculated correctly from the job line and job lines list.
   *
   * @throws Exception if there is an error accessing or invoking the method
   */
  @Test
  void testCalculateNextTopic() throws Exception {
    List<JobLine> jobLines = List.of(mockJobLine);
    when(mockJobLine.getEtapTargettopic()).thenReturn("custom-target");
    when(mockJobLine.getJobsJob()).thenReturn(mockJob);

    Method method = AsyncProcessStartup.class.getDeclaredMethod("calculateNextTopic", JobLine.class, List.class);
    method.setAccessible(true);
    String result = (String) method.invoke(asyncProcessStartup, mockJobLine, jobLines);

    assertEquals("custom-target", result);
  }

  /**
   * Tests the private calculateCurrentTopic method using reflection.
   * Verifies that the current topic is calculated correctly for the first job line.
   *
   * @throws Exception if there is an error accessing or invoking the method
   */
  @Test
  void testCalculateCurrentTopic() throws Exception {
    List<JobLine> jobLines = List.of(mockJobLine);
    when(mockJob.getEtapInitialTopic()).thenReturn("initial-topic");
    when(mockJobLine.getJobsJob()).thenReturn(mockJob);
    when(mockJobLine.getLineNo()).thenReturn(1L);

    Method method = AsyncProcessStartup.class.getDeclaredMethod("calculateCurrentTopic", JobLine.class, List.class);
    method.setAccessible(true);
    String result = (String) method.invoke(asyncProcessStartup, mockJobLine, jobLines);

    assertEquals("initial-topic", result);
  }

  /**
   * Tests the private getKafkaHost method when no kafka.url or docker property is set.
   * Expects the default Kafka host to be returned.
   *
   * @throws Exception if there is an error accessing or invoking the method
   */
  @Test
  void testGetKafkaHostDefault() throws Exception {
    when(mockProperties.containsKey("kafka.url")).thenReturn(false);
    when(mockProperties.containsKey("docker_com.etendoerp.tomcat")).thenReturn(false);

    Method method = AsyncProcessStartup.class.getDeclaredMethod("getKafkaHost", Properties.class);
    method.setAccessible(true);
    String result = (String) method.invoke(null, mockProperties);

    assertEquals("localhost:29092", result);
  }

  /**
   * Tests the private getKafkaHost method when kafka.url is set in the properties.
   * Expects the configured Kafka host to be returned.
   *
   * @throws Exception if there is an error accessing or invoking the method
   */
  @Test
  void testGetKafkaHostFromProperties() throws Exception {
    when(mockProperties.containsKey("kafka.url")).thenReturn(true);
    when(mockProperties.getProperty("kafka.url")).thenReturn("custom:9092");

    Method method = AsyncProcessStartup.class.getDeclaredMethod("getKafkaHost", Properties.class);
    method.setAccessible(true);
    String result = (String) method.invoke(null, mockProperties);

    assertEquals("custom:9092", result);
  }

  /**
   * Tests the private getKafkaHost method when docker property is set to true.
   * Expects the Docker Kafka host to be returned.
   *
   * @throws Exception if there is an error accessing or invoking the method
   */
  @Test
  void testGetKafkaHostDocker() throws Exception {
    when(mockProperties.containsKey("kafka.url")).thenReturn(false);
    when(mockProperties.containsKey("docker_com.etendoerp.tomcat")).thenReturn(true);
    when(mockProperties.getProperty("docker_com.etendoerp.tomcat", "false")).thenReturn("true");

    Method method = AsyncProcessStartup.class.getDeclaredMethod("getKafkaHost", Properties.class);
    method.setAccessible(true);
    String result = (String) method.invoke(null, mockProperties);

    assertEquals("kafka:9092", result);
  }

  /**
   * Tests the shutdown method to ensure all job schedulers are properly shut down.
   * Expects the scheduler to be shut down and await termination to be called.
   *
   * @throws Exception if there is an error accessing or invoking the method
   */
  @Test
  void testShutdown() throws Exception {
    ScheduledExecutorService mockScheduler = mock(ScheduledExecutorService.class);
    when(mockScheduler.awaitTermination(anyLong(), any())).thenReturn(true);

    Field field = AsyncProcessStartup.class.getDeclaredField("jobSchedulers");
    field.setAccessible(true);
    Map<String, ScheduledExecutorService> schedulers = new HashMap<>();
    schedulers.put("job-123", mockScheduler);
    field.set(asyncProcessStartup, schedulers);

    asyncProcessStartup.shutdown();

    verify(mockScheduler).shutdown();
    verify(mockScheduler).awaitTermination(anyLong(), any());
  }

  /**
   * Tests the shutdown method when a scheduler does not terminate within the timeout.
   * Expects shutdownNow to be called after awaitTermination fails.
   *
   * @throws Exception if there is an error accessing or invoking the method
   */
  @Test
  void testShutdownWithTimeout() throws Exception {
    ScheduledExecutorService mockScheduler = mock(ScheduledExecutorService.class);
    when(mockScheduler.awaitTermination(anyLong(), any())).thenReturn(false);

    Field field = AsyncProcessStartup.class.getDeclaredField("jobSchedulers");
    field.setAccessible(true);
    Map<String, ScheduledExecutorService> schedulers = new HashMap<>();
    schedulers.put("job-123", mockScheduler);
    field.set(asyncProcessStartup, schedulers);

    asyncProcessStartup.shutdown();

    verify(mockScheduler).shutdown();
    verify(mockScheduler).awaitTermination(anyLong(), any());
    verify(mockScheduler).shutdownNow();
  }
}