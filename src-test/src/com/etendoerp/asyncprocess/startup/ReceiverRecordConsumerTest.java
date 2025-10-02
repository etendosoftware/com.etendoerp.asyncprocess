package com.etendoerp.asyncprocess.startup;

import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.CLIENT_ID_VALUE;
import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.EXTRACT_TARGETS_METHOD;
import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.JOB_ID_KEY;
import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.ORG_ID_KEY;
import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.TEST_ERROR_MESSAGE;
import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.TEST_PROCESS_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.service.OBDal;
import org.openbravo.dal.service.OBCriteria;
import org.openbravo.model.ad.access.Role;
import org.openbravo.model.ad.access.User;
import org.openbravo.model.ad.system.Client;
import org.openbravo.model.common.enterprise.Organization;

import com.etendoerp.asyncprocess.model.AsyncProcessExecution;
import com.etendoerp.asyncprocess.model.AsyncProcessState;
import com.etendoerp.asyncprocess.retry.RetryPolicy;
import com.smf.jobs.Action;
import com.smf.jobs.ActionResult;
import com.smf.jobs.AsyncAction;

import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

/**
 * Unit tests for the {@link ReceiverRecordConsumer} class.
 * <p>
 * This test class verifies the behavior of the ReceiverRecordConsumer, including its handling of Kafka records,
 * retry logic, context management, and response creation. It uses Mockito for mocking dependencies and JUnit 5 for
 * structuring the tests. The tests cover normal execution, error handling, retry scenarios, and private utility methods.
 */
@MockitoSettings(strictness = Strictness.LENIENT)
@ExtendWith(MockitoExtension.class)
class ReceiverRecordConsumerTest {

  // Test constants
  private static final String TEST_JOB_ID = "test-job-id";
  private static final String TEST_NEXT_TOPIC = "next-topic";
  private static final String TEST_ERROR_TOPIC = "error-topic";
  private static final String TEST_CLIENT_ID = "test-client-id";
  private static final String TEST_ORG_ID = "test-org-id";
  private static final String TEST_KEY = "test-key";
  private static final String TEST_USER_ID = "test-user-id";
  private static final String TEST_ROLE_ID = "test-role-id";
  public static final String SETUP_CONTEXT_FROM_PARAMS = "setupContextFromParams";
  public static final String JSON_CONTEXT_PARAMS = "{\"context\":{\"user\":\"param-user\",\"role\":\"param-role\",\"client\":\"param-client\",\"organization\":\"param-org\"}}";
  public static final String PARAM_ROLE = "param-role";
  public static final String PARAM_CLIENT = "param-client";
  public static final String PARAM_ORG = "param-org";
  public static final String PARAM_USER = "param-user";
  public static final String PARAMS = "params";

  // Mocks
  @Mock
  private Supplier<Action> mockActionFactory;

  @Mock
  private ActionResult mockActionResult;

  @Mock
  private KafkaSender<String, AsyncProcessExecution> mockKafkaSender;

  @Mock
  private RetryPolicy mockRetryPolicy;

  @Mock
  private ScheduledExecutorService mockScheduler;

  @Mock
  private ReceiverRecord<String, AsyncProcessExecution> mockReceiverRecord;

  @Mock
  private AsyncProcessExecution mockAsyncProcessExecution;

  @Mock
  private ReceiverOffset mockReceiverOffset;

  @Mock
  private TopicPartition mockTopicPartition;

  @Mock
  private OBContext mockOBContext;

  @Mock
  private User mockUser;

  @Mock
  private Role mockRole;

  @Mock
  private Client mockClient;

  @Mock
  private Organization mockOrganization;

  // Static mocks
  private MockedStatic<AsyncAction> mockedAsyncAction;
  private MockedStatic<OBContext> mockedOBContext;
  private MockedStatic<Uuid> mockedUuid;
  private MockedStatic<OBDal> mockedOBDalStatic;

  // Test objects
  private ReceiverRecordConsumer consumerWithRetry;
  private ReceiverRecordConsumer consumerWithoutRetry;
  private AutoCloseable mocks;
  private Action mockActionInstance;

  /**
   * Sets up the test environment and initializes mocks before each test.
   *
   * @throws Exception
   *     if mock initialization fails
   */
  @BeforeEach
  void setUp() throws Exception {
    mocks = MockitoAnnotations.openMocks(this);

    mockedAsyncAction = mockStatic(AsyncAction.class);
    mockedOBContext = mockStatic(OBContext.class);
    mockedUuid = mockStatic(Uuid.class);

    consumerWithRetry = new ReceiverRecordConsumer(
        new ReceiverRecordConsumer.ConsumerConfig.Builder()
        .jobId(TEST_JOB_ID)
        .actionFactory(mockActionFactory)
        .nextTopic(TEST_NEXT_TOPIC)
        .errorTopic(TEST_ERROR_TOPIC)
        .targetStatus(AsyncProcessState.DONE)
        .kafkaSender(mockKafkaSender)
        .clientId(TEST_CLIENT_ID)
        .orgId(TEST_ORG_ID)
        .retryPolicy(mockRetryPolicy)
        .scheduler(mockScheduler)
        .build()
    );

    consumerWithoutRetry = new ReceiverRecordConsumer(
        new ReceiverRecordConsumer.ConsumerConfig.Builder()
        .jobId(TEST_JOB_ID)
        .actionFactory(mockActionFactory)
        .nextTopic(TEST_NEXT_TOPIC)
        .errorTopic(TEST_ERROR_TOPIC)
        .targetStatus(AsyncProcessState.DONE)
        .kafkaSender(mockKafkaSender)
        .clientId(TEST_CLIENT_ID)
        .orgId(TEST_ORG_ID)
        .build()
    );

    setupCommonMocks();
  }

  /**
   * Cleans up and closes mocks after each test.
   *
   * @throws Exception
   *     if closing mocks fails
   */
  @AfterEach
  void tearDown() throws Exception {
    if (mockedAsyncAction != null) {
      mockedAsyncAction.close();
    }
    if (mockedOBContext != null) {
      mockedOBContext.close();
    }
    if (mockedOBDalStatic != null) {
      mockedOBDalStatic.close();
    }
    if (mockedUuid != null) {
      mockedUuid.close();
    }
    if (mocks != null) {
      mocks.close();
    }
  }

  /**
   * Sets up common mock behaviors used across multiple tests.
   * This includes configuring return values and static mocks for dependencies.
   */
  private void setupCommonMocks() {
    when(mockReceiverRecord.receiverOffset()).thenReturn(mockReceiverOffset);
    when(mockReceiverRecord.key()).thenReturn(TEST_KEY);
    when(mockReceiverRecord.value()).thenReturn(mockAsyncProcessExecution);
    when(mockReceiverOffset.topicPartition()).thenReturn(mockTopicPartition);
    when(mockReceiverOffset.offset()).thenReturn(123L);

    when(mockTopicPartition.toString()).thenReturn("test-topic-0");

    when(mockAsyncProcessExecution.getDescription()).thenReturn("Test description");
  // provide params that include an inner 'params' JSON string (production expects params.has("params"))
  when(mockAsyncProcessExecution.getParams()).thenReturn("{\"params\":\"{}\"}");
    when(mockAsyncProcessExecution.getLog()).thenReturn("Test log");
    when(mockAsyncProcessExecution.getAsyncProcessId()).thenReturn(TEST_PROCESS_ID);

    when(mockActionResult.getMessage()).thenReturn("Success");

    mockedAsyncAction.when(() -> AsyncAction.run(eq(mockActionFactory), any(JSONObject.class)))
        .thenReturn(mockActionResult);

    mockedOBContext.when(OBContext::getOBContext).thenReturn(mockOBContext);
    when(mockOBContext.getUser()).thenReturn(mockUser);
    when(mockOBContext.getRole()).thenReturn(mockRole);
    when(mockOBContext.getCurrentClient()).thenReturn(mockClient);
    when(mockOBContext.getCurrentOrganization()).thenReturn(mockOrganization);

    when(mockUser.getId()).thenReturn(TEST_USER_ID);
    when(mockRole.getId()).thenReturn(TEST_ROLE_ID);
    when(mockClient.getId()).thenReturn(TEST_CLIENT_ID);
    when(mockOrganization.getId()).thenReturn(TEST_ORG_ID);

    SenderResult<String> mockSenderResult = mock(SenderResult.class);
    RecordMetadata mockRecordMetadata = mock(RecordMetadata.class);
    when(mockSenderResult.recordMetadata()).thenReturn(mockRecordMetadata);
    when(mockSenderResult.correlationMetadata()).thenReturn("test-correlation");
    when(mockRecordMetadata.topic()).thenReturn(TEST_NEXT_TOPIC);
    when(mockRecordMetadata.partition()).thenReturn(0);
    when(mockRecordMetadata.offset()).thenReturn(456L);

    Flux<SenderResult<String>> mockSenderFlux = Flux.just(mockSenderResult);
    when(mockKafkaSender.send(any(Flux.class))).thenReturn(mockSenderFlux);

    Uuid mockUuidInstance = mock(Uuid.class);
    when(mockUuidInstance.toString()).thenReturn("test-uuid");
    mockedUuid.when(Uuid::randomUuid).thenReturn(mockUuidInstance);
    // Ensure supplier.get() returns a non-null Action instance to avoid NPE in addProcessIdToParams
    mockActionInstance = mock(Action.class);
    when(mockActionFactory.get()).thenReturn(mockActionInstance);

    // Mock OBDal to avoid DB access when addProcessIdToParams queries for Process
    OBDal mockObdalInstance = mock(OBDal.class);
  @SuppressWarnings("unchecked")
  OBCriteria mockCriteria = mock(OBCriteria.class);
  Mockito.doReturn((OBCriteria) mockCriteria).when((OBCriteria) mockCriteria).add(any());
  Mockito.doReturn((OBCriteria) mockCriteria).when((OBCriteria) mockCriteria).setMaxResults(anyInt());
  Mockito.doReturn(null).when((OBCriteria) mockCriteria).uniqueResult();
  Mockito.doReturn((OBCriteria) mockCriteria).when(mockObdalInstance).createCriteria(org.openbravo.client.application.Process.class);
  // Attempt to statically mock OBDal.getInstance. If another test already registered a static mock for OBDal
  // in the current thread, Mockito will throw an exception; in that case skip creating a new static mock to avoid
  // "static mocking is already registered in the current thread" errors. This allows tests to run reliably
  // when other test classes may have left a static mock registered.
  try {
    mockedOBDalStatic = mockStatic(OBDal.class);
    mockedOBDalStatic.when(OBDal::getInstance).thenReturn(mockObdalInstance);
  } catch (org.mockito.exceptions.base.MockitoException ignored) {
    // Another static mock for OBDal is already registered in this thread; proceed without re-registering.
  }
  }

  /**
   * Tests the happy path scenario where the consumer processes a record successfully without retries.
   */
  @Test
  void testAcceptHappyPath() {

    consumerWithoutRetry.accept(mockReceiverRecord);

    verify(mockReceiverOffset, times(1)).acknowledge();
    mockedAsyncAction.verify(() -> AsyncAction.run(eq(mockActionFactory), any(JSONObject.class)), times(1));
    // Both setupOBContext and addProcessIdToParams set admin mode; both also restore. Expect two invocations.
    mockedOBContext.verify(() -> OBContext.setAdminMode(true), times(2));
    mockedOBContext.verify(OBContext::restorePreviousMode, times(2));

    ArgumentCaptor<Flux<SenderRecord<String, AsyncProcessExecution, String>>> fluxCaptor =
        ArgumentCaptor.forClass(Flux.class);
    verify(mockKafkaSender, times(1)).send(fluxCaptor.capture());
  }

  /**
   * Tests the scenario where the consumer processes a record with a retry policy.
   */
  @Test
  void testAcceptWithNullValue() {
    when(mockReceiverRecord.value()).thenReturn(null);

    consumerWithoutRetry.accept(mockReceiverRecord);

    verify(mockReceiverOffset, times(1)).acknowledge();
    mockedAsyncAction.verify(() -> AsyncAction.run(eq(mockActionFactory), any(JSONObject.class)), times(1));
  }

  /**
   * Tests the scenario where the consumer processes a record with a retry policy.
   */
  @Test
  void testAcceptWithContextInParams() {
    String paramsWithContext = "{\"params\":\"{\\\"context\\\":{\\\"user\\\":\\\"new-user\\\",\\\"role\\\":\\\"new-role\\\",\\\"client\\\":\\\"new-client\\\",\\\"organization\\\":\\\"new-org\\\"}}\"}";
    when(mockAsyncProcessExecution.getParams()).thenReturn(paramsWithContext);

    consumerWithoutRetry.accept(mockReceiverRecord);

    mockedOBContext.verify(() -> OBContext.setOBContext("new-user", "new-role", "new-client", "new-org"), times(1));
    // Verify that the OBContext previous mode was restored (indicates cleanup of admin mode / context)
    mockedOBContext.verify(OBContext::restorePreviousMode, times(2));
  }

  /**
   * Tests the scenario where the consumer processes a record with a null OBContext.
   * This simulates the case where the OBContext is not set, and it should still set the context based on the execution.
   */
  @Test
  void testAcceptWithNullOBContext() {
    mockedOBContext.when(OBContext::getOBContext).thenReturn(null);

    consumerWithoutRetry.accept(mockReceiverRecord);

    mockedOBContext.verify(() -> OBContext.setOBContext("100", "0", TEST_CLIENT_ID, TEST_ORG_ID), times(1));
  }

  /**
   * Tests the scenario where the consumer processes a record with a null OBContext and no parameters.
   * This simulates the case where the OBContext is not set, and it should still set the context based on default values.
   */
  @Test
  void testAcceptWithActionExceptionNoRetry() {
    RuntimeException testException = new RuntimeException(TEST_ERROR_MESSAGE);
    mockedAsyncAction.when(() -> AsyncAction.run(eq(mockActionFactory), any(JSONObject.class)))
        .thenThrow(testException);

    consumerWithoutRetry.accept(mockReceiverRecord);

    verify(mockReceiverOffset, times(1)).acknowledge();
    ArgumentCaptor<Flux<SenderRecord<String, AsyncProcessExecution, String>>> fluxCaptor =
        ArgumentCaptor.forClass(Flux.class);
    verify(mockKafkaSender, times(1)).send(fluxCaptor.capture());
  }

  /**
   * Tests the scenario where the consumer processes a record with an action exception and retry policy.
   * This simulates the case where an exception occurs during action execution, and the retry policy is applied.
   */
  @Test
  void testAcceptWithActionExceptionWithRetry() {
    RuntimeException testException = new RuntimeException(TEST_ERROR_MESSAGE);
    mockedAsyncAction.when(() -> AsyncAction.run(eq(mockActionFactory), any(JSONObject.class)))
        .thenThrow(testException);
    when(mockRetryPolicy.shouldRetry(1)).thenReturn(true);
    when(mockRetryPolicy.getRetryDelay(1)).thenReturn(1000L);

    consumerWithRetry.accept(mockReceiverRecord);

    verify(mockScheduler, times(1)).schedule(any(Runnable.class), eq(1000L), eq(TimeUnit.MILLISECONDS));
    verify(mockReceiverOffset, never()).acknowledge(); // Should not acknowledge until retry is done
  }

  /**
   * Tests the scenario where the consumer processes a record with an action exception and retry policy,
   * but the retry policy indicates no further retries should be attempted.
   */
  @Test
  void testAcceptWithRetryExhausted() {
    RuntimeException testException = new RuntimeException(TEST_ERROR_MESSAGE);
    mockedAsyncAction.when(() -> AsyncAction.run(eq(mockActionFactory), any(JSONObject.class)))
        .thenThrow(testException);
    when(mockRetryPolicy.shouldRetry(1)).thenReturn(false);

    consumerWithRetry.accept(mockReceiverRecord);

    verify(mockReceiverOffset, times(1)).acknowledge();
    verify(mockScheduler, never()).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    verify(mockKafkaSender, times(1)).send(any(Flux.class));
  }

  /**
   * Tests the extraction of "next" targets when the value is a single string.
   * Expects the result to contain both the default topic and the custom topic.
   *
   * @throws Exception
   *     if the private method invocation fails or JSON parsing fails
   */
  @Test
  void testExtractTargetsFromResultStringNext() throws Exception {
    when(mockActionResult.getMessage()).thenReturn("{\"next\":\"custom-topic\"}");

    List<String> targets = invokePrivateMethod(EXTRACT_TARGETS_METHOD, mockActionResult);

    assertEquals(2, targets.size());
    assertTrue(targets.contains(TEST_NEXT_TOPIC));
    assertTrue(targets.contains("custom-topic"));
  }

  /**
   * Tests the extraction of "next" targets when the value is a JSON array of strings.
   * Expects the result to contain the default topic and all items from the array.
   *
   * @throws Exception
   *     if the private method invocation fails or JSON parsing fails
   */
  @Test
  void testExtractTargetsFromResultArrayNext() throws Exception {
    when(mockActionResult.getMessage()).thenReturn("{\"next\":[\"topic1\",\"topic2\"]}");

    List<String> targets = invokePrivateMethod(EXTRACT_TARGETS_METHOD, mockActionResult);

    assertEquals(3, targets.size());
    assertTrue(targets.contains(TEST_NEXT_TOPIC));
    assertTrue(targets.contains("topic1"));
    assertTrue(targets.contains("topic2"));
  }

  /**
   * Tests the extraction of "next" targets when the message is not a valid JSON string.
   * Expects the result to contain only the default topic.
   *
   * @throws Exception
   *     if the private method invocation fails or JSON parsing fails
   */
  @Test
  void testExtractTargetsFromResultInvalidJSON() throws Exception {
    when(mockActionResult.getMessage()).thenReturn("invalid json");

    List<String> targets = invokePrivateMethod(EXTRACT_TARGETS_METHOD, mockActionResult);

    assertEquals(1, targets.size());
    assertTrue(targets.contains(TEST_NEXT_TOPIC));
  }

  /**
   * Tests the extraction of "next" targets when the "next" field is explicitly set to null.
   * Expects the result to contain only the default topic.
   *
   * @throws Exception
   *     if the private method invocation fails or JSON parsing fails
   */
  @Test
  void testExtractTargetsFromResultNullNext() throws Exception {
    when(mockActionResult.getMessage()).thenReturn("{\"next\":null}");

    List<String> targets = invokePrivateMethod(EXTRACT_TARGETS_METHOD, mockActionResult);

    assertEquals(1, targets.size());
    assertTrue(targets.contains(TEST_NEXT_TOPIC));
  }

  /**
   * Tests the setup of job parameters when the JSONObject is initially empty.
   * Expects the method to add job ID, client ID, and organization ID to the object.
   *
   * @throws Exception
   *     if the private method invocation fails
   */
  @Test
  void testSetupJobParams() throws Exception {
    JSONObject params = new JSONObject();

    invokePrivateMethod("setupJobParams", params);

    assertEquals(TEST_JOB_ID, params.getString(JOB_ID_KEY));
    assertEquals(TEST_CLIENT_ID, params.getString(CLIENT_ID_VALUE));
    assertEquals(TEST_ORG_ID, params.getString(ORG_ID_KEY));
  }

  /**
   * Tests the setup of job parameters when the JSONObject already contains values.
   * Expects the method to preserve existing values and not overwrite them.
   *
   * @throws Exception
   *     if the private method invocation fails
   */
  @Test
  void testSetupJobParamsExistingParams() throws Exception {
    JSONObject params = new JSONObject();
    params.put(JOB_ID_KEY, "existing-job-id");
    params.put(CLIENT_ID_VALUE, "existing-client-id");
    params.put(ORG_ID_KEY, "existing-org-id");

    invokePrivateMethod("setupJobParams", params);

    assertEquals("existing-job-id", params.getString(JOB_ID_KEY));
    assertEquals("existing-client-id", params.getString(CLIENT_ID_VALUE));
    assertEquals("existing-org-id", params.getString(ORG_ID_KEY));
  }

  /**
   * Tests the creation of a response record with a valid async process ID.
   * This verifies that the response record is created correctly and sent to the next topic.
   */
  @Test
  void testCreateResponse() {
    AsyncProcessExecution responseRecord = new AsyncProcessExecution();
    responseRecord.setAsyncProcessId(TEST_PROCESS_ID);

    consumerWithoutRetry.createResponse(TEST_NEXT_TOPIC, mockKafkaSender, responseRecord);

    assertNotNull(responseRecord.getId());
    assertNotNull(responseRecord.getTime());
    verify(mockKafkaSender, times(1)).send(any(Flux.class));
  }

  /**
   * Tests the creation of a response record with a null async process ID.
   * This verifies that the response record is created correctly and sent to the next topic.
   */
  @Test
  void testCreateResponseWithKafkaError() {
    AsyncProcessExecution responseRecord = new AsyncProcessExecution();
    responseRecord.setAsyncProcessId(TEST_PROCESS_ID);

    RuntimeException kafkaError = new RuntimeException("Kafka send failed");
    Flux<SenderResult<String>> errorFlux = Flux.error(kafkaError);
    when(mockKafkaSender.send(any(Flux.class))).thenReturn(errorFlux);

    consumerWithoutRetry.createResponse(TEST_NEXT_TOPIC, mockKafkaSender, responseRecord);

    verify(mockKafkaSender, times(1)).send(any(Flux.class));
  }

  /**
   * Tests the scenario where the consumer processes a record with a JSONException in the parameters.
   * This simulates the case where the parameters cannot be parsed as JSON.
   */
  @Test
  void testAcceptWithJSONException() {
    when(mockAsyncProcessExecution.getParams()).thenReturn("invalid json");

    consumerWithoutRetry.accept(mockReceiverRecord);

    verify(mockReceiverOffset, times(1)).acknowledge();
    verify(mockKafkaSender, times(1)).send(any(Flux.class));
  }

  /**
   * Tests the scenario where the consumer processes a record with a null action result.
   * This simulates the case where the action does not produce a result.
   */
  @Test
  void testConstructors() {
    ReceiverRecordConsumer basicConsumer = new ReceiverRecordConsumer(
        new ReceiverRecordConsumer.ConsumerConfig.Builder()
            .jobId(TEST_JOB_ID)
            .actionFactory(mockActionFactory)
            .nextTopic(TEST_NEXT_TOPIC)
            .errorTopic(TEST_ERROR_TOPIC)
            .targetStatus(AsyncProcessState.DONE)
            .kafkaSender(mockKafkaSender)
            .clientId(TEST_CLIENT_ID)
            .orgId(TEST_ORG_ID)
            .build()
    );
    assertNotNull(basicConsumer);

    ReceiverRecordConsumer extendedConsumer = new ReceiverRecordConsumer(
        new ReceiverRecordConsumer.ConsumerConfig.Builder()
            .jobId(TEST_JOB_ID)
            .actionFactory(mockActionFactory)
            .nextTopic(TEST_NEXT_TOPIC)
            .errorTopic(TEST_ERROR_TOPIC)
            .targetStatus(AsyncProcessState.DONE)
            .kafkaSender(mockKafkaSender)
            .clientId(TEST_CLIENT_ID)
            .orgId(TEST_ORG_ID)
            .build()
    );
    assertNotNull(extendedConsumer);
  }

  /**
   * Helper method to invoke private methods of {@link ReceiverRecordConsumer} using reflection.
   *
   * @param methodName
   *     the name of the private method to invoke
   * @param params
   *     the parameters to pass to the method
   * @param <T>
   *     the return type of the method
   * @return the result of the invoked method
   * @throws Exception
   *     if reflection fails or the method cannot be invoked
   */
  @SuppressWarnings("unchecked")
  private <T> T invokePrivateMethod(String methodName, Object... params) throws Exception {
    Class<?>[] paramTypes = new Class<?>[params.length];
    for (int i = 0; i < params.length; i++) {
      paramTypes[i] = params[i].getClass();
      // Handle special cases for interfaces
      if (params[i] instanceof ActionResult) {
        paramTypes[i] = ActionResult.class;
      } else if (params[i] instanceof JSONObject) {
        paramTypes[i] = JSONObject.class;
      }
    }

    Method method = ReceiverRecordConsumer.class.getDeclaredMethod(methodName, paramTypes);
    method.setAccessible(true);
    return (T) method.invoke(consumerWithoutRetry, params);
  }

  /**
   * Tests the private processRecord method to ensure that when an exception occurs and the retry policy allows,
   * the consumer schedules a retry with the correct delay.
   *
   * @throws Exception
   *     if reflection or invocation fails
   */
  @Test
  void testProcessRecordWithRetryAttempt() throws Exception {
    when(mockRetryPolicy.shouldRetry(2)).thenReturn(true);
    when(mockRetryPolicy.getRetryDelay(2)).thenReturn(2000L);
    RuntimeException testException = new RuntimeException("Retry test error");
    // Instead of invoking processRecord (which internally converts action exceptions into ActionResult
    // and won't reach handleError), invoke handleError directly to assert scheduling behavior.
    Method handleErrorMethod = ReceiverRecordConsumer.class.getDeclaredMethod(
        "handleError", ReceiverRecord.class, Exception.class, String.class,
        AsyncProcessExecution.class, int.class);
    handleErrorMethod.setAccessible(true);

    AsyncProcessExecution responseRecord = new AsyncProcessExecution();
    handleErrorMethod.invoke(consumerWithRetry, mockReceiverRecord, testException, "test log",
        responseRecord, 1);

    verify(mockScheduler, times(1)).schedule(any(Runnable.class), eq(2000L), eq(TimeUnit.MILLISECONDS));
  }

  /**
   * Tests the private handleError method to ensure that when no retry policy is present and an exception occurs,
   * the record is acknowledged and the response state is set to ERROR.
   *
   * @throws Exception
   *     if reflection or invocation fails
   */
  @Test
  void testHandleErrorWithNullRetryPolicy() throws Exception {
    RuntimeException testException = new RuntimeException(TEST_ERROR_MESSAGE);
    AsyncProcessExecution responseRecord = new AsyncProcessExecution();

    Method handleErrorMethod = ReceiverRecordConsumer.class.getDeclaredMethod(
        "handleError", ReceiverRecord.class, Exception.class, String.class,
        AsyncProcessExecution.class, int.class);
    handleErrorMethod.setAccessible(true);
    handleErrorMethod.invoke(consumerWithoutRetry, mockReceiverRecord, testException,
        "test log", responseRecord, 0);

    verify(mockReceiverOffset, times(1)).acknowledge();
    assertEquals(AsyncProcessState.ERROR, responseRecord.getState());
  }

  /**
   * Tests that ConsumerConfig.Builder.build() succeeds when all required fields are provided.
   */
  @Test
  void testConsumerConfigBuilderBuildSuccess() {
    ReceiverRecordConsumer.ConsumerConfig config = new ReceiverRecordConsumer.ConsumerConfig.Builder()
        .jobId(TEST_JOB_ID)
        .actionFactory(mockActionFactory)
        .nextTopic(TEST_NEXT_TOPIC)
        .errorTopic(TEST_ERROR_TOPIC)
        .targetStatus(AsyncProcessState.STARTED)
        .kafkaSender(mockKafkaSender)
        .clientId(TEST_CLIENT_ID)
        .orgId(TEST_ORG_ID)
        .build();

    assertNotNull(config);
    assertEquals(TEST_JOB_ID, config.getJobId());
    assertEquals(mockActionFactory, config.getActionFactory());
    assertEquals(TEST_NEXT_TOPIC, config.getNextTopic());
    assertEquals(TEST_ERROR_TOPIC, config.getErrorTopic());
    assertEquals(AsyncProcessState.STARTED, config.getTargetStatus());
    assertEquals(mockKafkaSender, config.getKafkaSender());
    assertEquals(TEST_CLIENT_ID, config.getClientId());
    assertEquals(TEST_ORG_ID, config.getOrgId());
  }

  /**
   * Tests that ConsumerConfig.Builder.build() throws IllegalArgumentException when jobId is null.
   */
  @Test
  void testConsumerConfigBuilderBuildNullJobId() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, this::buildConfigWithoutJobId);
    assertEquals("jobId is required", exception.getMessage());
  }

  @Test
  void testConsumerConfigBuilderBuildEmptyJobId() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, this::buildConfigWithEmptyJobId);
    assertEquals("jobId is required", exception.getMessage());
  }

  @Test
  void testConsumerConfigBuilderBuildNullActionFactory() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, this::buildConfigWithoutActionFactory);
    assertEquals("actionFactory is required", exception.getMessage());
  }

  @Test
  void testConsumerConfigBuilderBuildNullNextTopic() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, this::buildConfigWithoutNextTopic);
    assertEquals("nextTopic is required", exception.getMessage());
  }

  @Test
  void testConsumerConfigBuilderBuildEmptyNextTopic() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, this::buildConfigWithEmptyNextTopic);
    assertEquals("nextTopic is required", exception.getMessage());
  }

  @Test
  void testConsumerConfigBuilderBuildNullErrorTopic() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, this::buildConfigWithoutErrorTopic);
    assertEquals("errorTopic is required", exception.getMessage());
  }

  @Test
  void testConsumerConfigBuilderBuildEmptyErrorTopic() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, this::buildConfigWithEmptyErrorTopic);
    assertEquals("errorTopic is required", exception.getMessage());
  }

  @Test
  void testConsumerConfigBuilderBuildNullTargetStatus() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, this::buildConfigWithoutTargetStatus);
    assertEquals("targetStatus is required", exception.getMessage());
  }

  @Test
  void testConsumerConfigBuilderBuildNullKafkaSender() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, this::buildConfigWithoutKafkaSender);
    assertEquals("kafkaSender is required", exception.getMessage());
  }

  @Test
  void testConsumerConfigBuilderBuildNullClientId() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, this::buildConfigWithoutClientId);
    assertEquals("clientId is required", exception.getMessage());
  }

  @Test
  void testConsumerConfigBuilderBuildEmptyClientId() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, this::buildConfigWithEmptyClientId);
    assertEquals("clientId is required", exception.getMessage());
  }

  @Test
  void testConsumerConfigBuilderBuildNullOrgId() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, this::buildConfigWithoutOrgId);
    assertEquals("orgId is required", exception.getMessage());
  }

  @Test
  void testConsumerConfigBuilderBuildEmptyOrgId() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, this::buildConfigWithEmptyOrgId);
    assertEquals("orgId is required", exception.getMessage());
  }

  // Helper methods used as method references in assertThrows
  private void buildConfigWithoutJobId() {
    new ReceiverRecordConsumer.ConsumerConfig.Builder()
        .actionFactory(mockActionFactory)
        .nextTopic(TEST_NEXT_TOPIC)
        .errorTopic(TEST_ERROR_TOPIC)
        .targetStatus(AsyncProcessState.STARTED)
        .kafkaSender(mockKafkaSender)
        .clientId(TEST_CLIENT_ID)
        .orgId(TEST_ORG_ID)
        .build();
  }

  private void buildConfigWithEmptyJobId() {
    new ReceiverRecordConsumer.ConsumerConfig.Builder()
        .jobId("")
        .actionFactory(mockActionFactory)
        .nextTopic(TEST_NEXT_TOPIC)
        .errorTopic(TEST_ERROR_TOPIC)
        .targetStatus(AsyncProcessState.STARTED)
        .kafkaSender(mockKafkaSender)
        .clientId(TEST_CLIENT_ID)
        .orgId(TEST_ORG_ID)
        .build();
  }

  private void buildConfigWithoutActionFactory() {
    new ReceiverRecordConsumer.ConsumerConfig.Builder()
        .jobId(TEST_JOB_ID)
        .nextTopic(TEST_NEXT_TOPIC)
        .errorTopic(TEST_ERROR_TOPIC)
        .targetStatus(AsyncProcessState.STARTED)
        .kafkaSender(mockKafkaSender)
        .clientId(TEST_CLIENT_ID)
        .orgId(TEST_ORG_ID)
        .build();
  }

  private void buildConfigWithoutNextTopic() {
    new ReceiverRecordConsumer.ConsumerConfig.Builder()
        .jobId(TEST_JOB_ID)
        .actionFactory(mockActionFactory)
        .errorTopic(TEST_ERROR_TOPIC)
        .targetStatus(AsyncProcessState.STARTED)
        .kafkaSender(mockKafkaSender)
        .clientId(TEST_CLIENT_ID)
        .orgId(TEST_ORG_ID)
        .build();
  }

  private void buildConfigWithEmptyNextTopic() {
    new ReceiverRecordConsumer.ConsumerConfig.Builder()
        .jobId(TEST_JOB_ID)
        .actionFactory(mockActionFactory)
        .nextTopic("")
        .errorTopic(TEST_ERROR_TOPIC)
        .targetStatus(AsyncProcessState.STARTED)
        .kafkaSender(mockKafkaSender)
        .clientId(TEST_CLIENT_ID)
        .orgId(TEST_ORG_ID)
        .build();
  }

  private void buildConfigWithoutErrorTopic() {
    new ReceiverRecordConsumer.ConsumerConfig.Builder()
        .jobId(TEST_JOB_ID)
        .actionFactory(mockActionFactory)
        .nextTopic(TEST_NEXT_TOPIC)
        .targetStatus(AsyncProcessState.STARTED)
        .kafkaSender(mockKafkaSender)
        .clientId(TEST_CLIENT_ID)
        .orgId(TEST_ORG_ID)
        .build();
  }

  private void buildConfigWithEmptyErrorTopic() {
    new ReceiverRecordConsumer.ConsumerConfig.Builder()
        .jobId(TEST_JOB_ID)
        .actionFactory(mockActionFactory)
        .nextTopic(TEST_NEXT_TOPIC)
        .errorTopic("")
        .targetStatus(AsyncProcessState.STARTED)
        .kafkaSender(mockKafkaSender)
        .clientId(TEST_CLIENT_ID)
        .orgId(TEST_ORG_ID)
        .build();
  }

  private void buildConfigWithoutTargetStatus() {
    new ReceiverRecordConsumer.ConsumerConfig.Builder()
        .jobId(TEST_JOB_ID)
        .actionFactory(mockActionFactory)
        .nextTopic(TEST_NEXT_TOPIC)
        .errorTopic(TEST_ERROR_TOPIC)
        .kafkaSender(mockKafkaSender)
        .clientId(TEST_CLIENT_ID)
        .orgId(TEST_ORG_ID)
        .build();
  }

  private void buildConfigWithoutKafkaSender() {
    new ReceiverRecordConsumer.ConsumerConfig.Builder()
        .jobId(TEST_JOB_ID)
        .actionFactory(mockActionFactory)
        .nextTopic(TEST_NEXT_TOPIC)
        .errorTopic(TEST_ERROR_TOPIC)
        .targetStatus(AsyncProcessState.STARTED)
        .clientId(TEST_CLIENT_ID)
        .orgId(TEST_ORG_ID)
        .build();
  }

  private void buildConfigWithoutClientId() {
    new ReceiverRecordConsumer.ConsumerConfig.Builder()
        .jobId(TEST_JOB_ID)
        .actionFactory(mockActionFactory)
        .nextTopic(TEST_NEXT_TOPIC)
        .errorTopic(TEST_ERROR_TOPIC)
        .targetStatus(AsyncProcessState.STARTED)
        .kafkaSender(mockKafkaSender)
        .orgId(TEST_ORG_ID)
        .build();
  }

  private void buildConfigWithEmptyClientId() {
    new ReceiverRecordConsumer.ConsumerConfig.Builder()
        .jobId(TEST_JOB_ID)
        .actionFactory(mockActionFactory)
        .nextTopic(TEST_NEXT_TOPIC)
        .errorTopic(TEST_ERROR_TOPIC)
        .targetStatus(AsyncProcessState.STARTED)
        .kafkaSender(mockKafkaSender)
        .clientId("")
        .orgId(TEST_ORG_ID)
        .build();
  }

  private void buildConfigWithoutOrgId() {
    new ReceiverRecordConsumer.ConsumerConfig.Builder()
        .jobId(TEST_JOB_ID)
        .actionFactory(mockActionFactory)
        .nextTopic(TEST_NEXT_TOPIC)
        .errorTopic(TEST_ERROR_TOPIC)
        .targetStatus(AsyncProcessState.STARTED)
        .kafkaSender(mockKafkaSender)
        .clientId(TEST_CLIENT_ID)
        .build();
  }

  private void buildConfigWithEmptyOrgId() {
    new ReceiverRecordConsumer.ConsumerConfig.Builder()
        .jobId(TEST_JOB_ID)
        .actionFactory(mockActionFactory)
        .nextTopic(TEST_NEXT_TOPIC)
        .errorTopic(TEST_ERROR_TOPIC)
        .targetStatus(AsyncProcessState.STARTED)
        .kafkaSender(mockKafkaSender)
        .clientId(TEST_CLIENT_ID)
        .orgId("")
        .build();
  }

  /**
   * Tests setupContextFromParams with no params/after and valid existing context.
   * Should do nothing and keep existing context.
   */
  @Test
  void testSetupContextFromParamsNoParamsValidContext() throws Exception {
    JSONObject params = new JSONObject("{}");

    // Mock existing valid context
    when(mockOBContext.getUser()).thenReturn(mockUser);
    mockedOBContext.when(OBContext::getOBContext).thenReturn(mockOBContext);

    Method method = ReceiverRecordConsumer.class.getDeclaredMethod(SETUP_CONTEXT_FROM_PARAMS, JSONObject.class);
    method.setAccessible(true);
    method.invoke(consumerWithoutRetry, params);

    // Should not call setOBContext since context is valid
    mockedOBContext.verify(() -> OBContext.setOBContext(any(), any(), any(), any()), never());
  }

  /**
   * Tests setupContextFromParams with no params/after and no valid context.
   * Should set default context.
   */
  @Test
  void testSetupContextFromParamsNoParamsNoValidContext() throws Exception {
    JSONObject params = new JSONObject("{}");

    // Mock no existing context
    mockedOBContext.when(OBContext::getOBContext).thenReturn(null);

    Method method = ReceiverRecordConsumer.class.getDeclaredMethod(SETUP_CONTEXT_FROM_PARAMS, JSONObject.class);
    method.setAccessible(true);
    method.invoke(consumerWithoutRetry, params);

    // Should set default context
    mockedOBContext.verify(() -> OBContext.setOBContext("100", "0", TEST_CLIENT_ID, TEST_ORG_ID), times(1));
  }

  /**
   * Tests setupContextFromParams with params and no existing user.
   * Should extract context from params.
   */
  @Test
  void testSetupContextFromParamsWithParamsNoExistingUser() throws Exception {
    JSONObject params = new JSONObject();
    params.put(PARAMS, JSON_CONTEXT_PARAMS);

    // Mock no existing user
    when(mockOBContext.getUser()).thenReturn(null);
    mockedOBContext.when(OBContext::getOBContext).thenReturn(mockOBContext);

    Method method = ReceiverRecordConsumer.class.getDeclaredMethod(SETUP_CONTEXT_FROM_PARAMS, JSONObject.class);
    method.setAccessible(true);
    method.invoke(consumerWithoutRetry, params);

    // Should set context from params
    mockedOBContext.verify(() -> OBContext.setOBContext(PARAM_USER, PARAM_ROLE, PARAM_CLIENT,
        PARAM_ORG), times(1));
  }

  /**
   * Tests setupContextFromParams with params and existing user.
   * Should preserve previous context and use params context.
   */
  @Test
  void testSetupContextFromParamsWithParamsExistingUser() throws Exception {
    JSONObject params = new JSONObject();
    params.put(PARAMS, JSON_CONTEXT_PARAMS);

    // Mock existing user and context
    when(mockOBContext.getUser()).thenReturn(mockUser);
    when(mockUser.getId()).thenReturn("existing-user");
    when(mockOBContext.getRole()).thenReturn(mockRole);
    when(mockRole.getId()).thenReturn("existing-role");
    when(mockOBContext.getCurrentClient()).thenReturn(mockClient);
    when(mockClient.getId()).thenReturn("existing-client");
    when(mockOBContext.getCurrentOrganization()).thenReturn(mockOrganization);
    when(mockOrganization.getId()).thenReturn("existing-org");
    mockedOBContext.when(OBContext::getOBContext).thenReturn(mockOBContext);

    Method method = ReceiverRecordConsumer.class.getDeclaredMethod(SETUP_CONTEXT_FROM_PARAMS, JSONObject.class);
    method.setAccessible(true);
    method.invoke(consumerWithoutRetry, params);

    // Should use params context, not existing context
    mockedOBContext.verify(() -> OBContext.setOBContext(PARAM_USER, PARAM_ROLE, PARAM_CLIENT,
        PARAM_ORG), times(1));
  }

  /**
   * Tests setupContextFromParams with after parameter.
   * Should extract context from after.
   */
  @Test
  void testSetupContextFromParamsWithAfter() throws Exception {
    JSONObject params = new JSONObject();
    params.put("after", "{\"updatedby\":\"after-user\",\"ad_client_id\":\"after-client\",\"ad_org_id\":\"after-org\"}");

    // Mock existing user
    when(mockOBContext.getUser()).thenReturn(mockUser);
    when(mockUser.getId()).thenReturn("existing-user");
    when(mockOBContext.getRole()).thenReturn(mockRole);
    when(mockRole.getId()).thenReturn("existing-role");
    when(mockOBContext.getCurrentClient()).thenReturn(mockClient);
    when(mockClient.getId()).thenReturn("existing-client");
    when(mockOBContext.getCurrentOrganization()).thenReturn(mockOrganization);
    when(mockOrganization.getId()).thenReturn("existing-org");
    mockedOBContext.when(OBContext::getOBContext).thenReturn(mockOBContext);

    Method method = ReceiverRecordConsumer.class.getDeclaredMethod(SETUP_CONTEXT_FROM_PARAMS, JSONObject.class);
    method.setAccessible(true);
    method.invoke(consumerWithoutRetry, params);

    // Should use after context with existing role
    mockedOBContext.verify(() -> OBContext.setOBContext("after-user", null, "after-client", "after-org"), times(1));
  }

  /**
   * Tests setupContextFromParams with both params and after.
   * Params context should take precedence.
   */
  @Test
  void testSetupContextFromParamsWithParamsAndAfter() throws Exception {
    JSONObject params = new JSONObject();
    params.put(PARAMS, JSON_CONTEXT_PARAMS);
    params.put("after", "{\"updatedby\":\"after-user\",\"ad_client_id\":\"after-client\",\"ad_org_id\":\"after-org\"}");

    // Mock existing user
    when(mockOBContext.getUser()).thenReturn(mockUser);
    mockedOBContext.when(OBContext::getOBContext).thenReturn(mockOBContext);

    Method method = ReceiverRecordConsumer.class.getDeclaredMethod(SETUP_CONTEXT_FROM_PARAMS, JSONObject.class);
    method.setAccessible(true);
    method.invoke(consumerWithoutRetry, params);

    mockedOBContext.verify(() -> OBContext.setOBContext("after-user", PARAM_ROLE, "after-client", "after-org"), times(1));
  }

  /**
   * Tests setupContextFromParams with null role in context.
   * Should set role to null.
   */
  @Test
  void testSetupContextFromParamsNullRole() throws Exception {
    JSONObject params = new JSONObject();
    params.put(PARAMS, "{\"context\":{\"user\":\"test-user\",\"role\":\"null\",\"client\":\"test-client\",\"organization\":\"test-org\"}}");

    // Mock existing user
    when(mockOBContext.getUser()).thenReturn(mockUser);
    mockedOBContext.when(OBContext::getOBContext).thenReturn(mockOBContext);

    Method method = ReceiverRecordConsumer.class.getDeclaredMethod(SETUP_CONTEXT_FROM_PARAMS, JSONObject.class);
    method.setAccessible(true);
    method.invoke(consumerWithoutRetry, params);

    // Should set role to null
    mockedOBContext.verify(() -> OBContext.setOBContext("test-user", null, "test-client", "test-org"), times(1));
  }

  /**
   * Tests setupContextFromParams with empty role in context.
   * Should set role to null.
   */
  @Test
  void testSetupContextFromParamsEmptyRole() throws Exception {
    JSONObject params = new JSONObject();
    params.put(PARAMS, "{\"context\":{\"user\":\"test-user\",\"role\":\"\",\"client\":\"test-client\",\"organization\":\"test-org\"}}");

    // Mock existing user
    when(mockOBContext.getUser()).thenReturn(mockUser);
    mockedOBContext.when(OBContext::getOBContext).thenReturn(mockOBContext);

    Method method = ReceiverRecordConsumer.class.getDeclaredMethod(SETUP_CONTEXT_FROM_PARAMS, JSONObject.class);
    method.setAccessible(true);
    method.invoke(consumerWithoutRetry, params);

    // Should set role to null
    mockedOBContext.verify(() -> OBContext.setOBContext("test-user", null, "test-client", "test-org"), times(1));
  }

  /**
   * Tests the scenario where the consumer processes a record with invalid context parameters.
   * This simulates the case where user, role, client or organization are missing from context,
   * which should throw an OBException with the message "Invalid context in message parameters. user, role, client and organization are required."
   */
  @Test
  void testAcceptWithInvalidContextParameters() {
    // Setup context with missing required parameters (e.g., missing user)
    String paramsWithInvalidContext = "{\"params\":\"{\\\"context\\\":{\\\"role\\\":\\\"test-role\\\",\\\"client\\\":\\\"test-client\\\",\\\"organization\\\":\\\"test-org\\\"}}\"}";
    when(mockAsyncProcessExecution.getParams()).thenReturn(paramsWithInvalidContext);

    // Mock the OBException being thrown for invalid context
    RuntimeException contextException = new org.openbravo.base.exception.OBException(
        "Invalid context in message parameters. user, role, client and organization are required.");

    // Configure the consumer to throw the exception when processing invalid context
    mockedAsyncAction.when(() -> AsyncAction.run(eq(mockActionFactory), any(JSONObject.class)))
        .thenThrow(contextException);

    consumerWithoutRetry.accept(mockReceiverRecord);

    // Verify that the record is acknowledged and error handling is triggered
    verify(mockReceiverOffset, times(1)).acknowledge();
    ArgumentCaptor<Flux<SenderRecord<String, AsyncProcessExecution, String>>> fluxCaptor =
        ArgumentCaptor.forClass(Flux.class);
    verify(mockKafkaSender, times(1)).send(fluxCaptor.capture());
  }

  /**
   * Tests the scenario where an error occurs while obtaining the action class name.
   * This simulates the case where reflection or class loading fails,
   * which should log an error and throw an OBException wrapping the original exception.
   */
  @Test
  void testAcceptWithActionClassNameError() {
    // Simulate a ClassNotFoundException or similar error when getting action class name
    ClassNotFoundException classError = new ClassNotFoundException("Action class not found");
    RuntimeException actionClassException = new org.openbravo.base.exception.OBException(classError);

    mockedAsyncAction.when(() -> AsyncAction.run(eq(mockActionFactory), any(JSONObject.class)))
        .thenThrow(actionClassException);

    consumerWithoutRetry.accept(mockReceiverRecord);

    // Verify that the error is handled properly
    verify(mockReceiverOffset, times(1)).acknowledge();
    ArgumentCaptor<Flux<SenderRecord<String, AsyncProcessExecution, String>>> fluxCaptor =
        ArgumentCaptor.forClass(Flux.class);
    verify(mockKafkaSender, times(1)).send(fluxCaptor.capture());
  }

  /**
   * Tests the scenario where the ActionResult contains invalid JSON in its message.
   * This simulates the case where ActionResult.getMessage() returns malformed JSON,
   * which should log a warning about "Invalid JSON in ActionResult message".
   */
  @Test
  void testAcceptWithInvalidJSONInActionResult() {
    // Setup ActionResult with invalid JSON message
    when(mockActionResult.getMessage()).thenReturn("{ invalid json structure");

    // Configure AsyncAction to return the ActionResult with invalid JSON
    mockedAsyncAction.when(() -> AsyncAction.run(eq(mockActionFactory), any(JSONObject.class)))
        .thenReturn(mockActionResult);

    consumerWithoutRetry.accept(mockReceiverRecord);

    // Verify that processing continues despite invalid JSON
    verify(mockReceiverOffset, times(1)).acknowledge();
    mockedAsyncAction.verify(() -> AsyncAction.run(eq(mockActionFactory), any(JSONObject.class)), times(1));
    verify(mockKafkaSender, times(1)).send(any(Flux.class));

    // The invalid JSON should be handled gracefully and processing should continue
    // The extractTargetsFromResult method should default to using the nextTopic only
  }

  /**
   * Tests the scenario where ActionResult message parsing throws a JSONException.
   * This verifies that the consumer handles JSON parsing errors gracefully by falling back
   * to default behavior and logs a warning message.
   */
  @Test
  void testAcceptWithJSONExceptionInActionResultProcessing() {
    // Setup ActionResult with completely malformed JSON
    when(mockActionResult.getMessage()).thenReturn("not json at all {{{");

    mockedAsyncAction.when(() -> AsyncAction.run(eq(mockActionFactory), any(JSONObject.class)))
        .thenReturn(mockActionResult);

    consumerWithoutRetry.accept(mockReceiverRecord);

    // Verify normal processing flow despite JSON exception
    verify(mockReceiverOffset, times(1)).acknowledge();
    verify(mockKafkaSender, times(1)).send(any(Flux.class));
  }

}
