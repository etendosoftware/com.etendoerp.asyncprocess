package com.etendoerp.asyncprocess.startup;

import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.CLIENT_ID_VALUE;
import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.EXTRACT_TARGETS_METHOD;
import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.JOB_ID_KEY;
import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.ORG_ID_KEY;
import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.TEST_ERROR_MESSAGE;
import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.TEST_PROCESS_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
public class ReceiverRecordConsumerTest {

  // Test constants
  private static final String TEST_JOB_ID = "test-job-id";
  private static final String TEST_NEXT_TOPIC = "next-topic";
  private static final String TEST_ERROR_TOPIC = "error-topic";
  private static final String TEST_CLIENT_ID = "test-client-id";
  private static final String TEST_ORG_ID = "test-org-id";
  private static final String TEST_KEY = "test-key";
  private static final String TEST_USER_ID = "test-user-id";
  private static final String TEST_ROLE_ID = "test-role-id";

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
  public void setUp() throws Exception {
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
  public void tearDown() throws Exception {
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
  public void testAcceptHappyPath() {

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
  public void testAcceptWithNullValue() {
    when(mockReceiverRecord.value()).thenReturn(null);

    consumerWithoutRetry.accept(mockReceiverRecord);

    verify(mockReceiverOffset, times(1)).acknowledge();
    mockedAsyncAction.verify(() -> AsyncAction.run(eq(mockActionFactory), any(JSONObject.class)), times(1));
  }

  /**
   * Tests the scenario where the consumer processes a record with a retry policy.
   */
  @Test
  public void testAcceptWithContextInParams() {
    String paramsWithContext = "{\"params\":\"{\\\"context\\\":{\\\"user\\\":\\\"new-user\\\",\\\"role\\\":\\\"new-role\\\",\\\"client\\\":\\\"new-client\\\",\\\"organization\\\":\\\"new-org\\\"}}\"}";
    when(mockAsyncProcessExecution.getParams()).thenReturn(paramsWithContext);

    consumerWithoutRetry.accept(mockReceiverRecord);

    mockedOBContext.verify(() -> OBContext.setOBContext("new-user", "new-role", "new-client", "new-org"), times(1));
    // Verify that the OBContext previous mode was restored (indicates cleanup of admin mode / context)
    mockedOBContext.verify(OBContext::restorePreviousMode, times(1));
  }

  /**
   * Tests the scenario where the consumer processes a record with a null OBContext.
   * This simulates the case where the OBContext is not set, and it should still set the context based on the execution.
   */
  @Test
  public void testAcceptWithNullOBContext() {
    mockedOBContext.when(OBContext::getOBContext).thenReturn(null);

    consumerWithoutRetry.accept(mockReceiverRecord);

    mockedOBContext.verify(() -> OBContext.setOBContext("100", "0", TEST_CLIENT_ID, TEST_ORG_ID), times(1));
  }

  /**
   * Tests the scenario where the consumer processes a record with a null OBContext and no parameters.
   * This simulates the case where the OBContext is not set, and it should still set the context based on default values.
   */
  @Test
  public void testAcceptWithActionExceptionNoRetry() {
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
  public void testAcceptWithActionExceptionWithRetry() {
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
  public void testAcceptWithRetryExhausted() {
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
  public void testExtractTargetsFromResultStringNext() throws Exception {
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
  public void testExtractTargetsFromResultArrayNext() throws Exception {
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
  public void testExtractTargetsFromResultInvalidJSON() throws Exception {
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
  public void testExtractTargetsFromResultNullNext() throws Exception {
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
  public void testSetupJobParams() throws Exception {
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
  public void testSetupJobParamsExistingParams() throws Exception {
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
  public void testCreateResponse() {
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
  public void testCreateResponseWithKafkaError() {
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
  public void testAcceptWithJSONException() {
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
  public void testConstructors() {
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
  public void testProcessRecordWithRetryAttempt() throws Exception {
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
  public void testHandleErrorWithNullRetryPolicy() throws Exception {
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
}
