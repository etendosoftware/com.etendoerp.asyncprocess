package com.etendoerp.asyncprocess.action;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Iterator;

import javax.enterprise.inject.Instance;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.codehaus.jettison.json.JSONObject;
import org.hibernate.criterion.Criterion;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.service.OBCriteria;
import org.openbravo.dal.service.OBDal;

import com.etendoerp.asyncprocess.data.LogHeader;
import com.etendoerp.asyncprocess.hooks.LogPersistorIdentifierHook;
import com.smf.jobs.ActionResult;

/**
 * Unit tests for the {@link LogPersistorProcessor} class.
 * <p>
 * This test class uses Mockito and JUnit 5 to mock dependencies and verify the behavior of the LogPersistorProcessor.
 * It focuses on testing the action method with minimal parameters and ensures that the processor works as expected
 * when interacting with mocked dependencies such as OBDal, OBContext, and hooks.
 */
class LogPersistorProcessorTest {

  /**
   * Mocked CDI Instance for hooks injection.
   */
  @Mock
  private Instance<LogPersistorIdentifierHook> hooks;

  /**
   * The processor under test, with mocks injected.
   */
  @InjectMocks
  private LogPersistorProcessor processor;

  /**
   * Initializes mocks and injects the mock hooks instance into the processor before each test.
   *
   * @throws Exception
   *     if reflection fails
   */
  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    processor = new LogPersistorProcessor();
    // Inject the mock into the private field using reflection
    Field hooksField = LogPersistorProcessor.class.getDeclaredField("hooks");
    hooksField.setAccessible(true);
    hooksField.set(processor, hooks);
  }

  /**
   * Tests the action method of LogPersistorProcessor with minimal parameters.
   * <p>
   * This test mocks the required static and instance methods to avoid NullPointerExceptions and verifies
   * that the action method returns a non-null result when provided with minimal valid input.
   *
   * @throws Exception
   *     if any error occurs during the test
   */
  @Test
  void testActionWithMinimalParams() throws Exception {
    // Mock hooks.iterator() to avoid NullPointerException
    Iterator<LogPersistorIdentifierHook> mockIterator = mock(Iterator.class);
    when(hooks.iterator()).thenReturn(mockIterator);
    when(mockIterator.hasNext()).thenReturn(false);

    // Build the params JSON as a nested string for Debezium
    JSONObject after = new JSONObject();
    after.put("assigned_user", "100");
    after.put("assigned_role", "0");
    after.put("ad_client_id", "0");
    after.put("ad_org_id", "0");
    JSONObject debeziumParams = new JSONObject();
    debeziumParams.put("after", after);
    JSONObject params = new JSONObject();
    params.put("asyncProcessId", "test-id");
    params.put("params", debeziumParams.toString());
    params.put("state", "SUCCESS");
    MutableBoolean isStopped = new MutableBoolean(false);

    // Static mock for OBContext and OBDal
    try (MockedStatic<OBContext> obContextMock = org.mockito.Mockito.mockStatic(OBContext.class);
         MockedStatic<OBDal> obDalMock = org.mockito.Mockito.mockStatic(OBDal.class)) {
      OBContext obContext = mock(OBContext.class);
      obContextMock.when(OBContext::getOBContext).thenReturn(obContext);
      obContextMock.when(() -> OBContext.setOBContext(anyString(), anyString(), anyString(), anyString())).then(
          invocation -> null);
      when(obContext.isInAdministratorMode()).thenReturn(true);

      OBDal obDal = mock(OBDal.class);
      obDalMock.when(OBDal::getInstance).thenReturn(obDal);
      doNothing().when(obDal).save(any());
      doNothing().when(obDal).commitAndClose();
      doNothing().when(obDal).flush();
      com.etendoerp.asyncprocess.data.LogHeader logHeaderSpy = spy(new com.etendoerp.asyncprocess.data.LogHeader());
      doReturn(new java.util.ArrayList<>()).when(logHeaderSpy).getETAPLogList();
      doNothing().when(obDal).save(logHeaderSpy);

      com.etendoerp.asyncprocess.data.Log logSpy = spy(new com.etendoerp.asyncprocess.data.Log());
      doNothing().when(obDal).save(logSpy);

      OBCriteria<LogHeader> obCriteria = mock(OBCriteria.class);
      when(obDal.createCriteria(LogHeader.class)).thenReturn(obCriteria);
      when(obCriteria.add(any(Criterion.class))).thenReturn(obCriteria);
      when(obCriteria.setMaxResults(anyInt())).thenReturn(obCriteria);
      when(obCriteria.uniqueResult()).thenReturn(null);

      ActionResult result = processor.action(params, isStopped);
      assertNotNull(result);
    }
  }

  /**
   * Tests the action method of LogPersistorProcessor with malformed parameters JSON.
   * <p>
   * This test provides a malformed JSON string as parameters and verifies that the action method
   * handles the error gracefully without throwing exceptions.
   *
   * @throws Exception
   *     if any error occurs during the test
   */
  @Test
  void testActionWithMalformedParamsJson() throws Exception {
    JSONObject params = new JSONObject();
    params.put("asyncProcessId", "test-id");
    params.put("params", "{malformed_json}");
    params.put("state", "SUCCESS");
    MutableBoolean isStopped = new MutableBoolean(false);

    Iterator<LogPersistorIdentifierHook> mockIterator = mock(Iterator.class);
    when(hooks.iterator()).thenReturn(mockIterator);
    when(mockIterator.hasNext()).thenReturn(false);

    try (MockedStatic<OBContext> obContextMock = org.mockito.Mockito.mockStatic(OBContext.class);
         MockedStatic<OBDal> obDalMock = org.mockito.Mockito.mockStatic(OBDal.class)) {
      OBContext obContext = mock(OBContext.class);
      obContextMock.when(OBContext::getOBContext).thenReturn(obContext);
      when(obContext.isInAdministratorMode()).thenReturn(true);

      OBDal obDal = mock(OBDal.class);
      obDalMock.when(OBDal::getInstance).thenReturn(obDal);
      doNothing().when(obDal).save(any());
      doNothing().when(obDal).commitAndClose();
      doNothing().when(obDal).flush();
      OBCriteria<LogHeader> obCriteria = mock(OBCriteria.class);
      when(obDal.createCriteria(LogHeader.class)).thenReturn(obCriteria);
      when(obCriteria.add(any(Criterion.class))).thenReturn(obCriteria);
      when(obCriteria.setMaxResults(anyInt())).thenReturn(obCriteria);
      when(obCriteria.uniqueResult()).thenReturn(null);

      ActionResult result = processor.action(params, isStopped);
      assertNotNull(result);
    }
  }

  /**
   * Tests the action method of LogPersistorProcessor with parameters containing a valid JWT token.
   * <p>
   * This test provides a JSON object with a "token" field containing a fake JWT token and verifies that the action method
   * processes the token correctly, extracting the context and returning a non-null result.
   *
   * @throws Exception
   *     if any error occurs during the test
   */
  @Test
  void testActionWithTokenParams() throws Exception {
    Iterator<LogPersistorIdentifierHook> mockIterator = mock(Iterator.class);
    when(hooks.iterator()).thenReturn(mockIterator);
    when(mockIterator.hasNext()).thenReturn(false);

    String fakeToken = "fake.jwt.token";
    JSONObject paramsJson = new JSONObject();
    paramsJson.put("token", fakeToken);
    JSONObject params = new JSONObject();
    params.put("asyncProcessId", "test-id");
    params.put("params", paramsJson.toString());
    params.put("state", "SUCCESS");
    MutableBoolean isStopped = new MutableBoolean(false);

    com.auth0.jwt.interfaces.DecodedJWT jwt = mock(com.auth0.jwt.interfaces.DecodedJWT.class);
    com.auth0.jwt.interfaces.Claim userClaim = mock(com.auth0.jwt.interfaces.Claim.class);
    com.auth0.jwt.interfaces.Claim roleClaim = mock(com.auth0.jwt.interfaces.Claim.class);
    com.auth0.jwt.interfaces.Claim orgClaim = mock(com.auth0.jwt.interfaces.Claim.class);
    com.auth0.jwt.interfaces.Claim clientClaim = mock(com.auth0.jwt.interfaces.Claim.class);
    when(jwt.getClaim("user")).thenReturn(userClaim);
    when(jwt.getClaim("role")).thenReturn(roleClaim);
    when(jwt.getClaim("organization")).thenReturn(orgClaim);
    when(jwt.getClaim("client")).thenReturn(clientClaim);
    when(userClaim.asString()).thenReturn("100");
    when(roleClaim.asString()).thenReturn("0");
    when(orgClaim.asString()).thenReturn("0");
    when(clientClaim.asString()).thenReturn("0");

    try (MockedStatic<OBContext> obContextMock = org.mockito.Mockito.mockStatic(OBContext.class);
         MockedStatic<OBDal> obDalMock = org.mockito.Mockito.mockStatic(OBDal.class);
         MockedStatic<com.smf.securewebservices.utils.SecureWebServicesUtils> jwtMock = org.mockito.Mockito.mockStatic(
             com.smf.securewebservices.utils.SecureWebServicesUtils.class)) {
      OBContext obContext = mock(OBContext.class);
      obContextMock.when(OBContext::getOBContext).thenReturn(obContext);
      obContextMock.when(() -> OBContext.setOBContext(anyString(), anyString(), anyString(), anyString())).then(
          invocation -> null);
      when(obContext.isInAdministratorMode()).thenReturn(true);

      OBDal obDal = mock(OBDal.class);
      obDalMock.when(OBDal::getInstance).thenReturn(obDal);
      doNothing().when(obDal).save(any());
      doNothing().when(obDal).commitAndClose();
      doNothing().when(obDal).flush();
      OBCriteria<LogHeader> obCriteria = mock(OBCriteria.class);
      when(obDal.createCriteria(LogHeader.class)).thenReturn(obCriteria);
      when(obCriteria.add(any(Criterion.class))).thenReturn(obCriteria);
      when(obCriteria.setMaxResults(anyInt())).thenReturn(obCriteria);
      when(obCriteria.uniqueResult()).thenReturn(null);

      jwtMock.when(() -> com.smf.securewebservices.utils.SecureWebServicesUtils.decodeToken(fakeToken)).thenReturn(jwt);

      ActionResult result = processor.action(params, isStopped);
      assertNotNull(result);
    }
  }

  /**
   * Tests the action method of LogPersistorProcessor with parameters in Debezium format but missing required fields.
   * <p>
   * This test provides a JSON object with an "after" field containing missing required fields and verifies that the action method
   * handles the missing fields gracefully, without throwing exceptions.
   *
   * @throws Exception
   *     if any error occurs during the test
   */
  @Test
  void testActionWithDebeziumParamsMissingFields() throws Exception {
    Iterator<LogPersistorIdentifierHook> mockIterator = mock(Iterator.class);
    when(hooks.iterator()).thenReturn(mockIterator);
    when(mockIterator.hasNext()).thenReturn(false);

    JSONObject after = new JSONObject();
    after.put("assigned_user", ""); // userId vacío
    after.put("ad_client_id", "0");
    after.put("ad_org_id", "0");
    JSONObject debeziumParams = new JSONObject();
    debeziumParams.put("after", after);
    JSONObject params = new JSONObject();
    params.put("asyncProcessId", "test-id");
    params.put("params", debeziumParams.toString());
    params.put("state", "SUCCESS");
    MutableBoolean isStopped = new MutableBoolean(false);

    try (MockedStatic<OBContext> obContextMock = org.mockito.Mockito.mockStatic(OBContext.class);
         MockedStatic<OBDal> obDalMock = org.mockito.Mockito.mockStatic(OBDal.class)) {
      OBContext obContext = mock(OBContext.class);
      obContextMock.when(OBContext::getOBContext).thenReturn(obContext);
      obContextMock.when(() -> OBContext.setOBContext(anyString(), anyString(), anyString(), anyString())).then(
          invocation -> null);
      when(obContext.isInAdministratorMode()).thenReturn(true);

      OBDal obDal = mock(OBDal.class);
      obDalMock.when(OBDal::getInstance).thenReturn(obDal);
      doNothing().when(obDal).save(any());
      doNothing().when(obDal).commitAndClose();
      doNothing().when(obDal).flush();
      OBCriteria<LogHeader> obCriteria = mock(OBCriteria.class);
      when(obDal.createCriteria(LogHeader.class)).thenReturn(obCriteria);
      when(obCriteria.add(any(Criterion.class))).thenReturn(obCriteria);
      when(obCriteria.setMaxResults(anyInt())).thenReturn(obCriteria);
      when(obCriteria.uniqueResult()).thenReturn(null);

      ActionResult result = processor.action(params, isStopped);
      assertNotNull(result);
    }
  }

  /**
   * Tests the action method of LogPersistorProcessor with nested obuiapp_process_id.
   * <p>
   * This test provides a JSON object with an embedded "obuiapp_process_id" and verifies that the action method
   * correctly extracts and processes the nested ID, returning a non-null result.
   *
   * @throws Exception
   *     if any error occurs during the test
   */
  @Test
  void testActionWithNestedObuiappProcessId() throws Exception {
    // Mock hooks.iterator() vacío
    Iterator<LogPersistorIdentifierHook> mockIterator = mock(Iterator.class);
    when(hooks.iterator()).thenReturn(mockIterator);
    when(mockIterator.hasNext()).thenReturn(false);

    JSONObject nestedParams = new JSONObject();
    nestedParams.put("obuiapp_process_id", "nested-process-id");
    JSONObject paramsJson = new JSONObject();
    paramsJson.put("params", nestedParams.toString());
    JSONObject params = new JSONObject();
    params.put("asyncProcessId", "test-id");
    params.put("params", paramsJson.toString());
    params.put("state", "SUCCESS");
    MutableBoolean isStopped = new MutableBoolean(false);

    try (MockedStatic<OBContext> obContextMock = org.mockito.Mockito.mockStatic(OBContext.class);
         MockedStatic<OBDal> obDalMock = org.mockito.Mockito.mockStatic(OBDal.class)) {
      OBContext obContext = mock(OBContext.class);
      obContextMock.when(OBContext::getOBContext).thenReturn(obContext);
      obContextMock.when(() -> OBContext.setOBContext(anyString(), anyString(), anyString(), anyString())).then(
          invocation -> null);
      when(obContext.isInAdministratorMode()).thenReturn(true);

      OBDal obDal = mock(OBDal.class);
      obDalMock.when(OBDal::getInstance).thenReturn(obDal);
      doNothing().when(obDal).save(any());
      doNothing().when(obDal).commitAndClose();
      doNothing().when(obDal).flush();
      OBCriteria<LogHeader> obCriteria = mock(OBCriteria.class);
      when(obDal.createCriteria(LogHeader.class)).thenReturn(obCriteria);
      when(obCriteria.add(any(Criterion.class))).thenReturn(obCriteria);
      when(obCriteria.setMaxResults(anyInt())).thenReturn(obCriteria);
      when(obCriteria.uniqueResult()).thenReturn(null);

      ActionResult result = processor.action(params, isStopped);
      assertNotNull(result);
    }
  }

  /**
   * Tests the action method of LogPersistorProcessor with hooks identifying log records.
   * <p>
   * This test provides a JSON object with hooks that identify log records and verifies that the action method
   * processes the hooks correctly, returning a non-null result.
   *
   * @throws Exception
   *     if any error occurs during the test
   */
  @Test
  void testActionWithHooksIdentifyLogRecord() throws Exception {
    LogPersistorIdentifierHook hookTrue = mock(LogPersistorIdentifierHook.class);
    LogPersistorIdentifierHook hookFalse = mock(LogPersistorIdentifierHook.class);
    when(hookTrue.identifyLogRecord(any(LogHeader.class))).thenReturn(true);
    when(hookFalse.identifyLogRecord(any(LogHeader.class))).thenReturn(false);
    Iterator<LogPersistorIdentifierHook> iterator = mock(Iterator.class);
    when(iterator.hasNext()).thenReturn(true, true, false);
    when(iterator.next()).thenReturn(hookFalse, hookTrue);
    when(hooks.iterator()).thenReturn(iterator);

    JSONObject after = new JSONObject();
    after.put("assigned_user", "100");
    after.put("assigned_role", "0");
    after.put("ad_client_id", "0");
    after.put("ad_org_id", "0");
    JSONObject debeziumParams = new JSONObject();
    debeziumParams.put("after", after);
    JSONObject params = new JSONObject();
    params.put("asyncProcessId", "test-id");
    params.put("params", debeziumParams.toString());
    params.put("state", "SUCCESS");
    MutableBoolean isStopped = new MutableBoolean(false);

    try (MockedStatic<OBContext> obContextMock = org.mockito.Mockito.mockStatic(OBContext.class);
         MockedStatic<OBDal> obDalMock = org.mockito.Mockito.mockStatic(OBDal.class)) {
      OBContext obContext = mock(OBContext.class);
      obContextMock.when(OBContext::getOBContext).thenReturn(obContext);
      obContextMock.when(() -> OBContext.setOBContext(anyString(), anyString(), anyString(), anyString())).then(
          invocation -> null);
      when(obContext.isInAdministratorMode()).thenReturn(true);

      OBDal obDal = mock(OBDal.class);
      obDalMock.when(OBDal::getInstance).thenReturn(obDal);
      doNothing().when(obDal).save(any());
      doNothing().when(obDal).commitAndClose();
      doNothing().when(obDal).flush();
      com.etendoerp.asyncprocess.data.LogHeader logHeaderSpy = spy(new com.etendoerp.asyncprocess.data.LogHeader());
      doReturn(new java.util.ArrayList<>()).when(logHeaderSpy).getETAPLogList();
      doNothing().when(obDal).save(logHeaderSpy);

      com.etendoerp.asyncprocess.data.Log logSpy = spy(new com.etendoerp.asyncprocess.data.Log());
      doNothing().when(obDal).save(logSpy);

      OBCriteria<LogHeader> obCriteria = mock(OBCriteria.class);
      when(obDal.createCriteria(LogHeader.class)).thenReturn(obCriteria);
      when(obCriteria.add(any(Criterion.class))).thenReturn(obCriteria);
      when(obCriteria.setMaxResults(anyInt())).thenReturn(obCriteria);
      when(obCriteria.uniqueResult()).thenReturn(null);

      ActionResult result = processor.action(params, isStopped);
      assertNotNull(result);
    }
  }

  @Test
  void testActionWithNonJsonDescription() throws Exception {
    Iterator<LogPersistorIdentifierHook> mockIterator = mock(Iterator.class);
    when(hooks.iterator()).thenReturn(mockIterator);
    when(mockIterator.hasNext()).thenReturn(false);

    JSONObject params = new JSONObject();
    params.put("asyncProcessId", "test-id");
    params.put("params", "{}");
    params.put("log", "Texto plano no JSON");
    params.put("state", "SUCCESS");
    MutableBoolean isStopped = new MutableBoolean(false);

    try (MockedStatic<OBContext> obContextMock = org.mockito.Mockito.mockStatic(OBContext.class);
         MockedStatic<OBDal> obDalMock = org.mockito.Mockito.mockStatic(OBDal.class)) {
      OBContext obContext = mock(OBContext.class);
      obContextMock.when(OBContext::getOBContext).thenReturn(obContext);
      obContextMock.when(() -> OBContext.setOBContext(anyString(), anyString(), anyString(), anyString())).then(
          invocation -> null);
      when(obContext.isInAdministratorMode()).thenReturn(true);

      OBDal obDal = mock(OBDal.class);
      obDalMock.when(OBDal::getInstance).thenReturn(obDal);
      doNothing().when(obDal).save(any());
      doNothing().when(obDal).commitAndClose();
      doNothing().when(obDal).flush();
      OBCriteria<LogHeader> obCriteria = mock(OBCriteria.class);
      when(obDal.createCriteria(LogHeader.class)).thenReturn(obCriteria);
      when(obCriteria.add(any(Criterion.class))).thenReturn(obCriteria);
      when(obCriteria.setMaxResults(anyInt())).thenReturn(obCriteria);
      when(obCriteria.uniqueResult()).thenReturn(null);

      ActionResult result = processor.action(params, isStopped);
      assertNotNull(result);
    }
  }

  @Test
  void testActionWithMalformedJsonDescription() throws Exception {
    Iterator<LogPersistorIdentifierHook> mockIterator = mock(Iterator.class);
    when(hooks.iterator()).thenReturn(mockIterator);
    when(mockIterator.hasNext()).thenReturn(false);

    JSONObject params = new JSONObject();
    params.put("asyncProcessId", "test-id");
    params.put("params", "{}");
    params.put("log", "{malformed: json, sin comillas}");
    params.put("state", "SUCCESS");
    MutableBoolean isStopped = new MutableBoolean(false);

    try (MockedStatic<OBContext> obContextMock = org.mockito.Mockito.mockStatic(OBContext.class);
         MockedStatic<OBDal> obDalMock = org.mockito.Mockito.mockStatic(OBDal.class)) {
      OBContext obContext = mock(OBContext.class);
      obContextMock.when(OBContext::getOBContext).thenReturn(obContext);
      obContextMock.when(() -> OBContext.setOBContext(anyString(), anyString(), anyString(), anyString())).then(
          invocation -> null);
      when(obContext.isInAdministratorMode()).thenReturn(true);

      OBDal obDal = mock(OBDal.class);
      obDalMock.when(OBDal::getInstance).thenReturn(obDal);
      doNothing().when(obDal).save(any());
      doNothing().when(obDal).commitAndClose();
      doNothing().when(obDal).flush();
      OBCriteria<LogHeader> obCriteria = mock(OBCriteria.class);
      when(obDal.createCriteria(LogHeader.class)).thenReturn(obCriteria);
      when(obCriteria.add(any(Criterion.class))).thenReturn(obCriteria);
      when(obCriteria.setMaxResults(anyInt())).thenReturn(obCriteria);
      when(obCriteria.uniqueResult()).thenReturn(null);

      ActionResult result = processor.action(params, isStopped);
      assertNotNull(result);
    }
  }

  /**
   * Tests the action method with empty parameters string.
   */
  @Test
  void testActionWithEmptyParams() throws Exception {
    Iterator<LogPersistorIdentifierHook> mockIterator = mock(Iterator.class);
    when(hooks.iterator()).thenReturn(mockIterator);
    when(mockIterator.hasNext()).thenReturn(false);

    JSONObject params = new JSONObject();
    params.put("asyncProcessId", "test-id");
    params.put("params", "");
    params.put("state", "SUCCESS");
    MutableBoolean isStopped = new MutableBoolean(false);

    try (MockedStatic<OBContext> obContextMock = org.mockito.Mockito.mockStatic(OBContext.class);
         MockedStatic<OBDal> obDalMock = org.mockito.Mockito.mockStatic(OBDal.class)) {
      OBContext obContext = mock(OBContext.class);
      obContextMock.when(OBContext::getOBContext).thenReturn(obContext);
      when(obContext.isInAdministratorMode()).thenReturn(true);

      OBDal obDal = mock(OBDal.class);
      obDalMock.when(OBDal::getInstance).thenReturn(obDal);
      doNothing().when(obDal).save(any());
      doNothing().when(obDal).commitAndClose();
      doNothing().when(obDal).flush();

      LogHeader logHeaderSpy = spy(new LogHeader());
      doReturn(new java.util.ArrayList<>()).when(logHeaderSpy).getETAPLogList();

      OBCriteria<LogHeader> obCriteria = mock(OBCriteria.class);
      when(obDal.createCriteria(LogHeader.class)).thenReturn(obCriteria);
      when(obCriteria.add(any(Criterion.class))).thenReturn(obCriteria);
      when(obCriteria.setMaxResults(anyInt())).thenReturn(obCriteria);
      when(obCriteria.uniqueResult()).thenReturn(null);

      ActionResult result = processor.action(params, isStopped);
      assertNotNull(result);
    }
  }

  /**
   * Tests the action method with existing LogHeader.
   */
  @Test
  void testActionWithExistingLogHeader() throws Exception {
    Iterator<LogPersistorIdentifierHook> mockIterator = mock(Iterator.class);
    when(hooks.iterator()).thenReturn(mockIterator);
    when(mockIterator.hasNext()).thenReturn(false);

    JSONObject after = new JSONObject();
    after.put("assigned_user", "100");
    after.put("assigned_role", "0");
    after.put("ad_client_id", "0");
    after.put("ad_org_id", "0");
    JSONObject debeziumParams = new JSONObject();
    debeziumParams.put("after", after);
    JSONObject params = new JSONObject();
    params.put("asyncProcessId", "test-id");
    params.put("params", debeziumParams.toString());
    params.put("state", "SUCCESS");
    MutableBoolean isStopped = new MutableBoolean(false);

    LogHeader existingLogHeader = spy(new LogHeader());
    doReturn(new java.util.ArrayList<>()).when(existingLogHeader).getETAPLogList();

    try (MockedStatic<OBContext> obContextMock = org.mockito.Mockito.mockStatic(OBContext.class);
         MockedStatic<OBDal> obDalMock = org.mockito.Mockito.mockStatic(OBDal.class)) {
      OBContext obContext = mock(OBContext.class);
      obContextMock.when(OBContext::getOBContext).thenReturn(obContext);
      obContextMock.when(() -> OBContext.setOBContext(anyString(), anyString(), anyString(), anyString())).then(
          invocation -> null);
      when(obContext.isInAdministratorMode()).thenReturn(true);

      OBDal obDal = mock(OBDal.class);
      obDalMock.when(OBDal::getInstance).thenReturn(obDal);
      doNothing().when(obDal).save(any());
      doNothing().when(obDal).commitAndClose();
      doNothing().when(obDal).flush();

      OBCriteria<LogHeader> obCriteria = mock(OBCriteria.class);
      when(obDal.createCriteria(LogHeader.class)).thenReturn(obCriteria);
      when(obCriteria.add(any(Criterion.class))).thenReturn(obCriteria);
      when(obCriteria.setMaxResults(anyInt())).thenReturn(obCriteria);
      when(obCriteria.uniqueResult()).thenReturn(existingLogHeader);

      ActionResult result = processor.action(params, isStopped);
      assertNotNull(result);
    }
  }

  /**
   * Tests the action method with JWT token that fails to decode.
   */
  @Test
  void testActionWithInvalidJWTToken() throws Exception {
    Iterator<LogPersistorIdentifierHook> mockIterator = mock(Iterator.class);
    when(hooks.iterator()).thenReturn(mockIterator);
    when(mockIterator.hasNext()).thenReturn(false);

    String invalidToken = "invalid.jwt.token";
    JSONObject paramsJson = new JSONObject();
    paramsJson.put("token", invalidToken);
    JSONObject params = new JSONObject();
    params.put("asyncProcessId", "test-id");
    params.put("params", paramsJson.toString());
    params.put("state", "SUCCESS");
    MutableBoolean isStopped = new MutableBoolean(false);

    try (MockedStatic<OBContext> obContextMock = org.mockito.Mockito.mockStatic(OBContext.class);
         MockedStatic<OBDal> obDalMock = org.mockito.Mockito.mockStatic(OBDal.class);
         MockedStatic<com.smf.securewebservices.utils.SecureWebServicesUtils> jwtMock = org.mockito.Mockito.mockStatic(
             com.smf.securewebservices.utils.SecureWebServicesUtils.class)) {
      OBContext obContext = mock(OBContext.class);
      obContextMock.when(OBContext::getOBContext).thenReturn(obContext);
      when(obContext.isInAdministratorMode()).thenReturn(true);

      OBDal obDal = mock(OBDal.class);
      obDalMock.when(OBDal::getInstance).thenReturn(obDal);
      doNothing().when(obDal).save(any());
      doNothing().when(obDal).commitAndClose();
      doNothing().when(obDal).flush();

      LogHeader logHeaderSpy = spy(new LogHeader());
      doReturn(new java.util.ArrayList<>()).when(logHeaderSpy).getETAPLogList();

      OBCriteria<LogHeader> obCriteria = mock(OBCriteria.class);
      when(obDal.createCriteria(LogHeader.class)).thenReturn(obCriteria);
      when(obCriteria.add(any(Criterion.class))).thenReturn(obCriteria);
      when(obCriteria.setMaxResults(anyInt())).thenReturn(obCriteria);
      when(obCriteria.uniqueResult()).thenReturn(null);

      jwtMock.when(() -> com.smf.securewebservices.utils.SecureWebServicesUtils.decodeToken(invalidToken)).thenReturn(null);

      ActionResult result = processor.action(params, isStopped);
      assertNotNull(result);
    }
  }

  /**
   * Tests the action method with Debezium params missing after field.
   */
  @Test
  void testActionWithDebeziumParamsMissingAfter() throws Exception {
    Iterator<LogPersistorIdentifierHook> mockIterator = mock(Iterator.class);
    when(hooks.iterator()).thenReturn(mockIterator);
    when(mockIterator.hasNext()).thenReturn(false);

    JSONObject debeziumParams = new JSONObject();
    // No "after" field
    JSONObject params = new JSONObject();
    params.put("asyncProcessId", "test-id");
    params.put("params", debeziumParams.toString());
    params.put("state", "SUCCESS");
    MutableBoolean isStopped = new MutableBoolean(false);

    try (MockedStatic<OBContext> obContextMock = org.mockito.Mockito.mockStatic(OBContext.class);
         MockedStatic<OBDal> obDalMock = org.mockito.Mockito.mockStatic(OBDal.class)) {
      OBContext obContext = mock(OBContext.class);
      obContextMock.when(OBContext::getOBContext).thenReturn(obContext);
      when(obContext.isInAdministratorMode()).thenReturn(true);

      OBDal obDal = mock(OBDal.class);
      obDalMock.when(OBDal::getInstance).thenReturn(obDal);
      doNothing().when(obDal).save(any());
      doNothing().when(obDal).commitAndClose();
      doNothing().when(obDal).flush();

      LogHeader logHeaderSpy = spy(new LogHeader());
      doReturn(new java.util.ArrayList<>()).when(logHeaderSpy).getETAPLogList();

      OBCriteria<LogHeader> obCriteria = mock(OBCriteria.class);
      when(obDal.createCriteria(LogHeader.class)).thenReturn(obCriteria);
      when(obCriteria.add(any(Criterion.class))).thenReturn(obCriteria);
      when(obCriteria.setMaxResults(anyInt())).thenReturn(obCriteria);
      when(obCriteria.uniqueResult()).thenReturn(null);

      ActionResult result = processor.action(params, isStopped);
      assertNotNull(result);
    }
  }

  /**
   * Tests the action method with log parameter that has empty value.
   */
  @Test
  void testActionWithEmptyLogParameter() throws Exception {
    Iterator<LogPersistorIdentifierHook> mockIterator = mock(Iterator.class);
    when(hooks.iterator()).thenReturn(mockIterator);
    when(mockIterator.hasNext()).thenReturn(false);

    JSONObject params = new JSONObject();
    params.put("asyncProcessId", "test-id");
    params.put("log", "");
    params.put("params", "test-params");
    params.put("state", "SUCCESS");
    MutableBoolean isStopped = new MutableBoolean(false);

    try (MockedStatic<OBContext> obContextMock = org.mockito.Mockito.mockStatic(OBContext.class);
         MockedStatic<OBDal> obDalMock = org.mockito.Mockito.mockStatic(OBDal.class)) {
      OBContext obContext = mock(OBContext.class);
      obContextMock.when(OBContext::getOBContext).thenReturn(obContext);
      when(obContext.isInAdministratorMode()).thenReturn(true);

      OBDal obDal = mock(OBDal.class);
      obDalMock.when(OBDal::getInstance).thenReturn(obDal);
      doNothing().when(obDal).save(any());
      doNothing().when(obDal).commitAndClose();
      doNothing().when(obDal).flush();

      LogHeader logHeaderSpy = spy(new LogHeader());
      doReturn(new java.util.ArrayList<>()).when(logHeaderSpy).getETAPLogList();

      OBCriteria<LogHeader> obCriteria = mock(OBCriteria.class);
      when(obDal.createCriteria(LogHeader.class)).thenReturn(obCriteria);
      when(obCriteria.add(any(Criterion.class))).thenReturn(obCriteria);
      when(obCriteria.setMaxResults(anyInt())).thenReturn(obCriteria);
      when(obCriteria.uniqueResult()).thenReturn(null);

      ActionResult result = processor.action(params, isStopped);
      assertNotNull(result);
    }
  }

  /**
   * Tests the action method with JSON description that needs pretty formatting.
   */
  @Test
  void testActionWithJSONDescription() throws Exception {
    Iterator<LogPersistorIdentifierHook> mockIterator = mock(Iterator.class);
    when(hooks.iterator()).thenReturn(mockIterator);
    when(mockIterator.hasNext()).thenReturn(false);

    String jsonDescription = "{\"key1\":\"value1\",\"key2\":\"value2\"}";
    JSONObject params = new JSONObject();
    params.put("asyncProcessId", "test-id");
    params.put("log", jsonDescription);
    params.put("params", "");
    params.put("state", "SUCCESS");
    MutableBoolean isStopped = new MutableBoolean(false);

    try (MockedStatic<OBContext> obContextMock = org.mockito.Mockito.mockStatic(OBContext.class);
         MockedStatic<OBDal> obDalMock = org.mockito.Mockito.mockStatic(OBDal.class)) {
      OBContext obContext = mock(OBContext.class);
      obContextMock.when(OBContext::getOBContext).thenReturn(obContext);
      when(obContext.isInAdministratorMode()).thenReturn(true);

      OBDal obDal = mock(OBDal.class);
      obDalMock.when(OBDal::getInstance).thenReturn(obDal);
      doNothing().when(obDal).save(any());
      doNothing().when(obDal).commitAndClose();
      doNothing().when(obDal).flush();

      LogHeader logHeaderSpy = spy(new LogHeader());
      doReturn(new java.util.ArrayList<>()).when(logHeaderSpy).getETAPLogList();

      OBCriteria<LogHeader> obCriteria = mock(OBCriteria.class);
      when(obDal.createCriteria(LogHeader.class)).thenReturn(obCriteria);
      when(obCriteria.add(any(Criterion.class))).thenReturn(obCriteria);
      when(obCriteria.setMaxResults(anyInt())).thenReturn(obCriteria);
      when(obCriteria.uniqueResult()).thenReturn(null);

      ActionResult result = processor.action(params, isStopped);
      assertNotNull(result);
    }
  }

  /**
   * Tests the action method with nested JSON description.
   */
  @Test
  void testActionWithNestedJSONDescription() throws Exception {
    Iterator<LogPersistorIdentifierHook> mockIterator = mock(Iterator.class);
    when(hooks.iterator()).thenReturn(mockIterator);
    when(mockIterator.hasNext()).thenReturn(false);

    String nestedJsonDescription = "{\"outer\":\"{\\\"inner\\\":\\\"value\\\"}\"}";
    JSONObject params = new JSONObject();
    params.put("asyncProcessId", "test-id");
    params.put("log", nestedJsonDescription);
    params.put("params", "");
    params.put("state", "SUCCESS");
    MutableBoolean isStopped = new MutableBoolean(false);

    try (MockedStatic<OBContext> obContextMock = org.mockito.Mockito.mockStatic(OBContext.class);
         MockedStatic<OBDal> obDalMock = org.mockito.Mockito.mockStatic(OBDal.class)) {
      OBContext obContext = mock(OBContext.class);
      obContextMock.when(OBContext::getOBContext).thenReturn(obContext);
      when(obContext.isInAdministratorMode()).thenReturn(true);

      OBDal obDal = mock(OBDal.class);
      obDalMock.when(OBDal::getInstance).thenReturn(obDal);
      doNothing().when(obDal).save(any());
      doNothing().when(obDal).commitAndClose();
      doNothing().when(obDal).flush();

      LogHeader logHeaderSpy = spy(new LogHeader());
      doReturn(new java.util.ArrayList<>()).when(logHeaderSpy).getETAPLogList();

      OBCriteria<LogHeader> obCriteria = mock(OBCriteria.class);
      when(obDal.createCriteria(LogHeader.class)).thenReturn(obCriteria);
      when(obCriteria.add(any(Criterion.class))).thenReturn(obCriteria);
      when(obCriteria.setMaxResults(anyInt())).thenReturn(obCriteria);
      when(obCriteria.uniqueResult()).thenReturn(null);

      ActionResult result = processor.action(params, isStopped);
      assertNotNull(result);
    }
  }

  /**
   * Tests the action method with description containing date prefix and JSON.
   */
  @Test
  void testActionWithDatePrefixJSONDescription() throws Exception {
    Iterator<LogPersistorIdentifierHook> mockIterator = mock(Iterator.class);
    when(hooks.iterator()).thenReturn(mockIterator);
    when(mockIterator.hasNext()).thenReturn(false);

    String dateJsonDescription = "2023-10-01 12:00:00: {\"status\":\"completed\",\"result\":\"success\"}";
    JSONObject params = new JSONObject();
    params.put("asyncProcessId", "test-id");
    params.put("log", dateJsonDescription);
    params.put("params", "");
    params.put("state", "SUCCESS");
    MutableBoolean isStopped = new MutableBoolean(false);

    try (MockedStatic<OBContext> obContextMock = org.mockito.Mockito.mockStatic(OBContext.class);
         MockedStatic<OBDal> obDalMock = org.mockito.Mockito.mockStatic(OBDal.class)) {
      OBContext obContext = mock(OBContext.class);
      obContextMock.when(OBContext::getOBContext).thenReturn(obContext);
      when(obContext.isInAdministratorMode()).thenReturn(true);

      OBDal obDal = mock(OBDal.class);
      obDalMock.when(OBDal::getInstance).thenReturn(obDal);
      doNothing().when(obDal).save(any());
      doNothing().when(obDal).commitAndClose();
      doNothing().when(obDal).flush();

      LogHeader logHeaderSpy = spy(new LogHeader());
      doReturn(new java.util.ArrayList<>()).when(logHeaderSpy).getETAPLogList();

      OBCriteria<LogHeader> obCriteria = mock(OBCriteria.class);
      when(obDal.createCriteria(LogHeader.class)).thenReturn(obCriteria);
      when(obCriteria.add(any(Criterion.class))).thenReturn(obCriteria);
      when(obCriteria.setMaxResults(anyInt())).thenReturn(obCriteria);
      when(obCriteria.uniqueResult()).thenReturn(null);

      ActionResult result = processor.action(params, isStopped);
      assertNotNull(result);
    }
  }

  /**
   * Tests the getInputClass method.
   */
  @Test
  void testGetInputClass() {
    Class<?> inputClass = processor.getInputClass();
    assertEquals(JSONObject.class, inputClass);
  }

  /**
   * Tests the action method with invalid JSON in pretty description formatting.
   */
  @Test
  void testActionWithInvalidJSONForPrettyDescription() throws Exception {
    Iterator<LogPersistorIdentifierHook> mockIterator = mock(Iterator.class);
    when(hooks.iterator()).thenReturn(mockIterator);
    when(mockIterator.hasNext()).thenReturn(false);

    String invalidJsonDescription = "{invalid json syntax}";
    JSONObject params = new JSONObject();
    params.put("asyncProcessId", "test-id");
    params.put("log", invalidJsonDescription);
    params.put("params", "");
    params.put("state", "SUCCESS");
    MutableBoolean isStopped = new MutableBoolean(false);

    try (MockedStatic<OBContext> obContextMock = org.mockito.Mockito.mockStatic(OBContext.class);
         MockedStatic<OBDal> obDalMock = org.mockito.Mockito.mockStatic(OBDal.class)) {
      OBContext obContext = mock(OBContext.class);
      obContextMock.when(OBContext::getOBContext).thenReturn(obContext);
      when(obContext.isInAdministratorMode()).thenReturn(true);

      OBDal obDal = mock(OBDal.class);
      obDalMock.when(OBDal::getInstance).thenReturn(obDal);
      doNothing().when(obDal).save(any());
      doNothing().when(obDal).commitAndClose();
      doNothing().when(obDal).flush();

      LogHeader logHeaderSpy = spy(new LogHeader());
      doReturn(new java.util.ArrayList<>()).when(logHeaderSpy).getETAPLogList();

      OBCriteria<LogHeader> obCriteria = mock(OBCriteria.class);
      when(obDal.createCriteria(LogHeader.class)).thenReturn(obCriteria);
      when(obCriteria.add(any(Criterion.class))).thenReturn(obCriteria);
      when(obCriteria.setMaxResults(anyInt())).thenReturn(obCriteria);
      when(obCriteria.uniqueResult()).thenReturn(null);

      ActionResult result = processor.action(params, isStopped);
      assertNotNull(result);
    }
  }

  /**
   * Tests the action method with Debezium params where assigned_user is "null" string.
   */
  @Test
  void testActionWithDebeziumParamsNullStringUser() throws Exception {
    Iterator<LogPersistorIdentifierHook> mockIterator = mock(Iterator.class);
    when(hooks.iterator()).thenReturn(mockIterator);
    when(mockIterator.hasNext()).thenReturn(false);

    JSONObject after = new JSONObject();
    after.put("assigned_user", "null");
    after.put("updatedby", "200");
    after.put("assigned_role", "null");
    after.put("ad_client_id", "0");
    after.put("ad_org_id", "0");
    JSONObject debeziumParams = new JSONObject();
    debeziumParams.put("after", after);
    JSONObject params = new JSONObject();
    params.put("asyncProcessId", "test-id");
    params.put("params", debeziumParams.toString());
    params.put("state", "SUCCESS");
    MutableBoolean isStopped = new MutableBoolean(false);

    try (MockedStatic<OBContext> obContextMock = org.mockito.Mockito.mockStatic(OBContext.class);
         MockedStatic<OBDal> obDalMock = org.mockito.Mockito.mockStatic(OBDal.class)) {
      OBContext obContext = mock(OBContext.class);
      obContextMock.when(OBContext::getOBContext).thenReturn(obContext);
      obContextMock.when(() -> OBContext.setOBContext(anyString(), anyString(), anyString(), anyString())).then(
          invocation -> null);
      when(obContext.isInAdministratorMode()).thenReturn(true);

      OBDal obDal = mock(OBDal.class);
      obDalMock.when(OBDal::getInstance).thenReturn(obDal);
      doNothing().when(obDal).save(any());
      doNothing().when(obDal).commitAndClose();
      doNothing().when(obDal).flush();

      LogHeader logHeaderSpy = spy(new LogHeader());
      doReturn(new java.util.ArrayList<>()).when(logHeaderSpy).getETAPLogList();

      OBCriteria<LogHeader> obCriteria = mock(OBCriteria.class);
      when(obDal.createCriteria(LogHeader.class)).thenReturn(obCriteria);
      when(obCriteria.add(any(Criterion.class))).thenReturn(obCriteria);
      when(obCriteria.setMaxResults(anyInt())).thenReturn(obCriteria);
      when(obCriteria.uniqueResult()).thenReturn(null);

      ActionResult result = processor.action(params, isStopped);
      assertNotNull(result);
    }
  }
}

