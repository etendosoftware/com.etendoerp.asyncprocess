package com.etendoerp.asyncprocess.action;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;

import javax.enterprise.inject.Instance;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.hibernate.criterion.Criterion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.service.OBCriteria;
import org.openbravo.dal.service.OBDal;

import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.etendoerp.asyncprocess.data.LogHeader;
import com.etendoerp.asyncprocess.hooks.LogPersistorIdentifierHook;
import com.smf.jobs.ActionResult;
import com.smf.securewebservices.utils.SecureWebServicesUtils;

/**
 * Unit tests for the {@link LogPersistorProcessor} class.
 * This test class uses Mockito and JUnit 5 to mock dependencies and verify the behavior of the LogPersistorProcessor.
 */
class LogPersistorProcessorTest {

  public static final String ASSIGNED_USER = "assigned_user";
  public static final String ASSIGNED_ROLE = "assigned_role";
  public static final String AD_CLIENT_ID = "ad_client_id";
  public static final String AD_ORG_ID = "ad_org_id";
  public static final String ASYNC_PROCESS_ID = "asyncProcessId";
  public static final String PARAMS = "params";
  public static final String STATE = "state";
  public static final String TEST_ID = "test-id";
  public static final String AFTER = "after";
  public static final String SUCCESS = "SUCCESS";
  public static final String LOG = "log";

  @Mock
  private Instance<LogPersistorIdentifierHook> hooks;

  @InjectMocks
  private LogPersistorProcessor processor;

  private MockedStatic<OBContext> obContextMock;
  private MockedStatic<OBDal> obDalMock;
  private MockedStatic<SecureWebServicesUtils> jwtMock;

  private OBCriteria<LogHeader> obCriteria;
  private MutableBoolean isStopped;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    processor = new LogPersistorProcessor();
    isStopped = new MutableBoolean(false);

    // Inject the mock into the private field using reflection
    Field hooksField = LogPersistorProcessor.class.getDeclaredField("hooks");
    hooksField.setAccessible(true);
    hooksField.set(processor, hooks);

    // Mock static classes
    obContextMock = Mockito.mockStatic(OBContext.class);
    obDalMock = Mockito.mockStatic(OBDal.class);
    jwtMock = Mockito.mockStatic(SecureWebServicesUtils.class);

    // Common mock setup
    OBContext obContext = mock(OBContext.class);
    OBDal obDal = mock(OBDal.class);
    obCriteria = mock(OBCriteria.class);
    Iterator<LogPersistorIdentifierHook> mockIterator = mock(Iterator.class);

    obContextMock.when(OBContext::getOBContext).thenReturn(obContext);
    obContextMock.when(() -> OBContext.setOBContext(anyString(), anyString(), anyString(), anyString()))
        .then(invocation -> null);
    when(obContext.isInAdministratorMode()).thenReturn(true);

    obDalMock.when(OBDal::getInstance).thenReturn(obDal);
    doNothing().when(obDal).save(any());
    doNothing().when(obDal).commitAndClose();
    doNothing().when(obDal).flush();

    when(obDal.createCriteria(LogHeader.class)).thenReturn(obCriteria);
    when(obCriteria.add(any(Criterion.class))).thenReturn(obCriteria);
    when(obCriteria.setMaxResults(anyInt())).thenReturn(obCriteria);
    when(obCriteria.uniqueResult()).thenReturn(null); // Default case

    when(hooks.iterator()).thenReturn(mockIterator);
    when(mockIterator.hasNext()).thenReturn(false);
  }

  @AfterEach
  void tearDown() {
    obContextMock.close();
    obDalMock.close();
    jwtMock.close();
  }

  /**
   * Helper to create the base JSON structure for parameters.
   */
  private JSONObject createBaseParams() throws JSONException {
    JSONObject params = new JSONObject();
    params.put(ASYNC_PROCESS_ID, TEST_ID);
    params.put(STATE, SUCCESS);
    return params;
  }

  /**
   * Helper to create a Debezium-style payload string.
   */
  private String createDebeziumPayload(JSONObject after) throws JSONException {
    return new JSONObject().put(AFTER, after).toString();
  }

  @Test
  void testActionWithMinimalParams() throws Exception {
    JSONObject after = new JSONObject();
    after.put(ASSIGNED_USER, "100");
    after.put(ASSIGNED_ROLE, "0");
    after.put(AD_CLIENT_ID, "0");
    after.put(AD_ORG_ID, "0");

    JSONObject params = createBaseParams();
    params.put(PARAMS, createDebeziumPayload(after));

    assertNotNull(processor.action(params, isStopped));
  }

  @Test
  void testActionWithMalformedParamsJson() throws Exception {
    JSONObject params = createBaseParams();
    params.put(PARAMS, "{malformed_json}");
    assertNotNull(processor.action(params, isStopped));
  }

  @Test
  void testActionWithTokenParams() throws Exception {
    String fakeToken = "fake.jwt.token";
    JSONObject paramsJson = new JSONObject().put("token", fakeToken);
    JSONObject params = createBaseParams();
    params.put(PARAMS, paramsJson.toString());

    DecodedJWT jwt = mock(DecodedJWT.class);
    when(jwt.getClaim(anyString())).thenAnswer(inv -> {
      Claim claim = mock(Claim.class);
      when(claim.asString()).thenReturn(inv.getArgument(0).equals("user") ? "100" : "0");
      return claim;
    });

    jwtMock.when(() -> SecureWebServicesUtils.decodeToken(fakeToken)).thenReturn(jwt);

    assertNotNull(processor.action(params, isStopped));
  }

  @Test
  void testActionWithDebeziumParamsMissingFields() throws Exception {
    JSONObject after = new JSONObject();
    after.put(ASSIGNED_USER, "");
    after.put(AD_CLIENT_ID, "0");
    after.put(AD_ORG_ID, "0");

    JSONObject params = createBaseParams();
    params.put(PARAMS, createDebeziumPayload(after));

    assertNotNull(processor.action(params, isStopped));
  }

  @Test
  void testActionWithNestedObuiappProcessId() throws Exception {
    JSONObject nestedParams = new JSONObject().put("obuiapp_process_id", "nested-process-id");
    JSONObject params = createBaseParams();
    params.put(PARAMS, new JSONObject().put(PARAMS, nestedParams.toString()).toString());
    assertNotNull(processor.action(params, isStopped));
  }

  @Test
  void testActionWithHooksIdentifyLogRecord() throws Exception {
    LogPersistorIdentifierHook hookTrue = mock(LogPersistorIdentifierHook.class);
    when(hookTrue.identifyLogRecord(any(LogHeader.class))).thenReturn(true);
    Iterator<LogPersistorIdentifierHook> iterator = mock(Iterator.class);
    when(iterator.hasNext()).thenReturn(true, false);
    when(iterator.next()).thenReturn(hookTrue);
    when(hooks.iterator()).thenReturn(iterator);

    JSONObject after = new JSONObject();
    after.put(ASSIGNED_USER, "100");
    after.put(ASSIGNED_ROLE, "0");
    after.put(AD_CLIENT_ID, "0");
    after.put(AD_ORG_ID, "0");

    JSONObject params = createBaseParams();
    params.put(PARAMS, createDebeziumPayload(after));

    assertNotNull(processor.action(params, isStopped));
  }

  @Test
  void testActionWithNonJsonDescription() throws Exception {
    JSONObject params = createBaseParams();
    params.put(PARAMS, "{}");
    params.put(LOG, "Plain text, not JSON");
    assertNotNull(processor.action(params, isStopped));
  }

  @Test
  void testActionWithMalformedJsonDescription() throws Exception {
    JSONObject params = createBaseParams();
    params.put(PARAMS, "{}");
    params.put(LOG, "{malformed: json, no quotes}");
    assertNotNull(processor.action(params, isStopped));
  }

  @Test
  void testActionWithEmptyParams() throws Exception {
    JSONObject params = createBaseParams();
    params.put(PARAMS, "");
    assertNotNull(processor.action(params, isStopped));
  }

  @Test
  void testActionWithExistingLogHeader() throws Exception {
    LogHeader existingLogHeader = spy(new LogHeader());
    doReturn(new ArrayList<>()).when(existingLogHeader).getETAPLogList();
    when(obCriteria.uniqueResult()).thenReturn(existingLogHeader);

    JSONObject after = new JSONObject();
    after.put(ASSIGNED_USER, "100");
    after.put(ASSIGNED_ROLE, "0");
    after.put(AD_CLIENT_ID, "0");
    after.put(AD_ORG_ID, "0");

    JSONObject params = createBaseParams();
    params.put(PARAMS, createDebeziumPayload(after));

    assertNotNull(processor.action(params, isStopped));
  }

  @Test
  void testActionWithInvalidJWTToken() throws Exception {
    String invalidToken = "invalid.jwt.token";
    JSONObject paramsJson = new JSONObject().put("token", invalidToken);
    JSONObject params = createBaseParams();
    params.put(PARAMS, paramsJson.toString());
    jwtMock.when(() -> SecureWebServicesUtils.decodeToken(invalidToken)).thenReturn(null);
    assertNotNull(processor.action(params, isStopped));
  }

  @Test
  void testActionWithDebeziumParamsMissingAfter() throws Exception {
    JSONObject params = createBaseParams();
    params.put(PARAMS, new JSONObject().toString());
    assertNotNull(processor.action(params, isStopped));
  }

  @Test
  void testActionWithEmptyLogParameter() throws Exception {
    JSONObject params = createBaseParams();
    params.put(LOG, "");
    params.put(PARAMS, "test-params");
    assertNotNull(processor.action(params, isStopped));
  }

  @Test
  void testActionWithJSONDescription() throws Exception {
    JSONObject params = createBaseParams();
    params.put(LOG, "{\"key1\":\"value1\",\"key2\":\"value2\"}");
    params.put(PARAMS, "");
    assertNotNull(processor.action(params, isStopped));
  }

  @Test
  void testActionWithNestedJSONDescription() throws Exception {
    JSONObject params = createBaseParams();
    params.put(LOG, "{\"outer\":\"{\\\"inner\\\":\\\"value\\\"}\"}");
    params.put(PARAMS, "");
    assertNotNull(processor.action(params, isStopped));
  }

  @Test
  void testActionWithDatePrefixJSONDescription() throws Exception {
    JSONObject params = createBaseParams();
    params.put(LOG, "2023-10-01 12:00:00: {\"status\":\"completed\",\"result\":\"success\"}");
    params.put(PARAMS, "");
    assertNotNull(processor.action(params, isStopped));
  }

  @Test
  void testActionWithInvalidJSONForPrettyDescription() throws Exception {
    JSONObject params = createBaseParams();
    params.put(LOG, "{invalid json syntax}");
    params.put(PARAMS, "");
    assertNotNull(processor.action(params, isStopped));
  }

  @Test
  void testActionWithDebeziumParamsNullStringUser() throws Exception {
    JSONObject after = new JSONObject();
    after.put(ASSIGNED_USER, "null");
    after.put("updatedby", "200");
    after.put(ASSIGNED_ROLE, "null");
    after.put(AD_CLIENT_ID, "0");
    after.put(AD_ORG_ID, "0");

    JSONObject params = createBaseParams();
    params.put(PARAMS, createDebeziumPayload(after));

    assertNotNull(processor.action(params, isStopped));
  }

  @Test
  void testGetInputClass() {
    assertEquals(JSONObject.class, processor.getInputClass());
  }
}

