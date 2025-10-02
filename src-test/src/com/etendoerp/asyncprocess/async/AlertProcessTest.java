package com.etendoerp.asyncprocess.async;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
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
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.service.OBDal;
import org.openbravo.model.ad.access.Role;
import org.openbravo.model.ad.alert.Alert;
import org.openbravo.model.ad.alert.AlertRule;

import com.smf.jobs.ActionResult;
import com.smf.jobs.Result;

/**
 * Unit tests for {@link AlertProcess} class.
 * Tests cover successful alert creation, JSON parsing exceptions, and various error scenarios.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AlertProcessTest {

  private static final String TEST_DOCUMENT_NO = "DOC-001";
  private static final String ALERT_RULE_ID = "57CC65EA1D9C47E9BA20E09771004802";
  private static final String USER_ID = "100";
  private static final String ROLE_ID = "42D0EEB1C66F497A90DD526DC597E6F0";
  private static final String CLIENT_ID = "23C59575B9CF467C9620760EB255B389";
  private static final String ORG_ID = "0";
  public static final String ALERT_CREATED = "Alert created";
  public static final String AFTER = "after";
  public static final String DOCUMENTNO = "documentno";

  @Mock
  private OBContext mockOBContext;

  @Mock
  private OBDal mockOBDal;

  @Mock
  private Role mockRole;

  @Mock
  private AlertRule mockAlertRule;

  @Mock
  private org.openbravo.base.model.Entity mockEntity;

  @Mock
  private org.openbravo.base.model.Entity mockRoleEntity;

  private MockedStatic<OBContext> mockedOBContext;
  private MockedStatic<OBDal> mockedOBDal;

  private AlertProcess alertProcess;
  private AutoCloseable mocks;
  private MutableBoolean isStopped;

  @BeforeEach
  void setUp() {
    mocks = MockitoAnnotations.openMocks(this);
    alertProcess = new AlertProcess();
    isStopped = new MutableBoolean(false);

    // Setup static mocks
    mockedOBContext = mockStatic(OBContext.class);
    mockedOBDal = mockStatic(OBDal.class);

    // Configure basic mock behavior
    mockedOBContext.when(() -> OBContext.setOBContext(USER_ID, ROLE_ID, CLIENT_ID, ORG_ID))
        .thenAnswer(invocation -> null);
    mockedOBContext.when(OBContext::getOBContext).thenReturn(mockOBContext);
    mockedOBDal.when(OBDal::getInstance).thenReturn(mockOBDal);

    when(mockOBContext.getRole()).thenReturn(mockRole);
    when(mockOBDal.get(AlertRule.class, ALERT_RULE_ID)).thenReturn(mockAlertRule);

    // Configure AlertRule mock to have a valid entity to avoid NullPointerException
    when(mockAlertRule.getEntity()).thenReturn(mockEntity);
    when(mockEntity.getName()).thenReturn("ADAlertRule");

    // Configure Role mock to have a valid entity to avoid NullPointerException
    when(mockRole.getEntity()).thenReturn(mockRoleEntity);
    when(mockRoleEntity.getName()).thenReturn("ADRole");
  }

  @AfterEach
  void tearDown() throws Exception {
    if (mockedOBContext != null) {
      mockedOBContext.close();
    }
    if (mockedOBDal != null) {
      mockedOBDal.close();
    }
    if (mocks != null) {
      mocks.close();
    }
  }

  /**
   * Tests successful alert creation with valid parameters.
   */
  @Test
  void testActionSuccessfulAlertCreation() throws JSONException {
    JSONObject parameters = createValidParameters();
    ActionResult result = alertProcess.action(parameters, isStopped);
    assertSuccessResult(result);
    mockedOBContext.verify(() -> OBContext.setOBContext(USER_ID, ROLE_ID, CLIENT_ID, ORG_ID), times(1));
  }

  /**
   * Tests error handling when 'after' object is missing from parameters.
   */
  @Test
  void testActionMissingAfterObject() {
    JSONObject parameters = new JSONObject();
    ActionResult result = alertProcess.action(parameters, isStopped);
    assertErrorResult(result);
  }

  /**
   * Tests error handling when 'documentno' field is missing from 'after' object.
   */
  @Test
  void testActionMissingDocumentNo() throws JSONException {
    JSONObject parameters = new JSONObject();
    parameters.put(AFTER, new JSONObject());
    ActionResult result = alertProcess.action(parameters, isStopped);
    assertErrorResult(result);
  }

  /**
   * Tests error handling when 'after' object is null.
   */
  @Test
  void testActionNullAfterObject() throws JSONException {
    JSONObject parameters = new JSONObject();
    parameters.put(AFTER, JSONObject.NULL);
    ActionResult result = alertProcess.action(parameters, isStopped);
    assertErrorResult(result);
  }

  /**
   * Tests successful alert creation with empty document number.
   */
  @Test
  void testActionEmptyDocumentNo() throws JSONException {
    ActionResult result = alertProcess.action(createParametersWithDocumentNo(""), isStopped);
    assertSuccessResult(result);
  }

  /**
   * Tests alert creation with special characters in document number.
   */
  @Test
  void testActionSpecialCharactersInDocumentNo() throws JSONException {
    ActionResult result = alertProcess.action(createParametersWithDocumentNo("DOC-001/2023#$%"), isStopped);
    assertSuccessResult(result);
  }

  /**
   * Tests behavior when isStopped is true.
   */
  @Test
  void testActionWithStoppedFlag() throws JSONException {
    isStopped.setTrue();
    ActionResult result = alertProcess.action(createValidParameters(), isStopped);
    assertSuccessResult(result);
  }

  /**
   * Tests the getInputClass method.
   */
  @Test
  void testGetInputClass() {
    assertEquals(JSONObject.class, alertProcess.getInputClass());
  }

  /**
   * Helper method to create valid parameters for testing successful scenarios.
   */
  private JSONObject createValidParameters() throws JSONException {
    return createParametersWithDocumentNo(TEST_DOCUMENT_NO);
  }

  /**
   * Helper method to create parameters with a specific document number.
   */
  private JSONObject createParametersWithDocumentNo(String documentNo) throws JSONException {
    JSONObject parameters = new JSONObject();
    JSONObject after = new JSONObject();
    after.put(DOCUMENTNO, documentNo);
    parameters.put(AFTER, after);
    return parameters;
  }

  /**
   * Asserts that the action result is successful and verifies database operations.
   */
  private void assertSuccessResult(ActionResult result) {
    assertNotNull(result);
    assertEquals(Result.Type.SUCCESS, result.getType());
    assertEquals(ALERT_CREATED, result.getMessage());
    verify(mockOBDal, times(1)).save(any(Alert.class));
    verify(mockOBDal, times(1)).flush();
    verify(mockOBDal, times(1)).commitAndClose();
  }

  /**
   * Asserts that the action result is an error and verifies no database write operations occurred.
   */
  private void assertErrorResult(ActionResult result) {
    assertNotNull(result);
    assertEquals(Result.Type.ERROR, result.getType());
    assertNotNull(result.getMessage());
    verify(mockOBDal, never()).save(any(Alert.class));
    verify(mockOBDal, never()).flush();
    verify(mockOBDal, never()).commitAndClose();
  }
}
