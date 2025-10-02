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

  @BeforeEach
  void setUp() {
    mocks = MockitoAnnotations.openMocks(this);
    alertProcess = new AlertProcess();

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
   * Verifies that an alert is created with correct properties and database operations are performed.
   */
  @Test
  void testActionSuccessfulAlertCreation() throws JSONException {
    // Arrange
    JSONObject parameters = createValidParameters();
    MutableBoolean isStopped = new MutableBoolean(false);

    // Act
    ActionResult result = alertProcess.action(parameters, isStopped);

    // Assert
    assertNotNull(result);
    assertEquals(Result.Type.SUCCESS, result.getType());
    assertEquals("Alert created", result.getMessage());

    // Verify OBContext was set correctly
    mockedOBContext.verify(() -> OBContext.setOBContext(USER_ID, ROLE_ID, CLIENT_ID, ORG_ID), times(1));

    // Verify database operations
    verify(mockOBDal, times(1)).get(AlertRule.class, ALERT_RULE_ID);
    verify(mockOBDal, times(1)).save(any(Alert.class));
    verify(mockOBDal, times(1)).flush();
    verify(mockOBDal, times(1)).commitAndClose();
  }

  /**
   * Tests error handling when 'after' object is missing from parameters.
   * Should catch JSONException and return error result.
   */
  @Test
  void testActionMissingAfterObject() {
    // Arrange - Create parameters WITHOUT 'after' object to simulate missing data
    JSONObject parameters = new JSONObject();
    // Intentionally NOT adding 'after' object to simulate missing data
    MutableBoolean isStopped = new MutableBoolean(false);

    // Act
    ActionResult result = alertProcess.action(parameters, isStopped);

    // Assert
    assertNotNull(result);
    assertEquals(Result.Type.ERROR, result.getType());
    assertNotNull(result.getMessage());

    // Verify that database operations were not performed
    verify(mockOBDal, never()).save(any(Alert.class));
    verify(mockOBDal, never()).flush();
    verify(mockOBDal, never()).commitAndClose();
  }

  /**
   * Tests error handling when 'documentno' field is missing from 'after' object.
   * Should catch JSONException and return error result.
   */
  @Test
  void testActionMissingDocumentNo() throws JSONException {
    // Arrange
    JSONObject parameters = new JSONObject();
    JSONObject after = new JSONObject();
    // Not adding 'documentno' to simulate missing field
    parameters.put("after", after);
    MutableBoolean isStopped = new MutableBoolean(false);

    // Act
    ActionResult result = alertProcess.action(parameters, isStopped);

    // Assert
    assertNotNull(result);
    assertEquals(Result.Type.ERROR, result.getType());
    assertNotNull(result.getMessage());

    // Verify that database operations were not performed
    verify(mockOBDal, never()).save(any(Alert.class));
    verify(mockOBDal, never()).flush();
    verify(mockOBDal, never()).commitAndClose();
  }

  /**
   * Tests error handling when 'after' object is null.
   * Should catch JSONException and return error result.
   */
  @Test
  void testActionNullAfterObject() throws JSONException {
    // Arrange
    JSONObject parameters = new JSONObject();
    parameters.put("after", JSONObject.NULL);
    MutableBoolean isStopped = new MutableBoolean(false);

    // Act
    ActionResult result = alertProcess.action(parameters, isStopped);

    // Assert
    assertNotNull(result);
    assertEquals(Result.Type.ERROR, result.getType());
    assertNotNull(result.getMessage());
  }

  /**
   * Tests the getInputClass method.
   * Verifies that it returns JSONObject.class as expected.
   */
  @Test
  void testGetInputClass() {
    // Act
    Class<?> inputClass = alertProcess.getInputClass();

    // Assert
    assertEquals(JSONObject.class, inputClass);
  }

  /**
   * Tests successful alert creation with empty document number.
   * Verifies that empty strings are handled correctly.
   */
  @Test
  void testActionEmptyDocumentNo() throws JSONException {
    // Arrange
    JSONObject parameters = new JSONObject();
    JSONObject after = new JSONObject();
    after.put("documentno", ""); // Empty document number
    parameters.put("after", after);
    MutableBoolean isStopped = new MutableBoolean(false);

    // Act
    ActionResult result = alertProcess.action(parameters, isStopped);

    // Assert
    assertNotNull(result);
    assertEquals(Result.Type.SUCCESS, result.getType());
    assertEquals("Alert created", result.getMessage());

    // Verify database operations were performed
    verify(mockOBDal, times(1)).save(any(Alert.class));
    verify(mockOBDal, times(1)).flush();
    verify(mockOBDal, times(1)).commitAndClose();
  }

  /**
   * Tests alert creation with special characters in document number.
   * Verifies that special characters are handled correctly.
   */
  @Test
  void testActionSpecialCharactersInDocumentNo() throws JSONException {
    // Arrange
    JSONObject parameters = new JSONObject();
    JSONObject after = new JSONObject();
    after.put("documentno", "DOC-001/2023#$%"); // Special characters
    parameters.put("after", after);
    MutableBoolean isStopped = new MutableBoolean(false);

    // Act
    ActionResult result = alertProcess.action(parameters, isStopped);

    // Assert
    assertNotNull(result);
    assertEquals(Result.Type.SUCCESS, result.getType());
    assertEquals("Alert created", result.getMessage());

    // Verify database operations were performed
    verify(mockOBDal, times(1)).save(any(Alert.class));
    verify(mockOBDal, times(1)).flush();
    verify(mockOBDal, times(1)).commitAndClose();
  }

  /**
   * Tests behavior when isStopped is true.
   * Process should still execute normally as isStopped is not checked in implementation.
   */
  @Test
  void testActionWithStoppedFlag() throws JSONException {
    // Arrange
    JSONObject parameters = createValidParameters();
    MutableBoolean isStopped = new MutableBoolean(true);

    // Act
    ActionResult result = alertProcess.action(parameters, isStopped);

    // Assert - Process should still succeed as isStopped is not checked
    assertNotNull(result);
    assertEquals(Result.Type.SUCCESS, result.getType());
    assertEquals("Alert created", result.getMessage());
  }

  /**
   * Helper method to create valid parameters for testing successful scenarios.
   */
  private JSONObject createValidParameters() throws JSONException {
    JSONObject parameters = new JSONObject();
    JSONObject after = new JSONObject();
    after.put("documentno", TEST_DOCUMENT_NO);
    parameters.put("after", after);
    return parameters;
  }
}
