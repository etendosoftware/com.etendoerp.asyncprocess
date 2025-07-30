package com.etendoerp.asyncprocess.async;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.service.OBDal;
import org.openbravo.test.base.OBBaseTest;

import com.smf.jobs.ActionResult;
import com.smf.jobs.Result;

/**
 * Unit tests for the AlertProcess class, verifying alert creation and error handling.
 * Tests cover scenarios with missing or null parameters, missing "after" object, missing "documentno",
 * and correct input class type. Uses Mockito for mocking static methods and dependencies.
 */
class AlertProcessTest extends OBBaseTest {

  private AlertProcess alertProcess;
  private AutoCloseable mocks;

  @Mock
  private OBDal mockOBDal;

  @Mock
  private OBContext mockOBContext;

  private MockedStatic<OBContext> mockedOBContext;
  private MockedStatic<OBDal> mockedOBDal;

  /**
   * Initializes mocks and static method stubs before each test.
   *
   * @throws Exception
   *     if mock initialization fails
   */
  @BeforeEach
  public void setUp() throws Exception {
    mocks = MockitoAnnotations.openMocks(this);
    alertProcess = new AlertProcess();

    mockedOBContext = mockStatic(OBContext.class);
    mockedOBDal = mockStatic(OBDal.class);

    mockedOBDal.when(OBDal::getInstance).thenReturn(mockOBDal);
    mockedOBContext.when(OBContext::getOBContext).thenReturn(mockOBContext);
  }

  /**
   * Closes mocks and static method stubs after each test.
   *
   * @throws Exception
   *     if closing mocks fails
   */
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
   * Test the action method with a valid after object and a valid document number.
   * It should create an alert and return a successful result.
   */
  @Test
  void testActionMissingAfterObject() {
    JSONObject parameters = new JSONObject();
    MutableBoolean isStopped = new MutableBoolean(false);

    ActionResult result = alertProcess.action(parameters, isStopped);

    assertNotNull(result);
    assertEquals(Result.Type.ERROR, result.getType());
    assertEquals("JSONObject[\"after\"] not found.", result.getMessage());

    verify(mockOBDal, never()).save(any());
    verify(mockOBDal, never()).flush();
    verify(mockOBDal, never()).commitAndClose();
  }

  /**
   * Tests the action method when the "documentno" field is missing in the "after" object.
   * Expects an error result and verifies that no database operations are performed.
   *
   * @throws JSONException
   *     if there is an error constructing the JSON objects
   */
  @Test
  void testActionMissingDocumentNo() throws JSONException {
    JSONObject afterObject = new JSONObject();

    JSONObject parameters = new JSONObject();
    parameters.put("after", afterObject);
    MutableBoolean isStopped = new MutableBoolean(false);

    ActionResult result = alertProcess.action(parameters, isStopped);

    assertNotNull(result);
    assertEquals(Result.Type.ERROR, result.getType());
    assertEquals("JSONObject[\"documentno\"] not found.", result.getMessage());

    verify(mockOBDal, never()).save(any());
    verify(mockOBDal, never()).flush();
    verify(mockOBDal, never()).commitAndClose();
  }

  /**
   * Test the action method with a valid after object and a valid document number.
   * It should create an alert and return a successful result.
   */
  @Test
  void testActionWithNullParameters() {
    JSONObject parameters = null;
    MutableBoolean isStopped = new MutableBoolean(false);

    assertThrows(NullPointerException.class, () -> alertProcess.action(parameters, isStopped));
  }

  /**
   * Test the action method with a valid after object and a valid document number.
   * It should create an alert and return a successful result.
   */
  @Test
  void testGetInputClass() {
    Class<?> inputClass = alertProcess.getInputClass();

    assertEquals(JSONObject.class, inputClass);
  }

  /**
   * Tests the action method when the "after" object is explicitly set to null in the parameters.
   * Expects an error result.
   *
   * @throws JSONException
   *     if there is an error constructing the JSON objects
   */
  @Test
  void testActionWithNullAfterObject() throws JSONException {
    JSONObject parameters = new JSONObject();
    parameters.put("after", JSONObject.NULL);
    MutableBoolean isStopped = new MutableBoolean(false);

    ActionResult result = alertProcess.action(parameters, isStopped);

    assertNotNull(result);
    assertEquals(Result.Type.ERROR, result.getType());
  }

}
