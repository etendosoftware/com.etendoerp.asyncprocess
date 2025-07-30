package com.etendoerp.asyncprocess.async;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import com.smf.jobs.model.Job;

/**
 * Test class for ResultCollector, which collects results from asynchronous jobs.
 * It verifies the behavior of the action method under various conditions.
 */
class ResultCollectorTest extends OBBaseTest {

  private ResultCollector resultCollector;
  private AutoCloseable mocks;

  @Mock
  private OBDal mockOBDal;

  private MockedStatic<OBContext> mockedOBContext;
  private MockedStatic<OBDal> mockedOBDal;

  /**
   * Sets up the test environment before each test execution.
   * Initializes mocks for Mockito and static classes (OBContext, OBDal).
   * Ensures that OBDal.getInstance() returns the mocked OBDal instance.
   *
   * @throws Exception
   *     if an error occurs during mock initialization.
   */
  @BeforeEach
  public void setUp() throws Exception {
    mocks = MockitoAnnotations.openMocks(this);
    resultCollector = new ResultCollector();

    mockedOBContext = mockStatic(OBContext.class);
    mockedOBDal = mockStatic(OBDal.class);

    mockedOBDal.when(OBDal::getInstance).thenReturn(mockOBDal);
  }

  /**
   * Releases the resources of static mocks and Mockito after each test.
   *
   * @throws Exception
   *     if an error occurs while closing the mocks.
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
   * Tests the action method when the 'jobs_job_id' parameter is missing.
   * Expects an ActionResult of type ERROR and a specific error message.
   *
   * @throws JSONException
   *     if there is an error constructing the JSON parameters
   */
  @Test
  void testActionMissingJobsJobIdParameter() throws JSONException {
    JSONObject parameters = new JSONObject();
    parameters.put("message", "Test message");
    parameters.put("client_id", "testClientId");
    parameters.put("org_id", "testOrgId");
    MutableBoolean isStopped = new MutableBoolean(false);

    ActionResult result = resultCollector.action(parameters, isStopped);

    assertNotNull(result);
    assertEquals(Result.Type.ERROR, result.getType());
    assertEquals("JSONObject[\"jobs_job_id\"] not found.", result.getMessage());

    verify(mockOBDal, never()).save(any());
    verify(mockOBDal, never()).flush();
    verify(mockOBDal, never()).commitAndClose();
  }

  /**
   * Tests the action method when the 'message' parameter is missing.
   * Expects an ActionResult of type ERROR and a specific error message.
   *
   * @throws JSONException
   *     if there is an error constructing the JSON parameters
   */
  @Test
  void testActionMissingMessageParameter() throws JSONException {
    JSONObject parameters = new JSONObject();
    parameters.put("jobs_job_id", "testJobId");
    parameters.put("client_id", "testClientId");
    parameters.put("org_id", "testOrgId");
    MutableBoolean isStopped = new MutableBoolean(false);

    ActionResult result = resultCollector.action(parameters, isStopped);

    assertNotNull(result);
    assertEquals(Result.Type.ERROR, result.getType());
    assertEquals("JSONObject[\"message\"] not found.", result.getMessage());

    verify(mockOBDal, never()).save(any());
    verify(mockOBDal, never()).flush();
    verify(mockOBDal, never()).commitAndClose();
  }

  /**
   * Tests the action method when the 'client_id' parameter is missing.
   * Expects an ActionResult of type ERROR and a specific error message.
   *
   * @throws JSONException
   *     if there is an error constructing the JSON parameters
   */
  @Test
  void testActionMissingClientIdParameter() throws JSONException {
    JSONObject parameters = new JSONObject();
    parameters.put("jobs_job_id", "testJobId");
    parameters.put("message", "Test message");
    parameters.put("org_id", "testOrgId");
    MutableBoolean isStopped = new MutableBoolean(false);

    ActionResult result = resultCollector.action(parameters, isStopped);

    assertNotNull(result);
    assertEquals(Result.Type.ERROR, result.getType());
    assertEquals("JSONObject[\"client_id\"] not found.", result.getMessage());
  }

  /**
   * Tests the action method when the 'org_id' parameter is missing.
   * Expects an ActionResult of type ERROR and a specific error message.
   *
   * @throws JSONException
   *     if there is an error constructing the JSON parameters
   */
  @Test
  void testActionMissingOrgIdParameter() throws JSONException {
    JSONObject parameters = new JSONObject();
    parameters.put("jobs_job_id", "testJobId");
    parameters.put("message", "Test message");
    parameters.put("client_id", "testClientId");
    MutableBoolean isStopped = new MutableBoolean(false);

    ActionResult result = resultCollector.action(parameters, isStopped);

    assertNotNull(result);
    assertEquals(Result.Type.ERROR, result.getType());
    assertEquals("JSONObject[\"org_id\"] not found.", result.getMessage());
  }

  /**
   * Test the action method with a valid job ID and parameters.
   * It verifies that the job is fetched correctly and the result is saved.
   */
  /**
   * Tests the action method with null parameters.
   * Expects a NullPointerException to be thrown.
   */
  @Test
  void testActionWithNullParameters() {
    JSONObject parameters = null;
    MutableBoolean isStopped = new MutableBoolean(false);

    assertThrows(NullPointerException.class, () -> {
      resultCollector.action(parameters, isStopped);
    });
  }

  /**
   * Test the action method with a valid job ID and parameters.
   * It verifies that the job is fetched correctly and the result is saved.
   */
  /**
   * Tests the getInputClass method to ensure it returns the Job class type.
   */
  @Test
  void testGetInputClass() {
    Class<Job> inputClass = resultCollector.getInputClass();

    assertEquals(Job.class, inputClass);
  }

  /**
   * Tests the action method with an empty job ID.
   * Expects a successful ActionResult with a message "Collected" even if the Job reference is null.
   *
   * @throws JSONException
   *     if there is an error constructing the JSON parameters
   */
  @Test
  void testActionWithEmptyJobId() throws JSONException {
    JSONObject parameters = new JSONObject();
    parameters.put("jobs_job_id", "");
    parameters.put("message", "Test message");
    parameters.put("client_id", "testClientId");
    parameters.put("org_id", "testOrgId");
    MutableBoolean isStopped = new MutableBoolean(false);

    when(mockOBDal.get(eq(Job.class), eq(""))).thenReturn(null);

    // WHEN
    ActionResult result = resultCollector.action(parameters, isStopped);

    // THEN
    // Empty job ID creates a JobResult with null Job reference, which is allowed
    assertNotNull(result);
    assertEquals(Result.Type.SUCCESS, result.getType());
    assertEquals("Collected", result.getMessage());
  }

  /**
   * Tests the action method when a JSONException is thrown during parameter access.
   * Simulates a scenario where JSON parsing fails and expects the error message to be propagated.
   *
   * @throws Exception
   *     if there is an error during test setup or execution
   */
  @Test
  void testActionJSONExceptionInRealScenario() throws Exception {
    // GIVEN - Test with malformed JSON that causes real JSONException
    setTestUserContext();

    // Create JSON that will cause exception when trying to get a parameter
    JSONObject parameters = new JSONObject() {
      @Override
      public String getString(String key) throws JSONException {
        if ("jobs_job_id".equals(key)) {
          // Simulate a scenario where the parameter exists but getting it fails
          throw new JSONException("Simulated JSON parsing error");
        }
        return super.getString(key);
      }
    };
    parameters.put("jobs_job_id", "testJobId");
    parameters.put("message", "Test message");
    parameters.put("client_id", TEST_CLIENT_ID);
    parameters.put("org_id", TEST_ORG_ID);
    MutableBoolean isStopped = new MutableBoolean(false);

    ActionResult result = resultCollector.action(parameters, isStopped);

    assertNotNull(result);
    assertEquals(Result.Type.ERROR, result.getType());
    assertEquals("Simulated JSON parsing error", result.getMessage());
  }

  /**
   * Tests the action method with an invalid job ID that does not exist in the database.
   * Expects a successful ActionResult with a message "Collected".
   *
   * @throws Exception
   *     if there is an error during test setup or execution
   */
  @Test
  void testActionWithInvalidJobId() throws Exception {
    setTestUserContext();

    JSONObject parameters = new JSONObject();
    parameters.put("jobs_job_id", "INVALID_JOB_ID_THAT_DOES_NOT_EXIST");
    parameters.put("message", "Test message");
    parameters.put("client_id", TEST_CLIENT_ID);
    parameters.put("org_id", TEST_ORG_ID);
    MutableBoolean isStopped = new MutableBoolean(false);

    ActionResult result = resultCollector.action(parameters, isStopped);

    assertNotNull(result);
    assertEquals(Result.Type.SUCCESS, result.getType());
    assertEquals("Collected", result.getMessage());
  }

}
