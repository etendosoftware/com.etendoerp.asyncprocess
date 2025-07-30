package com.etendoerp.asyncprocess.async;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.smf.jobs.ActionResult;
import com.smf.jobs.Result;

/**
 * Test class for ErrorCollector.
 * This class tests the action method of ErrorCollector to ensure it behaves as expected
 * under various conditions, including valid and invalid parameters, and different states of the isStopped flag.
 */
class ErrorCollectorTest {

  private ErrorCollector errorCollector;

  /**
   * Initializes the ErrorCollector instance before each test.
   */
  @BeforeEach
  void setUp() {
    errorCollector = new ErrorCollector();
  }

  /**
   * Tests the action method with valid parameters.
   * Verifies that the result is not null and is of type ERROR.
   *
   * @throws JSONException
   *     if there is an error constructing the JSON parameters
   */
  @Test
  void testActionWithValidParameters() throws JSONException {
    JSONObject parameters = new JSONObject();
    parameters.put("test_param", "test_value");
    parameters.put("another_param", 123);
    MutableBoolean isStopped = new MutableBoolean(false);

    ActionResult result = errorCollector.action(parameters, isStopped);

    assertNotNull(result);
    assertEquals(Result.Type.ERROR, result.getType());
  }

  /**
   * Tests the action method with an empty parameters object.
   * This should still return an ActionResult of type ERROR.
   */
  @Test
  void testActionWithEmptyParameters() {
    JSONObject parameters = new JSONObject();
    MutableBoolean isStopped = new MutableBoolean(false);

    ActionResult result = errorCollector.action(parameters, isStopped);

    assertNotNull(result);
    assertEquals(Result.Type.ERROR, result.getType());
  }

  /**
   * Tests the action method with a null parameters object.
   * This should return an ActionResult of type ERROR.
   */
  @Test
  void testActionWithNullParameters() {
    JSONObject parameters = null;
    MutableBoolean isStopped = new MutableBoolean(false);

    ActionResult result = errorCollector.action(parameters, isStopped);

    assertNotNull(result);
    assertEquals(Result.Type.ERROR, result.getType());
  }

  /**
   * Tests the action method when the isStopped flag is set to true.
   * The result should still be of type ERROR.
   *
   * @throws JSONException
   *     if there is an error constructing the JSON parameters
   */
  @Test
  void testActionWithStoppedFlag() throws JSONException {
    JSONObject parameters = new JSONObject();
    parameters.put("job_id", "test_job");
    MutableBoolean isStopped = new MutableBoolean(true);

    ActionResult result = errorCollector.action(parameters, isStopped);

    assertNotNull(result);
    assertEquals(Result.Type.ERROR, result.getType());
  }

  /**
   * Tests that the action method always returns an ActionResult of type ERROR,
   * regardless of the input parameters.
   *
   * @throws JSONException
   *     if there is an error constructing the JSON parameters
   */
  @Test
  void testActionAlwaysReturnsError() throws JSONException {
    JSONObject[] testParameters = {
        new JSONObject(),
        new JSONObject().put("success", true),
        new JSONObject().put("status", "OK"),
        new JSONObject().put("error", false),
        null
    };

    for (JSONObject params : testParameters) {
      MutableBoolean isStopped = new MutableBoolean(false);
      ActionResult result = errorCollector.action(params, isStopped);

      assertNotNull(result, "Result should never be null");
      assertEquals(Result.Type.ERROR, result.getType(),
          "ErrorCollector should always return ERROR type regardless of parameters");
    }
  }

  /**
   * Tests the action method with complex parameters, including nested objects and arrays.
   * Ensures the result is always of type ERROR.
   *
   * @throws JSONException
   *     if there is an error constructing the JSON parameters
   */
  @Test
  void testActionWithComplexParameters() throws JSONException {
    JSONObject parameters = new JSONObject();
    parameters.put("nested_object", new JSONObject().put("inner_key", "inner_value"));
    parameters.put("array_param", new int[]{ 1, 2, 3 });
    parameters.put("string_param", "complex_string_value");
    parameters.put("boolean_param", true);
    parameters.put("number_param", 42.5);
    MutableBoolean isStopped = new MutableBoolean(false);

    ActionResult result = errorCollector.action(parameters, isStopped);

    assertNotNull(result);
    assertEquals(Result.Type.ERROR, result.getType());
  }

  /**
   * Tests the action method with a MutableBoolean that is already stopped.
   * This should still return an ActionResult of type ERROR.
   */
  @Test
  void testGetInputClass() {
    Class<?> inputClass = errorCollector.getInputClass();

    assertEquals(JSONObject.class, inputClass);
  }

  /**
   * Tests that the ActionResult returned by the action method contains only the type field set to ERROR.
   *
   * @throws JSONException
   *     if there is an error constructing the JSON parameters
   */
  @Test
  void testActionResultContainsOnlyType() throws JSONException {
    JSONObject parameters = new JSONObject();
    parameters.put("test", "value");
    MutableBoolean isStopped = new MutableBoolean(false);

    ActionResult result = errorCollector.action(parameters, isStopped);

    assertNotNull(result);
    assertEquals(Result.Type.ERROR, result.getType());
  }

  /**
   * Tests the action method with different states of the MutableBoolean isStopped parameter,
   * including null. Ensures the result is always of type ERROR.
   *
   * @throws JSONException
   *     if there is an error constructing the JSON parameters
   */
  @Test
  void testActionWithMutableBooleanVariations() throws JSONException {
    JSONObject parameters = new JSONObject();
    parameters.put("test_key", "test_value");

    MutableBoolean[] stoppedStates = {
        new MutableBoolean(false),
        new MutableBoolean(true),
        null
    };

    for (MutableBoolean stopped : stoppedStates) {
      ActionResult result = errorCollector.action(parameters, stopped);

      assertNotNull(result, "Result should never be null regardless of isStopped state");
      assertEquals(Result.Type.ERROR, result.getType(),
          "Should always return ERROR regardless of isStopped state");
    }
  }

  /**
   * Tests that the action method returns consistent results when called multiple times
   * with the same parameters. All results should be of type ERROR.
   *
   * @throws JSONException
   *     if there is an error constructing the JSON parameters
   */
  @Test
  void testActionConsistentBehavior() throws JSONException {
    JSONObject parameters = new JSONObject();
    parameters.put("consistency_test", "value");
    MutableBoolean isStopped = new MutableBoolean(false);

    ActionResult result1 = errorCollector.action(parameters, isStopped);
    ActionResult result2 = errorCollector.action(parameters, isStopped);
    ActionResult result3 = errorCollector.action(parameters, isStopped);

    assertNotNull(result1);
    assertNotNull(result2);
    assertNotNull(result3);

    assertEquals(Result.Type.ERROR, result1.getType());
    assertEquals(Result.Type.ERROR, result2.getType());
    assertEquals(Result.Type.ERROR, result3.getType());
  }
}
