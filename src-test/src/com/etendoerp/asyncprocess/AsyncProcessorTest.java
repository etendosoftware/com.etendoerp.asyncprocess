package com.etendoerp.asyncprocess;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.function.Function;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;

import com.smf.jobs.ActionResult;
import com.smf.jobs.Result;

/**
 * Unit tests for the AsyncProcessor class.
 * This class tests the action method and ensures that it behaves correctly
 * with various inputs and conditions.
 */
@ExtendWith(MockitoExtension.class)
class AsyncProcessorTest {

  @Mock
  private Function<JSONObject, ActionResult> mockConsumer;

  @Mock
  private ActionResult mockActionResult;

  @Mock
  private JSONObject mockParameters;

  private TestAsyncProcessor asyncProcessor;
  private MutableBoolean isStopped;

  /**
   * Set up the test environment before each test.
   * Initializes the AsyncProcessor with a mock consumer and a mutable stopped flag.
   */
  @BeforeEach
  void setUp() {
    asyncProcessor = new TestAsyncProcessor(mockConsumer);
    isStopped = new MutableBoolean(false);
  }

  /**
   * Test to ensure that the action method returns a non-null ActionResult
   * when the consumer function is called with valid parameters.
   */
  @Test
  void testActionSuccess() {
    when(mockConsumer.apply(mockParameters)).thenReturn(mockActionResult);
    when(mockActionResult.toString()).thenReturn("Success message");

    try (MockedConstruction<ActionResult> mockedConstruction = mockConstruction(ActionResult.class)) {

      ActionResult result = asyncProcessor.action(mockParameters, isStopped);

      assertNotNull(result);
      verify(mockConsumer, times(1)).apply(mockParameters);

      // Verify that the constructed ActionResult had setMessage and setType called
      assertEquals(1, mockedConstruction.constructed().size());
      ActionResult constructedResult = mockedConstruction.constructed().get(0);
      verify(constructedResult).setMessage("Success message");
      verify(constructedResult).setType(Result.Type.SUCCESS);
    }
  }

  /**
   * Test to ensure that the action method returns a non-null ActionResult
   * when the consumer function is called with valid parameters, and the ActionResult is constructed correctly.
   */
  @Test
  void testActionWithDifferentResult() {
    // Given
    ActionResult customResult = new ActionResult();
    customResult.setMessage("Custom message");
    when(mockConsumer.apply(mockParameters)).thenReturn(customResult);

    try (MockedConstruction<ActionResult> mockedConstruction = mockConstruction(ActionResult.class)) {

      ActionResult result = asyncProcessor.action(mockParameters, isStopped);

      assertNotNull(result);
      verify(mockConsumer, times(1)).apply(mockParameters);

      // Verify that the constructed ActionResult had setMessage and setType called
      assertEquals(1, mockedConstruction.constructed().size());
      ActionResult constructedResult = mockedConstruction.constructed().get(0);
      verify(constructedResult).setMessage(customResult.toString());
      verify(constructedResult).setType(Result.Type.SUCCESS);
    }
  }

  /**
   * Test to ensure that the action method handles the stopped flag correctly.
   * If the stopped flag is true, it should return an ActionResult indicating that the process was stopped.
   */
  @Test
  void testActionWithStoppedFlag() {
    MutableBoolean stoppedFlag = new MutableBoolean(true);
    when(mockConsumer.apply(mockParameters)).thenReturn(mockActionResult);
    when(mockActionResult.toString()).thenReturn("Stopped message");

    try (MockedConstruction<ActionResult> mockedConstruction = mockConstruction(ActionResult.class)) {

      ActionResult result = asyncProcessor.action(mockParameters, stoppedFlag);

      assertNotNull(result);
      verify(mockConsumer, times(1)).apply(mockParameters);

      // Verify that the constructed ActionResult had setMessage and setType called
      assertEquals(1, mockedConstruction.constructed().size());
      ActionResult constructedResult = mockedConstruction.constructed().get(0);
      verify(constructedResult).setMessage("Stopped message");
      verify(constructedResult).setType(Result.Type.SUCCESS);
    }
  }

  /**
   * Test to ensure that the getInputClass method returns the expected input class type.
   * In this case, it should return Map.class as per the implementation.
   */
  @Test
  void testGetInputClass() {
    Class<?> inputClass = asyncProcessor.getInputClass();

    assertEquals(Map.class, inputClass);
  }

  /**
   * Test to ensure that the consumer method returns the expected Function<JSONObject, ActionResult>.
   */
  @Test
  void testConsumerMethod() {
    Function<JSONObject, ActionResult> consumer = asyncProcessor.consumer();

    assertNotNull(consumer);
    assertEquals(mockConsumer, consumer);
  }

  /**
   * Test to ensure that the action method can handle null parameters gracefully.
   * It should return an ActionResult with a specific message indicating null handling.
   */
  @Test
  void testActionWithNullParameters() {
    when(mockConsumer.apply(null)).thenReturn(mockActionResult);
    when(mockActionResult.toString()).thenReturn("Null parameters handled");

    try (MockedConstruction<ActionResult> mockedConstruction = mockConstruction(ActionResult.class)) {

      ActionResult result = asyncProcessor.action(null, isStopped);

      assertNotNull(result);
      verify(mockConsumer, times(1)).apply(null);

      // Verify that the constructed ActionResult had setMessage and setType called
      assertEquals(1, mockedConstruction.constructed().size());
      ActionResult constructedResult = mockedConstruction.constructed().get(0);
      verify(constructedResult).setMessage("Null parameters handled");
      verify(constructedResult).setType(Result.Type.SUCCESS);
    }
  }

  /**
   * Test to ensure that the action method always returns a SUCCESS type ActionResult,
   * regardless of the input result type.
   */
  @Test
  void testActionAlwaysReturnsSuccessType() {
    // Given
    ActionResult errorResult = new ActionResult();
    errorResult.setMessage("Error occurred");
    errorResult.setType(Result.Type.ERROR);
    when(mockConsumer.apply(mockParameters)).thenReturn(errorResult);

    try (MockedConstruction<ActionResult> mockedConstruction = mockConstruction(ActionResult.class)) {

      ActionResult result = asyncProcessor.action(mockParameters, isStopped);

      assertNotNull(result);
      verify(mockConsumer, times(1)).apply(mockParameters);

      // Verify that regardless of input result type, output is always SUCCESS
      assertEquals(1, mockedConstruction.constructed().size());
      ActionResult constructedResult = mockedConstruction.constructed().get(0);
      verify(constructedResult).setMessage(errorResult.toString());
      verify(constructedResult).setType(Result.Type.SUCCESS); // Always SUCCESS
    }
  }

  /**
   * Test to ensure that the action method can handle a complex JSONObject as input.
   * This test verifies that the consumer processes a JSONObject with multiple types of values,
   * and that the resulting ActionResult is constructed correctly.
   *
   * @throws Exception
   *     if there is an error creating or processing the JSONObject
   */
  @Test
  void testActionWithComplexJSONObject() throws Exception {
    JSONObject complexJson = new JSONObject();
    complexJson.put("key1", "value1");
    complexJson.put("key2", 123);
    complexJson.put("key3", true);

    ActionResult complexResult = new ActionResult();
    complexResult.setMessage("Complex JSON processed");
    when(mockConsumer.apply(complexJson)).thenReturn(complexResult);

    try (MockedConstruction<ActionResult> mockedConstruction = mockConstruction(ActionResult.class)) {

      ActionResult result = asyncProcessor.action(complexJson, isStopped);

      assertNotNull(result);
      verify(mockConsumer, times(1)).apply(complexJson);

      assertEquals(1, mockedConstruction.constructed().size());
      ActionResult constructedResult = mockedConstruction.constructed().get(0);
      verify(constructedResult).setMessage(complexResult.toString());
      verify(constructedResult).setType(Result.Type.SUCCESS);
    }
  }

  /**
   * Test to ensure that multiple calls to the action method return distinct ActionResult instances.
   */
  @Test
  void testMultipleActionCalls() {
    when(mockConsumer.apply(any(JSONObject.class))).thenReturn(mockActionResult);
    when(mockActionResult.toString()).thenReturn("Multiple calls");

    try (MockedConstruction<ActionResult> mockedConstruction = mockConstruction(ActionResult.class)) {

      ActionResult result1 = asyncProcessor.action(mockParameters, isStopped);
      ActionResult result2 = asyncProcessor.action(mockParameters, isStopped);
      ActionResult result3 = asyncProcessor.action(mockParameters, isStopped);

      assertNotNull(result1);
      assertNotNull(result2);
      assertNotNull(result3);
      verify(mockConsumer, times(3)).apply(mockParameters);

      assertEquals(3, mockedConstruction.constructed().size());
      for (ActionResult constructedResult : mockedConstruction.constructed()) {
        verify(constructedResult).setMessage("Multiple calls");
        verify(constructedResult).setType(Result.Type.SUCCESS);
      }
    }
  }

  /**
   * A test implementation of AsyncProcessor to allow testing of the abstract methods.
   */
  private static class TestAsyncProcessor extends AsyncProcessor {
    private final Function<JSONObject, ActionResult> testConsumer;

    public TestAsyncProcessor(Function<JSONObject, ActionResult> consumer) {
      this.testConsumer = consumer;
    }

    @Override
    public Function<JSONObject, ActionResult> consumer() {
      return testConsumer;
    }
  }
}
