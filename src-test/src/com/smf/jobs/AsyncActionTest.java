package com.smf.jobs;

import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.ASSERT_RESULT_NOT_NULL;
import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.ASSERT_SHOULD_RETURN_MOCKED_RESULT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.util.function.Supplier;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for AsyncAction class
 */
@ExtendWith(MockitoExtension.class)
public class AsyncActionTest {

  @Mock
  private Supplier<Action> mockActionFactory;

  @Mock
  private Action mockAction;

  @Mock
  private ActionResult mockActionResult;

  private JSONObject testParams;
  private AutoCloseable mocks;

  /**
   * Sets up the test environment before each test execution.
   * Initializes mock objects and test parameters.
   *
   * @throws Exception
   *     if an error occurs during mock initialization or JSON object creation
   */
  @BeforeEach
  public void setUp() throws Exception {
    mocks = MockitoAnnotations.openMocks(this);

    testParams = new JSONObject();
    testParams.put("testKey", "testValue");
    testParams.put("numericKey", 123);
  }

  /**
   * Cleans up resources after each test execution.
   * Closes any open mocks to prevent resource leaks.
   *
   * @throws Exception
   *     if an error occurs while closing mocks
   */
  @AfterEach
  public void tearDown() throws Exception {
    if (mocks != null) {
      mocks.close();
    }
  }

  /**
   * Tests the happy path of the AsyncAction.run method.
   * Verifies that the method returns the expected ActionResult when provided with valid parameters.
   *
   * @throws JSONException
   *     if there is an error creating or manipulating JSON objects
   */
  @Test
  public void testRunHappyPath() throws JSONException {
    JSONObject expectedParams = new JSONObject();
    expectedParams.put("key", "value");

    when(mockActionFactory.get()).thenReturn(mockAction);
    when(mockAction.action(eq(expectedParams), any(MutableBoolean.class)))
        .thenReturn(mockActionResult);

    ActionResult result = AsyncAction.run(mockActionFactory, expectedParams);

    assertNotNull(result, ASSERT_RESULT_NOT_NULL);
    assertEquals(mockActionResult, result, ASSERT_SHOULD_RETURN_MOCKED_RESULT);

    verify(mockActionFactory, times(1)).get();

    ArgumentCaptor<MutableBoolean> booleanCaptor = ArgumentCaptor.forClass(MutableBoolean.class);
    verify(mockAction, times(1)).action(eq(expectedParams), booleanCaptor.capture());

    MutableBoolean capturedBoolean = booleanCaptor.getValue();
    assertNotNull(capturedBoolean, "MutableBoolean should not be null");
    assertFalse(capturedBoolean.booleanValue(), "MutableBoolean should be false initially");
  }

  /**
   * Tests the AsyncAction.run method with null parameters.
   * Verifies that the method handles null JSON parameters correctly and returns the expected ActionResult.
   */
  @Test
  public void testRunWithNullParams() {
    JSONObject nullParams = null;

    when(mockActionFactory.get()).thenReturn(mockAction);
    when(mockAction.action(eq(null), any(MutableBoolean.class)))
        .thenReturn(mockActionResult);

    ActionResult result = AsyncAction.run(mockActionFactory, null);

    assertNotNull(result, ASSERT_RESULT_NOT_NULL);
    assertEquals(mockActionResult, result, ASSERT_SHOULD_RETURN_MOCKED_RESULT);

    verify(mockActionFactory, times(1)).get();
    verify(mockAction, times(1)).action(eq(nullParams), any(MutableBoolean.class));
  }

  /**
   * Tests the AsyncAction.run method with an empty JSON object as parameters.
   * Verifies that the method can handle cases where no parameters are provided.
   */
  @Test
  public void testRunWithEmptyParams() {
    JSONObject emptyParams = new JSONObject();

    when(mockActionFactory.get()).thenReturn(mockAction);
    when(mockAction.action(eq(emptyParams), any(MutableBoolean.class)))
        .thenReturn(mockActionResult);

    ActionResult result = AsyncAction.run(mockActionFactory, emptyParams);

    assertNotNull(result, ASSERT_RESULT_NOT_NULL);
    assertEquals(mockActionResult, result, ASSERT_SHOULD_RETURN_MOCKED_RESULT);

    verify(mockActionFactory, times(1)).get();
    verify(mockAction, times(1)).action(eq(emptyParams), any(MutableBoolean.class));
  }

  /**
   * Tests that the AsyncAction.run method throws an exception when the action factory throws an exception.
   * Verifies that the exception is propagated correctly.
   */
  @Test
  public void testRunActionFactoryThrowsException() {
    when(mockActionFactory.get()).thenThrow(new RuntimeException("Factory error"));

    RuntimeException exception = assertThrows(RuntimeException.class,
        () -> AsyncAction.run(mockActionFactory, testParams));

    assertEquals("Factory error", exception.getMessage());
  }

  /**
   * Tests that multiple calls to AsyncAction.run create new Action instances each time.
   * Verifies that each call returns the correct ActionResult for each Action instance.
   */
  @Test
  public void testRunMultipleCallsCreateNewActions() {
    Action mockAction2 = mock(Action.class);
    ActionResult mockActionResult2 = mock(ActionResult.class);

    when(mockActionFactory.get())
        .thenReturn(mockAction)
        .thenReturn(mockAction2);
    when(mockAction.action(eq(testParams), any(MutableBoolean.class)))
        .thenReturn(mockActionResult);
    when(mockAction2.action(eq(testParams), any(MutableBoolean.class)))
        .thenReturn(mockActionResult2);

    ActionResult result1 = AsyncAction.run(mockActionFactory, testParams);
    ActionResult result2 = AsyncAction.run(mockActionFactory, testParams);

    assertNotNull(result1, "First result should not be null");
    assertNotNull(result2, "Second result should not be null");
    assertEquals(mockActionResult, result1, "First result should match first mock");
    assertEquals(mockActionResult2, result2, "Second result should match second mock");

    verify(mockActionFactory, times(2)).get();

    verify(mockAction, times(1)).action(eq(testParams), any(MutableBoolean.class));
    verify(mockAction2, times(1)).action(eq(testParams), any(MutableBoolean.class));
  }

  /**
   * Tests that the MutableBoolean passed to the action method is always initialized as false.
   * Ensures that the AsyncAction class does not modify the MutableBoolean state unexpectedly.
   */
  @Test
  public void testRunVerifyMutableBooleanIsAlwaysFalse() {
    ArgumentCaptor<MutableBoolean> booleanCaptor = ArgumentCaptor.forClass(MutableBoolean.class);

    when(mockActionFactory.get()).thenReturn(mockAction);
    when(mockAction.action(eq(testParams), any(MutableBoolean.class)))
        .thenReturn(mockActionResult);

    AsyncAction.run(mockActionFactory, testParams);
    AsyncAction.run(mockActionFactory, testParams);

    verify(mockAction, times(2)).action(eq(testParams), booleanCaptor.capture());

    for (MutableBoolean capturedBoolean : booleanCaptor.getAllValues()) {
      assertFalse(capturedBoolean.booleanValue(), "MutableBoolean should always be initialized as false");
    }
  }

  /**
   * Tests the AsyncAction.run method with complex JSON parameters.
   * Verifies that the method can handle nested and various types of JSON values.
   *
   * @throws JSONException
   *     if there is an error creating or manipulating JSON objects
   */
  @Test
  public void testRunWithComplexJSONParams() throws JSONException {
    // Given
    JSONObject complexParams = new JSONObject();
    complexParams.put("stringParam", "testString");
    complexParams.put("intParam", 42);
    complexParams.put("booleanParam", true);
    complexParams.put("doubleParam", 3.14);

    JSONObject nestedObject = new JSONObject();
    nestedObject.put("nestedKey", "nestedValue");
    complexParams.put("nestedParam", nestedObject);

    when(mockActionFactory.get()).thenReturn(mockAction);
    when(mockAction.action(eq(complexParams), any(MutableBoolean.class)))
        .thenReturn(mockActionResult);

    ActionResult result = AsyncAction.run(mockActionFactory, complexParams);

    assertNotNull(result, ASSERT_RESULT_NOT_NULL);
    assertEquals(mockActionResult, result, ASSERT_SHOULD_RETURN_MOCKED_RESULT);

    verify(mockAction, times(1)).action(eq(complexParams), any(MutableBoolean.class));
  }

  /**
   * Tests that the AsyncAction.run method returns null when the action method returns null.
   * Verifies that the method handles null ActionResult correctly.
   */
  @Test
  public void testRunActionReturnsNull() {
    when(mockActionFactory.get()).thenReturn(mockAction);
    when(mockAction.action(eq(testParams), any(MutableBoolean.class)))
        .thenReturn(null);

    ActionResult result = AsyncAction.run(mockActionFactory, testParams);

    assertNull(result, "Should return null when action returns null");
    verify(mockActionFactory, times(1)).get();
    verify(mockAction, times(1)).action(eq(testParams), any(MutableBoolean.class));
  }

  /**
   * Tests that the AsyncAction constructor is private and cannot be accessed directly.
   * Verifies that an IllegalAccessException is thrown when attempting to instantiate via reflection.
   *
   * @throws Exception
   *     if reflection fails or the constructor cannot be accessed
   */
  @Test
  public void testConstructorIsPrivate() throws Exception {
    Constructor<AsyncAction> constructor = AsyncAction.class.getDeclaredConstructor();

    assertFalse(constructor.canAccess(null), "Constructor should not be accessible");

    assertThrows(IllegalAccessException.class, constructor::newInstance);
  }

  /**
   * Tests that the private constructor of AsyncAction can be made accessible and an instance can be created via reflection.
   * Useful for testing private constructors in a controlled manner.
   *
   * @throws Exception
   *     if reflection fails or the constructor cannot be accessed
   */
  @Test
  public void testConstructorMakeAccessibleAndInstantiate() throws Exception {
    Constructor<AsyncAction> constructor = AsyncAction.class.getDeclaredConstructor();
    constructor.setAccessible(true);

    AsyncAction instance = constructor.newInstance();

    assertNotNull(instance, "Instance should be created when constructor is made accessible");
  }
}
