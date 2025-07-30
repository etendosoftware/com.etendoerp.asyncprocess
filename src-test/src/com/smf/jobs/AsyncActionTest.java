package com.smf.jobs;

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

  @BeforeEach
  public void setUp() throws Exception {
    mocks = MockitoAnnotations.openMocks(this);

    testParams = new JSONObject();
    testParams.put("testKey", "testValue");
    testParams.put("numericKey", 123);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (mocks != null) {
      mocks.close();
    }
  }

  @Test
  public void testRunHappyPath() throws JSONException {
    JSONObject expectedParams = new JSONObject();
    expectedParams.put("key", "value");

    when(mockActionFactory.get()).thenReturn(mockAction);
    when(mockAction.action(eq(expectedParams), any(MutableBoolean.class)))
        .thenReturn(mockActionResult);

    ActionResult result = AsyncAction.run(mockActionFactory, expectedParams);

    assertNotNull(result, "Result should not be null");
    assertEquals(mockActionResult, result, "Should return the mocked result");

    verify(mockActionFactory, times(1)).get();

    ArgumentCaptor<MutableBoolean> booleanCaptor = ArgumentCaptor.forClass(MutableBoolean.class);
    verify(mockAction, times(1)).action(eq(expectedParams), booleanCaptor.capture());

    MutableBoolean capturedBoolean = booleanCaptor.getValue();
    assertNotNull(capturedBoolean, "MutableBoolean should not be null");
    assertFalse(capturedBoolean.booleanValue(), "MutableBoolean should be false initially");
  }

  @Test
  public void testRunWithNullParams() {
    JSONObject nullParams = null;

    when(mockActionFactory.get()).thenReturn(mockAction);
    when(mockAction.action(eq(null), any(MutableBoolean.class)))
        .thenReturn(mockActionResult);

    ActionResult result = AsyncAction.run(mockActionFactory, null);

    assertNotNull(result, "Result should not be null");
    assertEquals(mockActionResult, result, "Should return the mocked result");

    verify(mockActionFactory, times(1)).get();
    verify(mockAction, times(1)).action(eq(nullParams), any(MutableBoolean.class));
  }

  @Test
  public void testRunWithEmptyParams() {
    JSONObject emptyParams = new JSONObject();

    when(mockActionFactory.get()).thenReturn(mockAction);
    when(mockAction.action(eq(emptyParams), any(MutableBoolean.class)))
        .thenReturn(mockActionResult);

    ActionResult result = AsyncAction.run(mockActionFactory, emptyParams);

    assertNotNull(result, "Result should not be null");
    assertEquals(mockActionResult, result, "Should return the mocked result");

    verify(mockActionFactory, times(1)).get();
    verify(mockAction, times(1)).action(eq(emptyParams), any(MutableBoolean.class));
  }

  @Test
  public void testRunActionFactoryThrowsException() {
    when(mockActionFactory.get()).thenThrow(new RuntimeException("Factory error"));

    RuntimeException exception = assertThrows(RuntimeException.class, () -> {
      AsyncAction.run(mockActionFactory, testParams);
    });

    assertEquals("Factory error", exception.getMessage());
  }

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

    assertNotNull(result, "Result should not be null");
    assertEquals(mockActionResult, result, "Should return the mocked result");

    verify(mockAction, times(1)).action(eq(complexParams), any(MutableBoolean.class));
  }

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

  @Test
  public void testConstructorIsPrivate() throws Exception {
    Constructor<AsyncAction> constructor = AsyncAction.class.getDeclaredConstructor();

    assertFalse(constructor.canAccess(null), "Constructor should not be accessible");

    assertThrows(IllegalAccessException.class, constructor::newInstance);
  }

  @Test
  public void testConstructorMakeAccessibleAndInstantiate() throws Exception {
    Constructor<AsyncAction> constructor = AsyncAction.class.getDeclaredConstructor();
    constructor.setAccessible(true);

    AsyncAction instance = constructor.newInstance();

    assertNotNull(instance, "Instance should be created when constructor is made accessible");
  }
}
