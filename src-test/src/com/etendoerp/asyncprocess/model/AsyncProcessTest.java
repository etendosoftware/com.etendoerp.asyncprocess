package com.etendoerp.asyncprocess.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Date;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for the AsyncProcess class.
 * This class tests the functionality of AsyncProcess, including its constructor,
 * getters and setters, and the process method.
 * It uses Mockito to mock dependencies and JUnit 5 for testing.
 */
@ExtendWith(MockitoExtension.class)
class AsyncProcessTest {

  private AsyncProcess asyncProcess;

  @Mock
  private AsyncProcessExecution mockExecution1;

  @Mock
  private AsyncProcessExecution mockExecution2;

  /**
   * Set up method to initialize the AsyncProcess instance before each test.
   * This ensures that each test starts with a fresh instance of AsyncProcess.
   */
  @BeforeEach
  void setUp() {
    asyncProcess = new AsyncProcess();
  }

  /**
   * Test to ensure that the constructor initializes the AsyncProcess with default values.
   * This test checks that the state is set to WAITING, executions is initialized as a TreeSet,
   * and no executions are present initially.
   */
  @Test
  void testConstructorInitializesDefaults() {
    // When creating new AsyncProcess
    AsyncProcess newProcess = new AsyncProcess();

    // Then defaults are set correctly
    assertEquals(AsyncProcessState.WAITING, newProcess.getState());
    assertNotNull(newProcess.getExecutions());
    assertTrue(newProcess.getExecutions().isEmpty());
    assertInstanceOf(TreeSet.class, newProcess.getExecutions());
  }

  /**
   * Test to ensure that the ID of AsyncProcess can be set and retrieved correctly.
   * This test checks that the ID is set to a specific string and can be retrieved as such.
   */
  @Test
  void testSetAndGetId() {
    String testId = "test-id-123";

    asyncProcess.setId(testId);

    assertEquals(testId, asyncProcess.getId());
  }

  /**
   * Test to ensure that the last update date of AsyncProcess can be set and retrieved correctly.
   * This test checks that the last update date is set to a specific date and can be retrieved as such.
   */
  @Test
  void testSetAndGetLastUpdate() {
    Date testDate = new Date();

    asyncProcess.setLastUpdate(testDate);

    assertEquals(testDate, asyncProcess.getLastUpdate());
  }

  /**
   * Test to ensure that the description of AsyncProcess can be set and retrieved correctly.
   * This test checks that the description is set to a specific string and can be retrieved as such.
   */
  @Test
  void testSetAndGetDescription() {
    String testDescription = "Test process description";

    asyncProcess.setDescription(testDescription);

    assertEquals(testDescription, asyncProcess.getDescription());
  }

  /**
   * Test to ensure that the state of AsyncProcess can be set and retrieved correctly.
   * This test checks that the state is set to STARTED and can be retrieved as such.
   */
  @Test
  void testSetAndGetState() {
    AsyncProcessState testState = AsyncProcessState.STARTED;

    asyncProcess.setState(testState);

    assertEquals(testState, asyncProcess.getState());
  }

  /**
   * Test to ensure that the executions set is initialized as a TreeSet.
   * This test checks that the executions set is not null and is an instance of TreeSet.
   */
  @Test
  void testSetAndGetExecutions() {
    SortedSet<AsyncProcessExecution> testExecutions = new TreeSet<>();

    asyncProcess.setExecutions(testExecutions);

    assertEquals(testExecutions, asyncProcess.getExecutions());
  }

  /**
   * Test to ensure that the process method updates the AsyncProcess instance
   * when a single execution is processed.
   * This test checks that the ID, last update time, state, and executions are updated correctly.
   */
  @Test
  void testProcessSingleExecution() {
    String processId = "process-123";
    Date executionTime = new Date();
    AsyncProcessState executionState = AsyncProcessState.DONE;

    when(mockExecution1.getAsyncProcessId()).thenReturn(processId);
    when(mockExecution1.getTime()).thenReturn(executionTime);
    when(mockExecution1.getState()).thenReturn(executionState);

    AsyncProcess result = asyncProcess.process(mockExecution1);

    assertEquals(asyncProcess, result);
    assertEquals(processId, asyncProcess.getId());
    assertEquals(executionTime, asyncProcess.getLastUpdate());
    assertEquals(executionState, asyncProcess.getState());
    assertEquals(1, asyncProcess.getExecutions().size());
    assertTrue(asyncProcess.getExecutions().contains(mockExecution1));
  }

  /**
   * Test to ensure that the process method can handle multiple executions
   * and updates the AsyncProcess instance accordingly.
   * This test checks that the ID, last update time, state, and executions are updated correctly.
   */
  @Test
  void testProcessMultipleExecutions() {
    String processId1 = "process-123";
    String processId2 = "process-456";
    Date executionTime1 = new Date(System.currentTimeMillis() - 1000);
    Date executionTime2 = new Date();
    AsyncProcessState state1 = AsyncProcessState.STARTED;
    AsyncProcessState state2 = AsyncProcessState.DONE;

    when(mockExecution1.getAsyncProcessId()).thenReturn(processId1);
    when(mockExecution1.getTime()).thenReturn(executionTime1);
    when(mockExecution1.getState()).thenReturn(state1);

    when(mockExecution2.getAsyncProcessId()).thenReturn(processId2);
    when(mockExecution2.getTime()).thenReturn(executionTime2);
    when(mockExecution2.getState()).thenReturn(state2);

    asyncProcess.process(mockExecution1);
    AsyncProcess result = asyncProcess.process(mockExecution2);

    assertEquals(asyncProcess, result);
    assertEquals(processId2, asyncProcess.getId());
    assertEquals(executionTime2, asyncProcess.getLastUpdate());
    assertEquals(state2, asyncProcess.getState());
    assertEquals(2, asyncProcess.getExecutions().size());
    assertTrue(asyncProcess.getExecutions().contains(mockExecution1));
    assertTrue(asyncProcess.getExecutions().contains(mockExecution2));
  }

  /**
   * Test to ensure that the process method updates all fields of AsyncProcess
   * when a new execution is processed.
   * This test checks that the ID, last update time, state, and executions are updated correctly.
   */
  @Test
  void testProcessUpdatesAllFields() {
    String initialId = "initial-id";
    Date initialDate = new Date(System.currentTimeMillis() - 5000);
    AsyncProcessState initialState = AsyncProcessState.ERROR;

    asyncProcess.setId(initialId);
    asyncProcess.setLastUpdate(initialDate);
    asyncProcess.setState(initialState);

    String newProcessId = "new-process-123";
    Date newExecutionTime = new Date();
    AsyncProcessState newExecutionState = AsyncProcessState.DONE;

    when(mockExecution1.getAsyncProcessId()).thenReturn(newProcessId);
    when(mockExecution1.getTime()).thenReturn(newExecutionTime);
    when(mockExecution1.getState()).thenReturn(newExecutionState);

    asyncProcess.process(mockExecution1);

    assertEquals(newProcessId, asyncProcess.getId());
    assertEquals(newExecutionTime, asyncProcess.getLastUpdate());
    assertEquals(newExecutionState, asyncProcess.getState());
    assertEquals(1, asyncProcess.getExecutions().size());
  }

  /**
   * Test to ensure that the process method can handle null values gracefully.
   * This test checks that the AsyncProcess instance remains unchanged when null values are passed.
   */
  @Test
  void testProcessWithNullValues() {
    when(mockExecution1.getAsyncProcessId()).thenReturn(null);
    when(mockExecution1.getTime()).thenReturn(null);
    when(mockExecution1.getState()).thenReturn(null);

    AsyncProcess result = asyncProcess.process(mockExecution1);

    assertEquals(asyncProcess, result);
    assertNull(asyncProcess.getId());
    assertNull(asyncProcess.getLastUpdate());
    assertNull(asyncProcess.getState());
    assertEquals(1, asyncProcess.getExecutions().size());
    assertTrue(asyncProcess.getExecutions().contains(mockExecution1));
  }

  /**
   * Test to ensure that the executions set is a TreeSet, which maintains order.
   * This test also checks that the set is sorted based on the natural ordering of AsyncProcessExecution.
   */
  @Test
  void testExecutionsSetIsSorted() {
    SortedSet<AsyncProcessExecution> executions = asyncProcess.getExecutions();

    asyncProcess.process(mockExecution1);
    asyncProcess.process(mockExecution2);

    assertInstanceOf(TreeSet.class, executions);
    assertEquals(2, executions.size());
  }

  /**
   * Test to ensure that the process method returns the same instance of AsyncProcess
   * for fluent interface usage.
   */
  @Test
  void testProcessReturnsFluentInterface() {
    when(mockExecution1.getAsyncProcessId()).thenReturn("test-id");
    when(mockExecution1.getTime()).thenReturn(new Date());
    when(mockExecution1.getState()).thenReturn(AsyncProcessState.DONE);

    AsyncProcess result = asyncProcess
        .process(mockExecution1);

    assertEquals(asyncProcess, result);
  }
}
