package com.etendoerp.asyncprocess.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.smf.jobs.model.Job;

/**
 * Unit tests for the TopicUtil class.
 * This class tests the topic creation functionality for background process communication
 * in async processing scenarios.
 */
@RunWith(MockitoJUnitRunner.class)
public class TopicUtilTest {

  @Mock
  private Job mockJob;

  /**
   * Tests the createTopic method with Long subtopic parameter.
   * Verifies that:
   * <ul>
   *   <li>The method correctly converts Long to String and delegates to the String version</li>
   *   <li>Spaces in job names are replaced with underscores</li>
   *   <li>The result follows the pattern: jobName.subtopic</li>
   * </ul>
   */
  @Test
  public void testCreateTopicWithLongSubtopic() {
    // Arrange
    when(mockJob.getName()).thenReturn("Test Job");
    Long subtopic = 12345L;

    // Act
    String result = TopicUtil.createTopic(mockJob, subtopic);

    // Assert
    assertEquals("Test_Job.12345", result);
  }

  /**
   * Tests the createTopic method with String subtopic parameter.
   * Verifies that:
   * <ul>
   *   <li>Spaces in job names are replaced with underscores</li>
   *   <li>The result follows the pattern: jobName.subtopic</li>
   *   <li>String subtopics are used directly without conversion</li>
   * </ul>
   */
  @Test
  public void testCreateTopicWithStringSubtopic() {
    // Arrange
    when(mockJob.getName()).thenReturn("Test Job");
    String subtopic = "process-data";

    // Act
    String result = TopicUtil.createTopic(mockJob, subtopic);

    // Assert
    assertEquals("Test_Job.process-data", result);
  }

  /**
   * Tests the createTopic method with job name containing multiple spaces.
   * Verifies that all spaces are properly replaced with underscores.
   */
  @Test
  public void testCreateTopicWithMultipleSpacesInJobName() {
    // Arrange
    when(mockJob.getName()).thenReturn("My Complex Job Name");
    String subtopic = "task1";

    // Act
    String result = TopicUtil.createTopic(mockJob, subtopic);

    // Assert
    assertEquals("My_Complex_Job_Name.task1", result);
  }

  /**
   * Tests the createTopic method with job name containing no spaces.
   * Verifies that job names without spaces remain unchanged.
   */
  @Test
  public void testCreateTopicWithNoSpacesInJobName() {
    // Arrange
    when(mockJob.getName()).thenReturn("SimpleJob");
    String subtopic = "subtask";

    // Act
    String result = TopicUtil.createTopic(mockJob, subtopic);

    // Assert
    assertEquals("SimpleJob.subtask", result);
  }

  /**
   * Tests the createTopic method with empty subtopic string.
   * Verifies that empty subtopics are handled correctly.
   */
  @Test
  public void testCreateTopicWithEmptySubtopic() {
    // Arrange
    when(mockJob.getName()).thenReturn("Test Job");
    String subtopic = "";

    // Act
    String result = TopicUtil.createTopic(mockJob, subtopic);

    // Assert
    assertEquals("Test_Job.", result);
  }

  /**
   * Tests the createTopic method with zero Long subtopic.
   * Verifies that zero values are handled correctly.
   */
  @Test
  public void testCreateTopicWithZeroLongSubtopic() {
    // Arrange
    when(mockJob.getName()).thenReturn("Zero Test");
    Long subtopic = 0L;

    // Act
    String result = TopicUtil.createTopic(mockJob, subtopic);

    // Assert
    assertEquals("Zero_Test.0", result);
  }

  /**
   * Tests the createTopic method with negative Long subtopic.
   * Verifies that negative values are handled correctly.
   */
  @Test
  public void testCreateTopicWithNegativeLongSubtopic() {
    // Arrange
    when(mockJob.getName()).thenReturn("Negative Test");
    Long subtopic = -100L;

    // Act
    String result = TopicUtil.createTopic(mockJob, subtopic);

    // Assert
    assertEquals("Negative_Test.-100", result);
  }

  /**
   * Tests the createTopic method with job name containing special characters.
   * Verifies that only spaces are replaced, other characters remain unchanged.
   */
  @Test
  public void testCreateTopicWithSpecialCharactersInJobName() {
    // Arrange
    when(mockJob.getName()).thenReturn("Job-Name With Spaces & Symbols");
    String subtopic = "special";

    // Act
    String result = TopicUtil.createTopic(mockJob, subtopic);

    // Assert
    assertEquals("Job-Name_With_Spaces_&_Symbols.special", result);
  }

  /**
   * Tests the createTopic method with numeric string subtopic.
   * Verifies that numeric strings are handled correctly as subtopics.
   */
  @Test
  public void testCreateTopicWithNumericStringSubtopic() {
    // Arrange
    when(mockJob.getName()).thenReturn("Numeric Job");
    String subtopic = "999";

    // Act
    String result = TopicUtil.createTopic(mockJob, subtopic);

    // Assert
    assertEquals("Numeric_Job.999", result);
  }

  /**
   * Tests that the createTopic method returns a non-null result.
   * This is a basic sanity check for the method's behavior.
   */
  @Test
  public void testCreateTopicReturnsNonNull() {
    // Arrange
    when(mockJob.getName()).thenReturn("Basic Job");
    String subtopic = "test";

    // Act
    String result = TopicUtil.createTopic(mockJob, subtopic);

    // Assert
    assertNotNull("Topic should not be null", result);
  }

  /**
   * Tests the Long version delegates correctly to String version.
   * This test verifies the internal behavior of method delegation.
   */
  @Test
  public void testLongVersionDelegatesToStringVersion() {
    // Arrange
    when(mockJob.getName()).thenReturn("Delegation Test");
    Long longSubtopic = 42L;
    String stringSubtopic = "42";

    // Act
    String resultFromLong = TopicUtil.createTopic(mockJob, longSubtopic);
    String resultFromString = TopicUtil.createTopic(mockJob, stringSubtopic);

    // Assert
    assertEquals("Both methods should produce the same result", resultFromString, resultFromLong);
  }

  /**
   * Tests the private constructor for code coverage.
   * This test ensures 100% line coverage by invoking the private constructor
   * using reflection, which is a common practice for utility classes.
   */
  @Test
  public void testPrivateConstructor() throws Exception {
    // Arrange
    Constructor<TopicUtil> constructor = TopicUtil.class.getDeclaredConstructor();
    constructor.setAccessible(true);

    // Act & Assert
    // This should not throw any exception
    TopicUtil instance = constructor.newInstance();
    assertNotNull("Instance should be created", instance);
  }

  /**
   * Tests the createTopic method with null String subtopic.
   * Verifies that null String values are handled correctly.
   */
  @Test
  public void testCreateTopicWithNullStringSubtopic() {
    // Arrange
    when(mockJob.getName()).thenReturn("Null String Test");
    String subtopic = null;

    // Act
    String result = TopicUtil.createTopic(mockJob, subtopic);

    // Assert
    assertEquals("Null_String_Test.null", result);
  }
}
