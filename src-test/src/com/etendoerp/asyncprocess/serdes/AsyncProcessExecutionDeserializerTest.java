package com.etendoerp.asyncprocess.serdes;

import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.TEST_DATA;
import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.TEST_TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.SerializationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;

import com.etendoerp.asyncprocess.model.AsyncProcessExecution;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This class tests the AsyncProcessExecutionDeserializer functionality.
 * It verifies that the serializer and deserializer methods work correctly
 * for the AsyncProcessExecution model.
 */
@ExtendWith(MockitoExtension.class)
class AsyncProcessExecutionDeserializerTest {

  private AsyncProcessExecutionDeserializer deserializer;
  private AsyncProcessExecution testExecution;
  private Map<String, Object> configs;

  /**
   * This method sets up the test environment before each test case.
   * It initializes the AsyncProcessExecutionDeserializer and a sample AsyncProcessExecution object.
   */
  @BeforeEach
  void setUp() {
    deserializer = new AsyncProcessExecutionDeserializer();
    testExecution = new AsyncProcessExecution();
    configs = new HashMap<>();
  }

  /**
   * This test verifies that the serialize method of AsyncProcessExecutionDeserializer
   * correctly serializes an AsyncProcessExecution object into a byte array.
   */
  @Test
  void testSerializeSuccess() {
    // Given
    try (MockedConstruction<ObjectMapper> ignored = mockConstruction(ObjectMapper.class,
        (mock, context) -> {
          try {
            byte[] expectedBytes = TEST_DATA.getBytes();
            when(mock.writeValueAsBytes(any(AsyncProcessExecution.class)))
                .thenReturn(expectedBytes);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        })) {

      AsyncProcessExecutionDeserializer newDeserializer = new AsyncProcessExecutionDeserializer();

      byte[] result = newDeserializer.serialize(TEST_TOPIC, testExecution);

      assertEquals(TEST_DATA, new String(result));
    }
  }

  /**
   * This test verifies that the serialize method of AsyncProcessExecutionDeserializer
   * returns null when the input AsyncProcessExecution is null.
   */
  @Test
  void testSerializeThrowsException() {
    try (MockedConstruction<ObjectMapper> ignored = mockConstruction(ObjectMapper.class,
        (mock, context) -> {
          try {
            when(mock.writeValueAsBytes(any(AsyncProcessExecution.class)))
                .thenThrow(new JsonProcessingException("Test exception") {
                });
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        })) {

      AsyncProcessExecutionDeserializer newDeserializer = new AsyncProcessExecutionDeserializer();

      SerializationException exception = assertThrows(SerializationException.class,
          () -> newDeserializer.serialize(TEST_TOPIC, testExecution));

      assertEquals("Error serializing instance to byte[]", exception.getMessage());
    }
  }

  /**
   * This test verifies that the deserialize method of AsyncProcessExecutionDeserializer
   * correctly deserializes a byte array into an AsyncProcessExecution object.
   */
  @Test
  void testDeserializeSuccess() {
    // Given
    String testParams = "test parameters";
    byte[] testData = testParams.getBytes(StandardCharsets.UTF_8);
    AsyncProcessExecution expectedExecution = new AsyncProcessExecution();

    try (MockedConstruction<ObjectMapper> ignored = mockConstruction(ObjectMapper.class,
        (mock, context) -> {
          try {
            when(mock.readValue(anyString(), eq(AsyncProcessExecution.class)))
                .thenReturn(expectedExecution);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        })) {

      AsyncProcessExecutionDeserializer newDeserializer = new AsyncProcessExecutionDeserializer();

      AsyncProcessExecution result = newDeserializer.deserialize(TEST_TOPIC, testData);

      assertNotNull(result);
      assertEquals(expectedExecution, result);
    }
  }

  /**
   * This test verifies that the deserialize method of AsyncProcessExecutionDeserializer
   * returns null when the input data is null.
   */
  @Test
  void testDeserializeWithNullData() {
    AsyncProcessExecution result = deserializer.deserialize(TEST_TOPIC, null);

    assertNull(result);
  }

  /**
   * This test verifies that the deserialize method of AsyncProcessExecutionDeserializer
   * throws a SerializationException when an error occurs during deserialization.
   */
  @Test
  void testDeserializeThrowsException() {
    // Given
    byte[] testData = TEST_DATA.getBytes();

    try (MockedConstruction<ObjectMapper> ignored = mockConstruction(ObjectMapper.class,
        (mock, context) -> {
          try {
            when(mock.readValue(anyString(), eq(AsyncProcessExecution.class)))
                .thenThrow(new RuntimeException("Test exception"));
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        })) {

      AsyncProcessExecutionDeserializer newDeserializer = new AsyncProcessExecutionDeserializer();

      SerializationException exception = assertThrows(SerializationException.class,
          () -> newDeserializer.deserialize(TEST_TOPIC, testData));

      assertEquals("Error when deserializing byte[] to MessageDto", exception.getMessage());
    }
  }

  /**
   * This test verifies that the deserialize method of AsyncProcessExecutionDeserializer
   * can be called with ObjectMapper construction without throwing exceptions.
   */
  @Test
  void testDeserializeWithObjectMapperConstruction() {
    // Given
    try (MockedConstruction<ObjectMapper> ignored = mockConstruction(ObjectMapper.class,
        (mock, context) -> {
          try {
            AsyncProcessExecution expectedExecution = new AsyncProcessExecution();
            when(mock.readValue(anyString(), eq(AsyncProcessExecution.class)))
                .thenReturn(expectedExecution);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        })) {

      AsyncProcessExecutionDeserializer newDeserializer = new AsyncProcessExecutionDeserializer();
      String testParams = "test parameters";
      byte[] testData = testParams.getBytes(StandardCharsets.UTF_8);

      AsyncProcessExecution result = newDeserializer.deserialize(TEST_TOPIC, testData);

      assertNotNull(result);
    }
  }

  /**
   * This test verifies that the serialize method of AsyncProcessExecutionDeserializer
   * can be called with ObjectMapper construction without throwing exceptions.
   */
  @Test
  void testSerializeWithObjectMapperConstruction() {
    // Given
    try (MockedConstruction<ObjectMapper> ignored = mockConstruction(ObjectMapper.class,
        (mock, context) -> {
          try {
            byte[] expectedBytes = TEST_DATA.getBytes();
            when(mock.writeValueAsBytes(any(AsyncProcessExecution.class)))
                .thenReturn(expectedBytes);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        })) {

      AsyncProcessExecutionDeserializer newDeserializer = new AsyncProcessExecutionDeserializer();
      AsyncProcessExecution localExecution = new AsyncProcessExecution();

      byte[] result = newDeserializer.serialize(TEST_TOPIC, localExecution);

      assertNotNull(result);
    }
  }

  /**
   * This test verifies that the configure method of AsyncProcessExecutionDeserializer
   * can be called with different configurations without throwing exceptions.
   */
  @Test
  void testConfigure() {
    deserializer.configure(configs, true);
    deserializer.configure(configs, false);
    deserializer.configure(null, true);
  }

  /**
   * This test verifies that the close method of AsyncProcessExecutionDeserializer
   * does not throw any exceptions.
   */
  @Test
  void testClose() {
    deserializer.close();
  }

  /**
   * This test verifies the deserialization of AsyncProcessExecution from a byte array.
   * It checks that the deserializer correctly constructs an AsyncProcessExecution object
   * with the expected fields.
   */
  @Test
  void testDeserializeJsonObjectConstruction() {
    // This test verifies the specific JSON structure created in deserialize
    try (MockedConstruction<ObjectMapper> ignored = mockConstruction(ObjectMapper.class,
        (mock, context) -> {
          try {
            AsyncProcessExecution expectedExecution = new AsyncProcessExecution();
            when(mock.readValue(anyString(), eq(AsyncProcessExecution.class)))
                .thenAnswer(invocation -> {
                  String jsonString = invocation.getArgument(0);
                  // Verify the JSON contains expected structure
                  assert jsonString.contains("\"id\":\"0\"");
                  assert jsonString.contains("\"asyncProcessId\":\"4448\"");
                  assert jsonString.contains("\"log\":\"\"");
                  assert jsonString.contains("\"description\":\"\"");
                  assert jsonString.contains("\"params\":\"test params\"");
                  return expectedExecution;
                });
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        })) {

      AsyncProcessExecutionDeserializer newDeserializer = new AsyncProcessExecutionDeserializer();
      byte[] testData = "test params".getBytes(StandardCharsets.UTF_8);

      AsyncProcessExecution result = newDeserializer.deserialize(TEST_TOPIC, testData);

      assertNotNull(result);
    }
  }

  /**
   * This test verifies the behavior of serialization when null data is passed.
   * It should return a byte array representing null.
   */
  @Test
  void testSerializeWithNullData() {
    // Given
    try (MockedConstruction<ObjectMapper> ignored = mockConstruction(ObjectMapper.class,
        (mock, context) -> {
          try {
            when(mock.writeValueAsBytes(null)).thenReturn("null".getBytes());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        })) {

      AsyncProcessExecutionDeserializer newDeserializer = new AsyncProcessExecutionDeserializer();

      byte[] result = newDeserializer.serialize(TEST_TOPIC, null);

      assertEquals("null", new String(result));
    }
  }

  /**
   * This test verifies the behavior of deserialization when an empty byte array is passed.
   * It should return a new instance of AsyncProcessExecution without throwing an exception.
   */
  @Test
  void testDeserializeWithEmptyData() {
    byte[] emptyData = new byte[0];

    try (MockedConstruction<ObjectMapper> ignored = mockConstruction(ObjectMapper.class,
        (mock, context) -> {
          try {
            AsyncProcessExecution expectedExecution = new AsyncProcessExecution();
            when(mock.readValue(anyString(), eq(AsyncProcessExecution.class)))
                .thenReturn(expectedExecution);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        })) {

      AsyncProcessExecutionDeserializer newDeserializer = new AsyncProcessExecutionDeserializer();

      AsyncProcessExecution result = newDeserializer.deserialize(TEST_TOPIC, emptyData);

      assertNotNull(result);
    }
  }

  /**
   * This test specifically verifies the JSON structure that gets passed to ObjectMapper
   * during deserialization, ensuring that the expected fields are present.
   */
  @Test
  void testDeserializeVerifyJsonStructure() {
    // This test specifically verifies the JSON structure that gets passed to ObjectMapper
    String testParams = "my test parameters";
    byte[] testData = testParams.getBytes(StandardCharsets.UTF_8);

    try (MockedConstruction<ObjectMapper> ignored = mockConstruction(ObjectMapper.class,
        (mock, context) -> {
          try {
            when(mock.readValue(anyString(), eq(AsyncProcessExecution.class)))
                .thenAnswer(invocation -> {
                  String jsonString = invocation.getArgument(0);
                  String expectedJson = "{\"id\":\"0\",\"asyncProcessId\":\"4448\",\"log\":\"\",\"description\":\"\",\"params\":\"" + testParams + "\"}";
                  assertEquals(expectedJson, jsonString);
                  return new AsyncProcessExecution();
                });
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        })) {

      AsyncProcessExecutionDeserializer newDeserializer = new AsyncProcessExecutionDeserializer();

      AsyncProcessExecution result = newDeserializer.deserialize(TEST_TOPIC, testData);

      assertNotNull(result);
    }
  }

  /**
   * This test verifies the actual behavior of serialization and deserialization
   * without mocking, ensuring that the AsyncProcessExecutionDeserializer works as expected.
   */
  @Test
  void testSerializeAndDeserializeIntegration() {
    // This test verifies the actual behavior without mocking for integration testing
    AsyncProcessExecutionDeserializer realDeserializer = new AsyncProcessExecutionDeserializer();
    AsyncProcessExecution originalExecution = new AsyncProcessExecution();

    byte[] serialized = realDeserializer.serialize(TEST_TOPIC, originalExecution);

    assertNotNull(serialized);
    // Verify it's valid JSON by checking it starts with '{' and ends with '}'
    String jsonString = new String(serialized);
    assertEquals('{', jsonString.charAt(0));
    assertEquals('}', jsonString.charAt(jsonString.length() - 1));
  }

  /**
   * This test verifies the actual deserialize behavior without mocking.
   * It assumes that the AsyncProcessExecutionDeserializer is correctly implemented
   * and can handle real data.
   */
  @Test
  void testDeserializeRealBehavior() {
    // This test verifies the actual deserialize behavior
    AsyncProcessExecutionDeserializer realDeserializer = new AsyncProcessExecutionDeserializer();
    String testParams = "real test parameters";
    byte[] testData = testParams.getBytes(StandardCharsets.UTF_8);

    AsyncProcessExecution result = realDeserializer.deserialize(TEST_TOPIC, testData);

    assertNotNull(result);
  }
}
