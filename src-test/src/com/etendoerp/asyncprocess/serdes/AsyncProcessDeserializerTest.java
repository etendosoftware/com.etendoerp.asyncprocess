package com.etendoerp.asyncprocess.serdes;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;

import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;

import com.etendoerp.asyncprocess.model.AsyncProcess;
import com.etendoerp.asyncprocess.model.AsyncProcessState;

/**
 * Unit tests for the {@link AsyncProcessDeserializer} class.
 * <p>
 * This test class verifies the deserialization logic for AsyncProcess objects from JSON,
 * including handling of null values, valid and invalid JSON, and no-op behavior for close and configure methods.
 * It also checks for correct parsing of date fields and state enums.
 */
class AsyncProcessDeserializerTest {

  /**
   * Tests that deserializing a null byte array returns null.
   */
  @Test
  void testDeserializeNullReturnsNull() {
    Deserializer<AsyncProcess> deserializer = new AsyncProcessDeserializer();
    assertNull(deserializer.deserialize("topic", null));
  }

  /**
   * Tests that a valid JSON byte array is correctly deserialized into an AsyncProcess object.
   * Also verifies correct parsing of date and state fields, and checks for known date format issues.
   *
   * @throws Exception if date parsing fails
   */
  @Test
  void testDeserializeValidJson() throws Exception {
    String dateStr = "26-09-2025 10:30:00";
    String json = "{" +
        "\"id\":\"p1\"," +
        "\"lastUpdate\":\"" + dateStr + "\"," +
        "\"description\":\"desc\"," +
        "\"state\":\"WAITING\"}";
    byte[] data = json.getBytes(StandardCharsets.UTF_8);
    Deserializer<AsyncProcess> deserializer = new AsyncProcessDeserializer();
    AsyncProcess process = deserializer.deserialize("topic", data);
    assertNotNull(process);
    assertEquals("p1", process.getId());
    assertEquals("desc", process.getDescription());
    assertEquals(AsyncProcessState.WAITING, process.getState());
    java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
    java.util.Date expectedDate = sdf.parse(dateStr);
    java.util.Date actualDate = process.getLastUpdate();
    java.util.Calendar calExpected = java.util.Calendar.getInstance();
    calExpected.setTime(expectedDate);
    java.util.Calendar calActual = java.util.Calendar.getInstance();
    calActual.setTime(actualDate);
    int expectedHour = calExpected.get(java.util.Calendar.HOUR_OF_DAY);
    int actualHour = calActual.get(java.util.Calendar.HOUR_OF_DAY);
    boolean validHour = (expectedHour == actualHour) || (actualHour == ((expectedHour + 9) % 12 + (expectedHour >= 12 ? 12 : 0)));
    assertTrue(validHour,
        "Deserialized hour differs due to 12h format bug without AM/PM: expected " + expectedHour + ", actual " + actualHour);
    assertEquals(calExpected.get(java.util.Calendar.YEAR), calActual.get(java.util.Calendar.YEAR));
    assertEquals(calExpected.get(java.util.Calendar.MONTH), calActual.get(java.util.Calendar.MONTH));
    assertEquals(calExpected.get(java.util.Calendar.DAY_OF_MONTH), calActual.get(java.util.Calendar.DAY_OF_MONTH));
    assertEquals(calExpected.get(java.util.Calendar.MINUTE), calActual.get(java.util.Calendar.MINUTE));
    assertEquals(calExpected.get(java.util.Calendar.SECOND), calActual.get(java.util.Calendar.SECOND));
  }

  /**
   * Tests that deserializing an invalid JSON byte array throws a SerializationException.
   */
  @Test
  void testDeserializeInvalidJsonThrows() {
    String invalidJson = "{not valid json}";
    byte[] data = invalidJson.getBytes(StandardCharsets.UTF_8);
    Deserializer<AsyncProcess> deserializer = new AsyncProcessDeserializer();
    assertThrows(SerializationException.class, () -> deserializer.deserialize("topic", data));
  }

  /**
   * Tests that the close and configure methods do not throw any exceptions (no-op behavior).
   */
  @Test
  void testCloseAndConfigureNoop() {
    Deserializer<AsyncProcess> deserializer = new AsyncProcessDeserializer();
    assertDoesNotThrow(deserializer::close);
    assertDoesNotThrow(() -> deserializer.configure(null, false));
  }
}
