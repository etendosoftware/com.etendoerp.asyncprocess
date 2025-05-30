package com.etendoerp.asyncprocess.serdes;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import com.etendoerp.asyncprocess.model.AsyncProcess;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AsyncProcessDeserializer implements Deserializer<AsyncProcess> {
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    /* This method is needed for future configurations on deserialize */
  }

  @Override
  public AsyncProcess deserialize(String topic, byte[] data) {
    try {
      if (data == null) {
        return null;
      }
      return objectMapper.readValue(new String(data, StandardCharsets.UTF_8), AsyncProcess.class);
    } catch (Exception e) {
      throw new SerializationException("Error when deserializing byte[] to MessageDto");
    }
  }

  @Override
  public void close() {
    /* Placeholder for deserialization actions */
  }
}
