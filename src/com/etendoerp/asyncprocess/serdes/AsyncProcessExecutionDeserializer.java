package com.etendoerp.asyncprocess.serdes;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.google.gson.JsonObject;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import com.etendoerp.asyncprocess.model.AsyncProcessExecution;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AsyncProcessExecutionDeserializer implements Deserializer<AsyncProcessExecution>, Serializer<AsyncProcessExecution> {
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    /* This method is needed for future configurations on deserialize */
  }

  @Override
  public byte[] serialize(String topic, AsyncProcessExecution data) {
    try {
      return objectMapper.writeValueAsBytes(data);
    } catch (JsonProcessingException e) {
      throw new SerializationException("Error serializing instance to byte[]");
    }
  }

  @Override
  public AsyncProcessExecution deserialize(String topic, byte[] data) {
    try {
      if (data == null) {
        return null;
      }
      JsonObject jsonObject = new JsonObject();
      jsonObject.addProperty("id", "0");
      jsonObject.addProperty("asyncProcessId", "4448");
      jsonObject.addProperty("log", "");
      jsonObject.addProperty("description", "");
      jsonObject.addProperty("params", new String(data, StandardCharsets.UTF_8));
      String newData = jsonObject.toString();
      return objectMapper.readValue(newData, AsyncProcessExecution.class);
    } catch (Exception e) {
      throw new SerializationException("Error when deserializing byte[] to MessageDto");
    }
  }

  @Override
  public void close() {
    /* Placeholder for deserialization actions */
  }
}
