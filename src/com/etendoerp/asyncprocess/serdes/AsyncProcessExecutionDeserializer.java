package com.etendoerp.asyncprocess.serdes;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.commons.lang.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import com.etendoerp.asyncprocess.model.AsyncProcessExecution;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AsyncProcessExecutionDeserializer implements Deserializer<AsyncProcessExecution>, Serializer<AsyncProcessExecution> {
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public byte[] serialize(String topic, AsyncProcessExecution data) {
    try {
      return objectMapper.writeValueAsBytes(data);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public AsyncProcessExecution deserialize(String topic, byte[] data) {
    try {
      if (data == null) {
        return null;
      }
      return objectMapper.readValue(new String(data, StandardCharsets.UTF_8), AsyncProcessExecution.class);
    } catch (Exception e) {
      throw new SerializationException("Error when deserializing byte[] to MessageDto");
    }
  }

  @Override
  public void close() {
  }
}
