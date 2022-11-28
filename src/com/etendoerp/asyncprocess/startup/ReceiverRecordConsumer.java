package com.etendoerp.asyncprocess.startup;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Uuid;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;

import com.etendoerp.asyncprocess.model.AsyncProcessExecution;
import com.etendoerp.asyncprocess.model.AsyncProcessState;
import com.smf.jobs.ActionResult;

import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

/**
 * Class with encapsulates all need object to receive a message, call consumer and response based on result
 */
class ReceiverRecordConsumer implements Consumer<ReceiverRecord<String, AsyncProcessExecution>> {
  private static final Logger logger = LogManager.getLogger();
  private final String jobId;
  private final Function<JSONObject, ActionResult> consumer;
  private final KafkaSender<String, AsyncProcessExecution> kafkaSender;
  private final String nextTopic;
  private final String errorTopic;
  private final String clientId;
  private final String orgId;
  private final AsyncProcessState targetStatus;

  public ReceiverRecordConsumer(
      String jobId,
      Function<JSONObject, ActionResult> consumer, String nextTopic, String errorTopic,
      AsyncProcessState targetStatus,
      KafkaSender<String, AsyncProcessExecution> kafkaSender,
      String clientId,
      String orgId) {
    this.jobId = jobId;
    this.consumer = consumer;
    this.nextTopic = nextTopic;
    this.errorTopic = errorTopic;
    this.targetStatus = targetStatus;
    this.kafkaSender = kafkaSender;
    this.clientId = clientId;
    this.orgId = orgId;
  }

  @Override
  public void accept(
      ReceiverRecord<String, AsyncProcessExecution> record) {
    var value = record.value();
    AsyncProcessExecution responseRecord = new AsyncProcessExecution();
    responseRecord.setDescription(value.getDescription());
    responseRecord.setAsyncProcessId(value.getAsyncProcessId());
    String log = value.getLog();

    try {
      ReceiverOffset offset = record.receiverOffset();
      logger.info("Received message: topic-partition={} offset={} key={} value={}",
          offset.topicPartition(),
          offset.offset(),
          record.key(),
          record.value());
      var params = record.value().getParams();
      var jsObj = new JSONObject(params);
      if (!jsObj.has("jobs_job_id")) {
        jsObj.put("jobs_job_id", jobId);
      }
      if (!jsObj.has("client_id")) {
        jsObj.put("client_id", clientId);
      }
      if (!jsObj.has("org_id")) {
        jsObj.put("org_id", orgId);
      }
      var result = consumer.apply(jsObj);
      jsObj.put("message", result.getMessage());
      if (!StringUtils.isEmpty(result.getMessage())) {
        log = log + "\n" + new Date() + ": " + result.getMessage();
      }
      responseRecord.setLog(log);
      responseRecord.setParams(jsObj.toString());
      responseRecord.setState(targetStatus);
      createResponse(nextTopic, record.value().getAsyncProcessId(), kafkaSender,
          responseRecord);
    } catch (Exception e) {
      e.printStackTrace();
      log = log + new Date() + e.getMessage();
      responseRecord.setLog(log);
      responseRecord.setState(AsyncProcessState.ERROR);
      createResponse(errorTopic, record.value().getAsyncProcessId(), kafkaSender,
          responseRecord);
    }
  }

  public void createResponse(String topic, String asyncProcessId,
      KafkaSender<String, AsyncProcessExecution> kafkaSender,
      AsyncProcessExecution responseRecord) {
    responseRecord.setId(Uuid.randomUuid().toString());
    responseRecord.setTime(new Date());
    List<AsyncProcessExecution> list = new ArrayList<>();
    list.add(responseRecord);

    kafkaSender.send(Flux.fromStream(list.stream())
            .map(fluxMsg ->
                SenderRecord.create(
                    new ProducerRecord<>(topic, fluxMsg.getAsyncProcessId(), fluxMsg), fluxMsg.getAsyncProcessId())))
        .doOnError(e -> logger.error("Send failed", e))
        .subscribe(r -> {
          RecordMetadata metadata = r.recordMetadata();
          logger.info("Message {} sent successfully, topic-partition={}-{} offset={}",
              r.correlationMetadata(),
              metadata.topic(),
              metadata.partition(),
              metadata.offset());
        });
  }
}
