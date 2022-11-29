package com.etendoerp.asyncprocess.startup;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Uuid;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;

import com.etendoerp.asyncprocess.model.AsyncProcessExecution;
import com.etendoerp.asyncprocess.model.AsyncProcessState;
import com.smf.jobs.Action;
import com.smf.jobs.AsyncAction;

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
  private final Supplier<Action> actionFactory;
  private final KafkaSender<String, AsyncProcessExecution> kafkaSender;
  private final String nextTopic;
  private final String errorTopic;
  private final String clientId;
  private final String orgId;
  private final AsyncProcessState targetStatus;

  public ReceiverRecordConsumer(
      String jobId,
      Supplier<Action> actionFactory,
      String nextTopic, String errorTopic,
      AsyncProcessState targetStatus,
      KafkaSender<String, AsyncProcessExecution> kafkaSender,
      String clientId,
      String orgId) {
    this.jobId = jobId;
    this.actionFactory = actionFactory;
    this.nextTopic = nextTopic;
    this.errorTopic = errorTopic;
    this.targetStatus = targetStatus;
    this.kafkaSender = kafkaSender;
    this.clientId = clientId;
    this.orgId = orgId;
  }

  @Override
  public void accept(
      ReceiverRecord<String, AsyncProcessExecution> receiverRecord) {
    var value = receiverRecord.value();
    AsyncProcessExecution responseRecord = new AsyncProcessExecution();
    responseRecord.setDescription(value.getDescription());
    responseRecord.setAsyncProcessId(value.getAsyncProcessId());
    String log = value.getLog();

    try {
      ReceiverOffset offset = receiverRecord.receiverOffset();
      logger.info("Received message: topic-partition={} offset={} key={} value={}",
          offset.topicPartition(),
          offset.offset(),
          receiverRecord.key(),
          receiverRecord.value());
      var strParams = receiverRecord.value().getParams();
      var params = new JSONObject(strParams);
      if (!params.has("jobs_job_id")) {
        params.put("jobs_job_id", jobId);
      }
      if (!params.has("client_id")) {
        params.put("client_id", clientId);
      }
      if (!params.has("org_id")) {
        params.put("org_id", orgId);
      }
      var result = AsyncAction.run(actionFactory, params);
      params.put("message", result.getMessage());
      if (!StringUtils.isEmpty(result.getMessage())) {
        log = log + "\n" + new Date() + ": " + result.getMessage();
      }
      responseRecord.setLog(log);
      responseRecord.setParams(params.toString());
      responseRecord.setState(targetStatus);
      createResponse(nextTopic, receiverRecord.value().getAsyncProcessId(), kafkaSender,
          responseRecord);
    } catch (Exception e) {
      logger.error("Ann error has ocurred on ReceiverRecordConsumer accept method", e);
      log = log + new Date() + e.getMessage();
      responseRecord.setLog(log);
      responseRecord.setState(AsyncProcessState.ERROR);
      createResponse(errorTopic, receiverRecord.value().getAsyncProcessId(), kafkaSender,
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
