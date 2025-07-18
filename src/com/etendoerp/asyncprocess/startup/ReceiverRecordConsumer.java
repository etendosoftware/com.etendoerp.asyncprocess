package com.etendoerp.asyncprocess.startup;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Uuid;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.openbravo.dal.core.OBContext;

import com.etendoerp.asyncprocess.model.AsyncProcessExecution;
import com.etendoerp.asyncprocess.model.AsyncProcessState;
import com.etendoerp.asyncprocess.retry.RetryPolicy;
import com.smf.jobs.Action;
import com.smf.jobs.ActionResult;
import com.smf.jobs.AsyncAction;

import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

/**
 * Enhanced class that encapsulates all necessary objects to receive a message,
 * call the consumer, and respond based on the result, with support for retries and parallel processing.
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

  // New fields for advanced configuration support
  private final RetryPolicy retryPolicy;
  private final ScheduledExecutorService scheduler;
  private final Map<String, AtomicInteger> retryAttempts = new ConcurrentHashMap<>();

  public ReceiverRecordConsumer(
      String jobId,
      Supplier<Action> actionFactory,
      String nextTopic, String errorTopic,
      AsyncProcessState targetStatus,
      KafkaSender<String, AsyncProcessExecution> kafkaSender,
      String clientId,
      String orgId) {
    this(jobId, actionFactory, nextTopic, errorTopic, targetStatus, kafkaSender, clientId, orgId, null, null);
  }

  /**
   * Extended constructor with support for advanced configuration
   */
  public ReceiverRecordConsumer(
      String jobId,
      Supplier<Action> actionFactory,
      String nextTopic, String errorTopic,
      AsyncProcessState targetStatus,
      KafkaSender<String, AsyncProcessExecution> kafkaSender,
      String clientId,
      String orgId,
      RetryPolicy retryPolicy,
      ScheduledExecutorService scheduler) {
    this.jobId = jobId;
    this.actionFactory = actionFactory;
    this.nextTopic = nextTopic;
    this.errorTopic = errorTopic;
    this.targetStatus = targetStatus;
    this.kafkaSender = kafkaSender;
    this.clientId = clientId;
    this.orgId = orgId;
    this.retryPolicy = retryPolicy;
    this.scheduler = scheduler;
  }

  @Override
  public void accept(ReceiverRecord<String, AsyncProcessExecution> receiverRecord) {
    processRecord(receiverRecord, 0);
  }

  /**
   * Processes a record with retry support
   * @param receiverRecord The record to process
   * @param attemptNumber The current attempt number
   */
  private void processRecord(ReceiverRecord<String, AsyncProcessExecution> receiverRecord, int attemptNumber) {
    var value = receiverRecord.value();
    AsyncProcessExecution responseRecord = new AsyncProcessExecution();
    responseRecord.setDescription(value == null ? StringUtils.EMPTY : value.getDescription());
    responseRecord.setAsyncProcessId(value == null ? StringUtils.EMPTY : receiverRecord.key());
    String log = value == null ? StringUtils.EMPTY : value.getLog();
    ReceiverOffset offset = receiverRecord.receiverOffset();

    // Establish OBContext
    boolean contextChanged = false;
    String previousClientId = null;
    String previousOrgId = null;
    String previousRoleId = null;
    String previousUserId = null;
    try {
      //if no context, set the default context. This is needed in the first message received
      if (OBContext.getOBContext() == null) {
        OBContext.setOBContext("100", "0", clientId, orgId);
      }
      //always set admin mode
      OBContext.setAdminMode(true);
      var strParams = receiverRecord.value() == null ? "{}" : receiverRecord.value().getParams();
      var params = new JSONObject(strParams);
      // if the context info is in message, set it. And remember if it was changed
      if (params.has("params")) {
        previousUserId = OBContext.getOBContext().getUser().getId();
        previousRoleId = OBContext.getOBContext().getRole().getId();
        previousClientId = OBContext.getOBContext().getCurrentClient().getId();
        previousOrgId = OBContext.getOBContext().getCurrentOrganization().getId();
        contextChanged = true;
        JSONObject paramsObj = new JSONObject(params.getString("params"));
        JSONObject context = paramsObj.getJSONObject("context");
        OBContext.setOBContext(
            context.optString("user", previousUserId),
            context.optString("role", previousRoleId),
            context.optString("client", previousClientId),
            context.optString("organization", previousOrgId)
        );
      }

      logger.info("Received message: topic-partition={} offset={} key={} attempt={}",
          offset.topicPartition(),
          offset.offset(),
          receiverRecord.key(),
          attemptNumber);

      setupJobParams(params);

      // Añadir información sobre el intento actual si hay reintentos
      if (attemptNumber > 0) {
        params.put("retry_attempt", attemptNumber);
        log = log + "\n" + new Date() + ": Reintento #" + attemptNumber;
      }

      var result = AsyncAction.run(actionFactory, params);
      params.put("message", result.getMessage());
      if (!StringUtils.isEmpty(result.getMessage())) {
        log = log + "\n" + new Date() + ": " + result.getMessage();
      }

      responseRecord.setLog(log);
      responseRecord.setParams(params.toString());
      responseRecord.setState(targetStatus);

      // Acknowledge the message only if no more retries are needed
      offset.acknowledge();

      List<String> targets = extractTargetsFromResult(result);
      for (String tp : targets) {
        createResponse(tp, kafkaSender, responseRecord);
      }

    } catch (Exception e) {
      logger.error("Error processing message: {}", e.getMessage(), e);
      handleError(receiverRecord, e, log, responseRecord, attemptNumber);
    } finally {
      // Revert admin mode
      OBContext.restorePreviousMode();
      if (contextChanged) {
        // Restore old context
        OBContext.setOBContext(
            previousUserId,
            previousRoleId,
            previousClientId,
            previousOrgId
        );
      }
    }
  }

  /**
   * Extracts the topics to send the response to from the ActionResult. The following rules are applied:
   * <ul>
   *   <li>The next topic is always included.</li>
   *   <li>If the message field is a JSONObject with a "next" property, this property is added to the
   *       list of topics. If the "next" property is a JSONArray, each element is added as a topic.</li>
   *   <li>If the message field is not a valid JSON, it is ignored.</li>
   * </ul>
   *
   * @param result
   *     The ActionResult to extract the topics from
   * @return A list of topics to send the response to
   */
  private List<String> extractTargetsFromResult(ActionResult result) {
    List<String> targets = new ArrayList<>();
    targets.add(nextTopic);

    try {
      JSONObject j = new JSONObject(result.getMessage());
      Object nxt = j.opt("next");

      if (nxt instanceof String && !JSONObject.NULL.equals(nxt)) {
        targets.add((String) nxt);
      } else if (nxt instanceof JSONArray) {
        JSONArray arr = (JSONArray) nxt;
        for (int i = 0; i < arr.length(); i++) {
          targets.add(arr.getString(i));
        }
      }
    } catch (JSONException e) {
      logger.warn("Invalid JSON in ActionResult message: {}", result.getMessage(), e);
    }

    return targets;
  }

  /**
   * Handles errors with retry support
   */
  private void handleError(
      ReceiverRecord<String, AsyncProcessExecution> receiverRecord,
      Exception e,
      String log,
      AsyncProcessExecution responseRecord,
      int attemptNumber) {

    // If retry policy exists and more retries are allowed
    if (retryPolicy != null && scheduler != null && retryPolicy.shouldRetry(attemptNumber + 1)) {
      int nextAttempt = attemptNumber + 1;
      long delay = retryPolicy.getRetryDelay(nextAttempt);

      logger.info("Scheduling retry {} for message {} after {} ms",
          nextAttempt, receiverRecord.key(), delay);

      // No confirmar el offset para permitir el reintento después
      scheduler.schedule(() -> processRecord(receiverRecord, nextAttempt), delay, TimeUnit.MILLISECONDS);
    } else {
      // No hay más reintentos, enviar al topic de error
      log = log + "\n" + new Date() + ": " + e.getMessage();
      if (attemptNumber > 0) {
        log = log + "\n" + new Date() + ": Max reintentos alcanzados (" + attemptNumber + ")";
      }

      responseRecord.setLog(log);
      responseRecord.setState(AsyncProcessState.ERROR);

      // Acknowledge the message since we will send to error topic
      receiverRecord.receiverOffset().acknowledge();

      createResponse(errorTopic, kafkaSender, responseRecord);
    }
  }

  /**
   * Sets job parameters
   */
  private void setupJobParams(JSONObject params) throws JSONException {
    if (!params.has("jobs_job_id")) {
      params.put("jobs_job_id", jobId);
    }
    if (!params.has("client_id")) {
      params.put("client_id", clientId);
    }
    if (!params.has("org_id")) {
      params.put("org_id", orgId);
    }
  }

  public void createResponse(String topic,
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