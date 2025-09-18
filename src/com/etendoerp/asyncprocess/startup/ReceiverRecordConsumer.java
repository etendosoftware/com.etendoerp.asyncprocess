package com.etendoerp.asyncprocess.startup;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
import org.openbravo.base.exception.OBException;
import org.openbravo.client.application.Process;
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.service.OBDal;

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
public class ReceiverRecordConsumer implements Consumer<ReceiverRecord<String, AsyncProcessExecution>> {
  private static final Logger logger = LogManager.getLogger();

  // Configuration container to reduce constructor parameters
  public static class ConsumerConfig {
    private final String jobId;
    private final Supplier<Action> actionFactory;
    private final String nextTopic;
    private final String errorTopic;
    private final AsyncProcessState targetStatus;
    private final KafkaSender<String, AsyncProcessExecution> kafkaSender;
    private final String clientId;
    private final String orgId;
    private final RetryPolicy retryPolicy;
    private final ScheduledExecutorService scheduler;

    private ConsumerConfig(Builder builder) {
      this.jobId = builder.jobId;
      this.actionFactory = builder.actionFactory;
      this.nextTopic = builder.nextTopic;
      this.errorTopic = builder.errorTopic;
      this.targetStatus = builder.targetStatus;
      this.kafkaSender = builder.kafkaSender;
      this.clientId = builder.clientId;
      this.orgId = builder.orgId;
      this.retryPolicy = builder.retryPolicy;
      this.scheduler = builder.scheduler;
    }

    public static class Builder {
      private String jobId;
      private Supplier<Action> actionFactory;
      private String nextTopic;
      private String errorTopic;
      private AsyncProcessState targetStatus;
      private KafkaSender<String, AsyncProcessExecution> kafkaSender;
      private String clientId;
      private String orgId;
      private RetryPolicy retryPolicy;
      private ScheduledExecutorService scheduler;

      public Builder jobId(String jobId) {
        this.jobId = jobId;
        return this;
      }

      public Builder actionFactory(Supplier<Action> actionFactory) {
        this.actionFactory = actionFactory;
        return this;
      }

      public Builder nextTopic(String nextTopic) {
        this.nextTopic = nextTopic;
        return this;
      }

      public Builder errorTopic(String errorTopic) {
        this.errorTopic = errorTopic;
        return this;
      }

      public Builder targetStatus(AsyncProcessState targetStatus) {
        this.targetStatus = targetStatus;
        return this;
      }

      public Builder kafkaSender(KafkaSender<String, AsyncProcessExecution> kafkaSender) {
        this.kafkaSender = kafkaSender;
        return this;
      }

      public Builder clientId(String clientId) {
        this.clientId = clientId;
        return this;
      }

      public Builder orgId(String orgId) {
        this.orgId = orgId;
        return this;
      }

      public Builder retryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
        return this;
      }

      public Builder scheduler(ScheduledExecutorService scheduler) {
        this.scheduler = scheduler;
        return this;
      }

      public ConsumerConfig build() {
        // Validate required fields
        if (jobId == null || jobId.trim().isEmpty()) {
          throw new IllegalArgumentException("jobId is required");
        }
        if (actionFactory == null) {
          throw new IllegalArgumentException("actionFactory is required");
        }
        if (nextTopic == null || nextTopic.trim().isEmpty()) {
          throw new IllegalArgumentException("nextTopic is required");
        }
        if (errorTopic == null || errorTopic.trim().isEmpty()) {
          throw new IllegalArgumentException("errorTopic is required");
        }
        if (targetStatus == null) {
          throw new IllegalArgumentException("targetStatus is required");
        }
        if (kafkaSender == null) {
          throw new IllegalArgumentException("kafkaSender is required");
        }
        if (clientId == null || clientId.trim().isEmpty()) {
          throw new IllegalArgumentException("clientId is required");
        }
        if (orgId == null || orgId.trim().isEmpty()) {
          throw new IllegalArgumentException("orgId is required");
        }

        return new ConsumerConfig(this);
      }
    }

    // Getters
    public String getJobId() { return jobId; }
    public Supplier<Action> getActionFactory() { return actionFactory; }
    public String getNextTopic() { return nextTopic; }
    public String getErrorTopic() { return errorTopic; }
    public AsyncProcessState getTargetStatus() { return targetStatus; }
    public KafkaSender<String, AsyncProcessExecution> getKafkaSender() { return kafkaSender; }
    public String getClientId() { return clientId; }
    public String getOrgId() { return orgId; }
    public RetryPolicy getRetryPolicy() { return retryPolicy; }
    public ScheduledExecutorService getScheduler() { return scheduler; }
  }

  private final ConsumerConfig config;

  /**
   * Extended constructor with support for advanced configuration
   */
  public ReceiverRecordConsumer(
      ConsumerConfig config) {
    this.config = config;
  }

  /**
   * Accepts and processes the received Kafka record.
   */
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
    AsyncProcessExecution responseRecord = createInitialResponseRecord(value, receiverRecord.key());
    String log = value == null ? StringUtils.EMPTY : value.getLog();
    ReceiverOffset offset = receiverRecord.receiverOffset();

    ContextInfo contextInfo = new ContextInfo();

    try {
      setupOBContext();

      JSONObject params = parseAndSetupParams(receiverRecord, attemptNumber);
      log = updateLogWithRetryInfo(log, attemptNumber);

      ActionResult result = executeAction(params);
      params = enrichParamsWithActionResult(params, result);
      log = updateLogWithResult(log, result);

      responseRecord.setLog(log);
      responseRecord.setParams(params.toString());
      responseRecord.setState(config.getTargetStatus());

      sendResponsesAndAcknowledge(receiverRecord, responseRecord, offset, result);

    } catch (Exception e) {
      logger.error("Error processing message: {}", e.getMessage(), e);
      handleError(receiverRecord, e, log, responseRecord, attemptNumber);
    } finally {
      restoreOBContext(contextInfo);
    }
  }

  /**
   * Helper class to store OB context information
   */
  private static class ContextInfo {
    boolean contextChanged = false;
    String previousClientId;
    String previousOrgId;
    String previousRoleId;
    String previousUserId;
  }

  /**
   * Creates initial response record from received data
   */
  private AsyncProcessExecution createInitialResponseRecord(AsyncProcessExecution value, String key) {
    AsyncProcessExecution responseRecord = new AsyncProcessExecution();
    responseRecord.setDescription(value == null ? StringUtils.EMPTY : value.getDescription());
    responseRecord.setAsyncProcessId(value == null ? StringUtils.EMPTY : key);
    return responseRecord;
  }

  /**
   * Sets up OB context for message processing
   */
  private void setupOBContext() {
    // If no context, set the default context. This is needed in the first message received
    if (OBContext.getOBContext() == null) {
      OBContext.setOBContext("100", "0", config.getClientId(), config.getOrgId());
    }
    // Always set admin mode
    OBContext.setAdminMode(true);
  }

  /**
   * Parses parameters and sets up context if needed
   */
  private JSONObject parseAndSetupParams(ReceiverRecord<String, AsyncProcessExecution> receiverRecord,
                                        int attemptNumber) throws JSONException {
    var strParams = receiverRecord.value() == null ? "{}" : receiverRecord.value().getParams();
    var params = new JSONObject(strParams);

    // Handle context setup from message parameters
    setupContextFromParams(params);

    logger.debug("Received message: topic-partition={} offset={} key={} attempt={}",
        receiverRecord.receiverOffset().topicPartition(),
        receiverRecord.receiverOffset().offset(),
        receiverRecord.key(),
        attemptNumber);

    setupJobParams(params);
    return params;
  }

  /**
   * Sets up context from message parameters
   */
  private void setupContextFromParams(JSONObject params) throws JSONException {
    if (params.has("params")) {
      ContextInfo contextInfo = new ContextInfo();
      contextInfo.previousUserId = OBContext.getOBContext().getUser().getId();
      contextInfo.previousRoleId = OBContext.getOBContext().getRole().getId();
      contextInfo.previousClientId = OBContext.getOBContext().getCurrentClient().getId();
      contextInfo.previousOrgId = OBContext.getOBContext().getCurrentOrganization().getId();
      contextInfo.contextChanged = true;

      JSONObject paramsObj = new JSONObject(params.getString("params"));
      JSONObject context = paramsObj.optJSONObject("context");
      if (context == null) {
        context = new JSONObject();
      }

      OBContext.setOBContext(
          context.optString("user", contextInfo.previousUserId),
          context.optString("role", contextInfo.previousRoleId),
          context.optString("client", contextInfo.previousClientId),
          context.optString("organization", contextInfo.previousOrgId)
      );
    }
  }

  /**
   * Updates log with retry information
   */
  private String updateLogWithRetryInfo(String log, int attemptNumber) {
    if (attemptNumber > 0) {
      return log + "\n" + new Date() + ": Retry #" + attemptNumber;
    }
    return log;
  }

  /**
   * Executes the action with given parameters
   */
  private ActionResult executeAction(JSONObject params) {
    // Add information about the current attempt if there are retries
    if (params.has("retry_attempt")) {
      // retry_attempt was already set in parseAndSetupParams
    }

    return AsyncAction.run(config.getActionFactory(), params);
  }

  /**
   * Enriches parameters with action result information
   */
  private JSONObject enrichParamsWithActionResult(JSONObject params, ActionResult result) throws JSONException {
    params = params == null ? new JSONObject() : params;

    addProcessIdToParams(params);
    params.put("message", result.getMessage());

    return params;
  }

  /**
   * Adds process ID to parameters
   */
  private void addProcessIdToParams(JSONObject params) throws JSONException {
    try {
      String actionClassName = config.getActionFactory().get().getClass().getName();
      Process actionObj = (Process) OBDal.getInstance().createCriteria(org.openbravo.client.application.Process.class)
          .add(org.hibernate.criterion.Restrictions.eq("javaClassName", actionClassName))
          .setMaxResults(1)
          .uniqueResult();
      params.put("obuiapp_process_id", actionObj == null ? "" : actionObj.getId());
    } catch (JSONException e) {
      logger.error("Error obtaining action class name: {}", e.getMessage(), e);
      throw new OBException(e);
    }
  }

  /**
   * Updates log with action result
   */
  private String updateLogWithResult(String log, ActionResult result) {
    if (!StringUtils.isEmpty(result.getMessage())) {
      if (!StringUtils.isEmpty(log)) {
        log += "\n";
      }
      log = log + new Date() + ": " + result.getMessage();
    }
    return log;
  }

  /**
   * Sends responses and acknowledges the message
   */
  private void sendResponsesAndAcknowledge(ReceiverRecord<String, AsyncProcessExecution> receiverRecord,
                                          AsyncProcessExecution responseRecord, ReceiverOffset offset,
                                          ActionResult result) {
    if (receiverRecord.topic() != null && !StringUtils.equals(receiverRecord.topic(), "async-process-execution")) {
      createResponse("async-process-execution", config.getKafkaSender(), responseRecord);
    }

    // Acknowledge the message only if no more retries are needed
    offset.acknowledge();

    List<String> targets = extractTargetsFromResult(result);
    for (String tp : targets) {
      createResponse(tp, config.getKafkaSender(), responseRecord);
    }
  }

  /**
   * Restores OB context to previous state
   */
  private void restoreOBContext(ContextInfo contextInfo) {
    // Revert admin mode
    OBContext.restorePreviousMode();
    if (contextInfo.contextChanged) {
      // Restore old context
      OBContext.setOBContext(
          contextInfo.previousUserId,
          contextInfo.previousRoleId,
          contextInfo.previousClientId,
          contextInfo.previousOrgId
      );
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
    targets.add(config.getNextTopic());

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
    if (config.getRetryPolicy() != null && config.getScheduler() != null && config.getRetryPolicy().shouldRetry(attemptNumber + 1)) {
      int nextAttempt = attemptNumber + 1;
      long delay = config.getRetryPolicy().getRetryDelay(nextAttempt);

      logger.debug("Scheduling retry {} for message {} after {} ms",
          nextAttempt, receiverRecord.key(), delay);

      // Do not acknowledge the offset to allow retry later
      config.getScheduler().schedule(() -> processRecord(receiverRecord, nextAttempt), delay, TimeUnit.MILLISECONDS);
    } else {
      // No more retries, send to error topic
      log = log + "\n" + new Date() + ": " + e.getMessage();
      if (attemptNumber > 0) {
        log = log + "\n" + new Date() + ": Max retries reached (" + attemptNumber + ")";
      }

      responseRecord.setLog(log);
      responseRecord.setState(AsyncProcessState.ERROR);

      // Acknowledge the message since we will send to error topic
      receiverRecord.receiverOffset().acknowledge();

      createResponse(config.getErrorTopic(), config.getKafkaSender(), responseRecord);
    }
  }

  /**
   * Sets job parameters
   */
  private void setupJobParams(JSONObject params) throws JSONException {
    if (!params.has("jobs_job_id")) {
      params.put("jobs_job_id", config.getJobId());
    }
    if (!params.has("client_id")) {
      params.put("client_id", config.getClientId());
    }
    if (!params.has("org_id")) {
      params.put("org_id", config.getOrgId());
    }
  }

  /**
   * Creates and sends a response message to the specified topic.
   *
   * @param topic The Kafka topic to send the response to
   * @param kafkaSender The Kafka sender instance
   * @param responseRecord The response record to send
   */
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
          logger.debug("Message {} sent successfully, topic-partition={}-{} offset={}",
              r.correlationMetadata(),
              metadata.topic(),
              metadata.partition(),
              metadata.offset());
        });
  }
}
