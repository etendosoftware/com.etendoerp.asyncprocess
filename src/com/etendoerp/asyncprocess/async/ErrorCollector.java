package com.etendoerp.asyncprocess.async;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;

import com.smf.jobs.Action;
import com.smf.jobs.ActionResult;
import com.smf.jobs.Result;

/**
 * Action implementation used to collect and record error payloads produced by asynchronous
 * processes. This action receives a JSON object as input and converts the execution result
 * into an error result so that calling frameworks or orchestration layers recognize the
 * invocation as failed.
 *
 * <p>Current implementation simply logs the provided JSON payload and returns an
 * ActionResult with type Result.Type.ERROR. It can be extended to persist error details,
 * send notifications, or enrich the result with additional metadata.</p>
 */
public class ErrorCollector extends Action {
  private static final Logger log = LogManager.getLogger();

  /**
   * Handle the action execution.
   *
   * <p>This method is invoked by the job framework with the parsed input parameters
   * (a JSONObject) and a MutableBoolean flag which can be used to request early
   * termination of a longer-running action. The current implementation logs the
   * parameters and returns an ActionResult marked as ERROR.</p>
   *
   * @param parameters JSON object containing the payload or error details provided by the caller
   * @param isStopped mutable flag that may indicate the action should stop early; not used here
   * @return ActionResult instance with Result.Type.ERROR to indicate the action produced an error outcome
   */
  @Override
  protected ActionResult action(JSONObject parameters, MutableBoolean isStopped) {
    log.info("{}", parameters);
    ActionResult actionResult = new ActionResult();
    actionResult.setType(Result.Type.ERROR);
    return actionResult;
  }

  /**
   * Return the expected input class for this action.
   *
   * <p>The job framework uses this to parse and validate incoming parameters before
   * invoking {@link #action(JSONObject, MutableBoolean)}. This implementation expects
   * a {@link JSONObject} as input.</p>
   *
   * @return Class representing the expected input type (JSONObject.class)
   */
  @Override
  protected Class<?> getInputClass() {
    return JSONObject.class;
  }
}
