package com.etendoerp.asyncprocess.async;

import java.util.function.Function;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;

import com.etendoerp.asyncprocess.AsyncProcessor;
import com.smf.jobs.Action;
import com.smf.jobs.ActionResult;
import com.smf.jobs.Result;

public class ErrorCollector extends Action {
  private static final Logger log = LogManager.getLogger();

  @Override
  protected ActionResult action(JSONObject parameters, MutableBoolean isStopped) {
    log.info("{}", parameters);
    ActionResult actionResult = new ActionResult();
    actionResult.setType(Result.Type.ERROR);
    return actionResult;
  }

  @Override
  protected Class<?> getInputClass() {
    return JSONObject.class;
  }
}
