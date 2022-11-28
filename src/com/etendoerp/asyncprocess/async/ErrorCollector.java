package com.etendoerp.asyncprocess.async;

import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;

import com.etendoerp.asyncprocess.AsyncProcessor;
import com.smf.jobs.ActionResult;
import com.smf.jobs.Result;

public class ErrorCollector extends AsyncProcessor {
  private static final Logger log = LogManager.getLogger();

  @Override
  public Function<JSONObject, ActionResult> consumer() {
    return value -> {
      log.info("{}", value);
      ActionResult actionResult = new ActionResult();
      actionResult.setType(Result.Type.ERROR);
      return actionResult;
    };
  }
}
