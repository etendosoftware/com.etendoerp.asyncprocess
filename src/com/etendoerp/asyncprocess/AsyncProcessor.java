package com.etendoerp.asyncprocess;

import java.util.Map;
import java.util.function.Function;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.codehaus.jettison.json.JSONObject;

import com.smf.jobs.Action;
import com.smf.jobs.ActionResult;
import com.smf.jobs.Result;

public abstract class AsyncProcessor extends Action {
  public abstract Function<JSONObject, ActionResult> consumer();

  protected ActionResult action(JSONObject parameters, MutableBoolean isStopped) {
    var result = consumer().apply(parameters);
    var actionResult = new ActionResult();
    actionResult.setMessage(result.toString());
    actionResult.setType(Result.Type.SUCCESS);
    return actionResult;
  }

  @Override
  protected Class<?> getInputClass() {
    return Map.class;
  }
}
