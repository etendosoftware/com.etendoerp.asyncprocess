package com.smf.jobs;

import java.util.function.Supplier;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.codehaus.jettison.json.JSONObject;
import org.openbravo.base.structure.BaseOBObject;

public class AsyncAction {
  public static ActionResult run(Supplier<Action> actionFactory, JSONObject params) {
    var action = actionFactory.get();
    return action.action(params, new MutableBoolean(false));
  }
}
