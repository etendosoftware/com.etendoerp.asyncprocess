package com.etendoerp.asyncprocess.async;

import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.service.OBDal;
import org.openbravo.model.ad.access.Role;
import org.openbravo.model.ad.alert.Alert;
import org.openbravo.model.ad.alert.AlertRule;

import com.etendoerp.asyncprocess.AsyncProcessor;
import com.smf.jobs.ActionResult;
import com.smf.jobs.Result;

public class AlertProcess extends AsyncProcessor {
  private static final Logger log = LogManager.getLogger();

  @Override
  public Function<JSONObject, ActionResult> consumer() {
    return value -> {
      log.info("{}", value);
      ActionResult actionResult = new ActionResult();
      try {
        OBContext.setOBContext("100", "42D0EEB1C66F497A90DD526DC597E6F0", "23C59575B9CF467C9620760EB255B389", "0");
        Alert alert = new Alert();
        Role role = OBContext.getOBContext().getRole();
        alert.setDescription((String) value.get("description"));
        alert.setAlertRule(OBDal.getInstance().get(AlertRule.class, "57CC65EA1D9C47E9BA20E09771004802"));
        alert.setReferenceSearchKey("");
        alert.setRole(role);
        OBDal.getInstance().save(alert);
        OBDal.getInstance().flush();
        OBDal.getInstance().commitAndClose();
        actionResult.setType(Result.Type.SUCCESS);
        actionResult.setMessage("Alert created");
      } catch (JSONException e) {
        e.printStackTrace();
        actionResult.setType(Result.Type.ERROR);
        actionResult.setMessage(e.getMessage());
      }
      return actionResult;
    };
  }
}
