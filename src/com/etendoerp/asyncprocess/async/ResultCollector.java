package com.etendoerp.asyncprocess.async;

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
import com.smf.jobs.model.Job;
import com.smf.jobs.model.JobResult;

public class ResultCollector extends AsyncProcessor {
  private static final Logger log = LogManager.getLogger();

  @Override
  public Function<JSONObject, ActionResult> consumer() {
    return value -> {
      log.info("{}", value);
      ActionResult actionResult = new ActionResult();
      if(!value.has("jobs_job_id") || !value.has("message")) {
        actionResult.setType(Result.Type.ERROR);
        actionResult.setMessage("Missing parameters collecting result");
      }
      try {
        OBContext.setOBContext("100", "42D0EEB1C66F497A90DD526DC597E6F0", value.getString("client_id"), value.getString("org_id"));
        JobResult jobResult = new JobResult();
        jobResult.setJobsJob(OBDal.getInstance().get(Job.class, value.getString("jobs_job_id")));
        jobResult.setMessage(value.getString("message"));
        OBDal.getInstance().save(jobResult);
        OBDal.getInstance().flush();
        OBDal.getInstance().commitAndClose();
        actionResult.setType(Result.Type.SUCCESS);
        actionResult.setMessage("Collected");
      } catch (JSONException e) {
        e.printStackTrace();
      }
      return actionResult;
    };
  }
}
