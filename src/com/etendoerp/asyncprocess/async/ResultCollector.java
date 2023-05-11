package com.etendoerp.asyncprocess.async;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.service.OBDal;

import com.smf.jobs.Action;
import com.smf.jobs.ActionResult;
import com.smf.jobs.Result;
import com.smf.jobs.model.Job;
import com.smf.jobs.model.JobResult;

public class ResultCollector extends Action {
  private static final Logger log = LogManager.getLogger();

  @Override
  protected ActionResult action(JSONObject parameters, MutableBoolean isStopped) {
    log.info("{}", parameters);
    ActionResult actionResult = new ActionResult();
    if (!parameters.has("jobs_job_id") || !parameters.has("message")) {
      actionResult.setType(Result.Type.ERROR);
      actionResult.setMessage("Missing parameters collecting result");
    }
    try {
      OBContext.setOBContext("100", "42D0EEB1C66F497A90DD526DC597E6F0", parameters.getString("client_id"),
          parameters.getString("org_id"));
      JobResult jobResult = new JobResult();
      jobResult.setJobsJob(OBDal.getInstance().get(Job.class, parameters.getString("jobs_job_id")));
      jobResult.setMessage(parameters.getString("message"));
      OBDal.getInstance().save(jobResult);
      OBDal.getInstance().flush();
      OBDal.getInstance().commitAndClose();
      actionResult.setType(Result.Type.SUCCESS);
      actionResult.setMessage("Collected");
    } catch (JSONException e) {
      log.error("Result Collection Exception", e);
      actionResult.setType(Result.Type.ERROR);
      actionResult.setMessage(e.getMessage());
    }
    return actionResult;
  }

  @Override
  protected Class<Job> getInputClass() {
    return Job.class;
  }
}
