package com.etendoerp.asyncprocess.action;

import com.auth0.jwt.interfaces.DecodedJWT;
import com.etendoerp.asyncprocess.data.Log;
import com.etendoerp.asyncprocess.data.LogHeader;
import com.etendoerp.asyncprocess.hooks.LogPersistorIdentifierHook;
import com.etendoerp.task.action.TaskTypeMatchJob;
import com.smf.jobs.Action;
import com.smf.jobs.ActionResult;
import com.smf.jobs.Result;
import com.smf.securewebservices.utils.SecureWebServicesUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;
import org.openbravo.base.exception.OBException;
import org.openbravo.client.application.Process;
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.service.OBDal;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.Iterator;

public class LogPersistorProcessor extends Action {
  private static final Logger log = LogManager.getLogger(LogPersistorProcessor.class);
  @Inject
  @Any
  private Instance<LogPersistorIdentifierHook> hooks;


  @Override
  protected ActionResult action(JSONObject parameters, MutableBoolean isStopped) {
    ActionResult actionResult = new ActionResult();
    String asyncProcessId = parameters.optString("asyncProcessId");
    String userId = null;
    String roleId = null;
    String orgId = null;
    String clientId = null;
    try {
      if (!parameters.getString("params").isEmpty()) {
        String params = parameters.getString("params");
        JSONObject paramsJson = new JSONObject(params);
        if (paramsJson.has("params")) {
          try {
            paramsJson = new JSONObject(paramsJson.getString("params"));
          } catch (Exception e) {
            log.error("Error parsing params JSON: " + e.getMessage(), e);
          }
        }
        // Has token
        if (paramsJson.has("token") && !paramsJson.isNull("token")) {
          String token = paramsJson.getString("token");
          DecodedJWT decodedJWT = SecureWebServicesUtils.decodeToken(token);
          if (decodedJWT != null) {
            // Set OBContext
            userId = decodedJWT.getClaim("user").asString();
            roleId = decodedJWT.getClaim("role").asString();
            orgId = decodedJWT.getClaim("organization").asString();
            clientId = decodedJWT.getClaim("client").asString();
          }
        } else {
          // Debezium style
          if (paramsJson.has("after") && !paramsJson.isNull("after")) {
            JSONObject after = paramsJson.getJSONObject("after");
            userId = after.getString("assigned_user");
            roleId = after.getString("assigned_role");
            orgId = after.getString("ad_org_id");
            clientId = after.getString("ad_client_id");

            if (roleId.isEmpty() || roleId.equals("null")) {
              roleId = null;
            }
          }
        }
      }
    } catch (Exception e) {
      log.error("Error parsing params JSON: " + e.getMessage(), e);
    }
    if (userId == null || orgId == null || clientId == null) {
      log.error("Cannot process LogPersistorProcessor. Missing user, org or client. With Params: " + parameters);
      throw new OBException("Cannot set OBContext. Missing user, role, org or client");
    }
    OBContext.setOBContext(userId, roleId, clientId, orgId);

    LogHeader logHeader = (LogHeader) OBDal.getInstance()
        .createCriteria(LogHeader.class)
        .add(org.hibernate.criterion.Restrictions.eq(LogHeader.PROPERTY_PROCESS, asyncProcessId))
        .setMaxResults(1)
        .uniqueResult();
    if (logHeader == null) {
      logHeader = new LogHeader();
      logHeader.setProcess(asyncProcessId);
      if (hooks != null) {
        for (LogPersistorIdentifierHook hook : hooks) {
          if(hook.identifyLogRecord(logHeader)) {
            break;
          }
        }
      }
    }
    logHeader.setStatus(parameters.optString("state"));
    OBDal.getInstance().save(logHeader);
    Log logEntry = new Log();
    logEntry.setEtapLogHeader(logHeader);
    String description = parameters.optString("log");
    if (StringUtils.isEmpty(description) || description.equals("0")) {
      description = parameters.optString("params");
    }
    logEntry.setMessage(prettyDescription(description));
    String obuiappProcessId = null;
    if (parameters.has("obuiapp_process_id") && !parameters.isNull("obuiapp_process_id")) {
      obuiappProcessId = parameters.optString("obuiapp_process_id");
    }
    if (obuiappProcessId == null && parameters.has("params") && !parameters.isNull("params")) {
      JSONObject paramsJson = null;
      try {
        paramsJson = new JSONObject(parameters.getString("params"));
        if (paramsJson.has("obuiapp_process_id") && !paramsJson.isNull("obuiapp_process_id")) {
          obuiappProcessId = paramsJson.optString("obuiapp_process_id");
        }
        if (obuiappProcessId == null && paramsJson.has("params")) {
          try {
            paramsJson = new JSONObject(paramsJson.getString("params"));
            obuiappProcessId = paramsJson.optString("obuiapp_process_id");
          } catch (Exception e) {
            log.error("Error parsing params JSON: " + e.getMessage(), e);
          }
        }
      } catch (Exception e) {
        log.error("Error parsing params JSON: " + e.getMessage(), e);
      }
    }
    if (obuiappProcessId != null) {
      logEntry.setObuiappProcess(
        OBDal.getInstance().get(Process.class, obuiappProcessId));
    }
    logEntry.setLineno((logHeader.getETAPLogList().size() + 1) * 10L);
    OBDal.getInstance().save(logEntry);
    OBDal.getInstance().commitAndClose();
    OBDal.getInstance().flush();
    actionResult.setMessage(parameters.toString());
    actionResult.setType(Result.Type.SUCCESS);
    return actionResult;
  }

  private String prettyDescription(String description) {
    String jsonPart = null;
    String datePart = null;
    StringBuilder pretty = new StringBuilder();
    if (description.contains(": {")) {
      int index = description.indexOf(": {");
      datePart = description.substring(0, index + 1).trim();
      jsonPart = description.substring(index + 2).trim();
    }
    if (description.startsWith("{") && description.endsWith("}")) {
      jsonPart = description;
    }
    if (jsonPart != null) {
      try {
        JSONObject json = new JSONObject(jsonPart);
        pretty.append(datePart).append("\n");
        for (Iterator it = json.keys(); it.hasNext(); ) {
          String key = it.next().toString();
          String value = json.getString(key);
          // value could be a json, format it recursively adding spaces on each recursion
          if (value.startsWith("{") && value.endsWith("}")) {
            value = prettyDescription(value).replace("\n", "\n  ");
          }
          pretty
              .append(key.toUpperCase()).append(": ").append(value).append("\n");
        }
      } catch (Exception e) {
        log.error("Error parsing log description JSON: " + e.getMessage(), e);
        return description;
      }
    }
    return pretty.toString();
  }

  @Override
  protected Class<?> getInputClass() {
    return JSONObject.class;
  }
}
