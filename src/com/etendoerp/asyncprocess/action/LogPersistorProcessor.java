package com.etendoerp.asyncprocess.action;

import com.auth0.jwt.interfaces.DecodedJWT;
import com.etendoerp.asyncprocess.data.Log;
import com.etendoerp.asyncprocess.data.LogHeader;
import com.etendoerp.asyncprocess.hooks.LogPersistorIdentifierHook;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.smf.jobs.Action;
import com.smf.jobs.ActionResult;
import com.smf.jobs.Result;
import com.smf.securewebservices.utils.SecureWebServicesUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.openbravo.base.exception.OBException;
import org.openbravo.client.application.Process;
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.service.OBDal;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Iterator;

public class LogPersistorProcessor extends Action {
  private static final Logger log = LogManager.getLogger(LogPersistorProcessor.class);
  public static final String ERROR_PARSING_PARAMS_JSON = "Error parsing params JSON: {}";
  public static final String PARAM_PARAMS = "params";
  public static final String PARAM_TOKEN = "token";
  public static final String PARAM_AFTER = "after";
  public static final String PARAM_OBUIAPP_PROCESS_ID = "obuiapp_process_id";

  @Inject
  @Any
  private Instance<LogPersistorIdentifierHook> hooks;


  @Override
  protected ActionResult action(JSONObject parameters, MutableBoolean isStopped) {
    String asyncProcessId = parameters.optString("asyncProcessId");

    // Extract and set OB context
    ContextInfo contextInfo = extractContextInfo(parameters);
    if (contextInfo != null) {
      try {
        OBContext.setOBContext(contextInfo.userId != null ? contextInfo.userId : "100", contextInfo.roleId, contextInfo.clientId,
            contextInfo.orgId);
      } catch (Exception e) {
        log.error("Error setting OBContext: {}", e.getMessage(), e);
        OBContext.setOBContext("100", "0", "0", "0");
      }
    }

    // Process log header and entry
    LogHeader logHeader = getOrCreateLogHeader(asyncProcessId);
    logHeader.setStatus(parameters.optString("state"));
    OBDal.getInstance().save(logHeader);

    Log logEntry = createLogEntry(parameters, logHeader);
    OBDal.getInstance().save(logEntry);
    OBDal.getInstance().commitAndClose();
    OBDal.getInstance().flush();

    ActionResult actionResult = new ActionResult();
    actionResult.setMessage(parameters.toString());
    actionResult.setType(Result.Type.SUCCESS);
    return actionResult;
  }

  /**
   * Context information extracted from parameters
   */
  private static class ContextInfo {
    final String userId;
    final String roleId;
    final String clientId;
    final String orgId;

    ContextInfo(String userId, String roleId, String clientId, String orgId) {
      this.userId = userId;
      this.roleId = roleId;
      this.clientId = clientId;
      this.orgId = orgId;
    }
  }

  /**
   * Extracts context information (user, role, client, org) from parameters
   */
  private ContextInfo extractContextInfo(JSONObject parameters) {
    try {
      if (!parameters.getString(PARAM_PARAMS).isEmpty()) {
        String params = parameters.getString(PARAM_PARAMS);
        JSONObject paramsJson = new JSONObject(params);
        paramsJson = parseNestedParamsIfExists(paramsJson);

        if (paramsJson.has(PARAM_TOKEN) && !paramsJson.isNull(PARAM_TOKEN)) {
          return extractContextFromToken(paramsJson);
        } else {
          return extractContextFromDebezium(paramsJson);
        }
      }
    } catch (Exception e) {
      log.error(ERROR_PARSING_PARAMS_JSON, e.getMessage(), e);
    }

    return null;
  }

  /**
   * Extracts context information from JWT token
   */
  private ContextInfo extractContextFromToken(JSONObject paramsJson)
      throws JSONException, UnsupportedEncodingException, NoSuchAlgorithmException,
      InvalidKeySpecException, JsonProcessingException {
    String token = paramsJson.getString(PARAM_TOKEN);
    DecodedJWT decodedJWT = SecureWebServicesUtils.decodeToken(token);
    if (decodedJWT != null) {
      String userId = decodedJWT.getClaim("user").asString();
      String roleId = decodedJWT.getClaim("role").asString();
      String orgId = decodedJWT.getClaim("organization").asString();
      String clientId = decodedJWT.getClaim("client").asString();

      validateContextInfo(userId, orgId, clientId);
      return new ContextInfo(userId, roleId, clientId, orgId);
    }
    throw new OBException("Cannot decode JWT token");
  }

  /**
   * Extracts context information from Debezium-style parameters
   */
  private ContextInfo extractContextFromDebezium(JSONObject paramsJson) throws JSONException {
    if (paramsJson.has(PARAM_AFTER) && !paramsJson.isNull(PARAM_AFTER)) {
      JSONObject after = paramsJson.getJSONObject(PARAM_AFTER);
      String userId = after.getString("assigned_user");
      if (userId.isEmpty() || userId.equals("null")) {
        userId = null;
      }
      if (userId == null) {
        userId = after.getString("updatedby");
      }
      String roleId = after.getString("assigned_role");
      if (roleId.isEmpty() || roleId.equals("null")) {
        roleId = null;
      }
      String orgId = after.getString("ad_org_id");
      String clientId = after.getString("ad_client_id");

      if (roleId != null && (roleId.isEmpty() || roleId.equals("null"))) {
        roleId = null;
      }

      return new ContextInfo(userId, roleId, clientId, orgId);
    }
    throw new OBException("Cannot extract context from Debezium parameters");
  }

  /**
   * Validates that essential context information is present
   */
  private void validateContextInfo(String userId, String orgId, String clientId) {
    if (userId == null || orgId == null || clientId == null) {
      throw new OBException("Cannot set OBContext. Missing user, role, org or client");
    }
  }

  /**
   * Gets existing log header or creates a new one
   */
  private LogHeader getOrCreateLogHeader(String asyncProcessId) {
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
          if (hook.identifyLogRecord(logHeader)) {
            break;
          }
        }
      }
    }
    return logHeader;
  }

  /**
   * Creates a log entry with message and process information
   */
  private Log createLogEntry(JSONObject parameters, LogHeader logHeader) {
    Log logEntry = new Log();
    logEntry.setEtapLogHeader(logHeader);

    String description = getLogDescription(parameters);
    logEntry.setMessage(prettyDescription(description));

    String obuiappProcessId = getObuiappProcessId(parameters);
    if (obuiappProcessId != null) {
      logEntry.setObuiappProcess(OBDal.getInstance().get(Process.class, obuiappProcessId));
    }

    logEntry.setLineno((logHeader.getETAPLogList().size() + 1) * 10L);
    return logEntry;
  }

  /**
   * Gets log description from parameters
   */
  private String getLogDescription(JSONObject parameters) {
    String description = parameters.optString("log");
    if (StringUtils.isEmpty(description) || description.equals("0")) {
      description = parameters.optString(PARAM_PARAMS);
    }
    return description;
  }

  /**
   * Gets obuiapp process ID from parameters
   */
  private String getObuiappProcessId(JSONObject parameters) {
    if (parameters.has(PARAM_OBUIAPP_PROCESS_ID) && !parameters.isNull(PARAM_OBUIAPP_PROCESS_ID)) {
      return parameters.optString(PARAM_OBUIAPP_PROCESS_ID);
    }
    return extractObuiappProcessId(parameters);
  }

  /**
   * Extracts the obuiapp_process_id from the parameters JSON, handling nested params structure.
   *
   * @param parameters the main parameters JSONObject
   * @return the extracted obuiapp_process_id or null if not found or parsing fails
   */
  private String extractObuiappProcessId(JSONObject parameters) {
    if (!parameters.has(PARAM_PARAMS) || parameters.isNull(PARAM_PARAMS)) {
      return null;
    }

    try {
      JSONObject paramsJson = new JSONObject(parameters.getString(PARAM_PARAMS));

      // First level: check if obuiapp_process_id exists directly
      if (paramsJson.has(PARAM_OBUIAPP_PROCESS_ID) && !paramsJson.isNull(PARAM_OBUIAPP_PROCESS_ID)) {
        return paramsJson.optString(PARAM_OBUIAPP_PROCESS_ID);
      }

      // Second level: check nested params structure
      paramsJson = parseNestedParamsIfExists(paramsJson);
      return extractFromNestedParams(paramsJson);

    } catch (Exception e) {
      log.error(ERROR_PARSING_PARAMS_JSON, e.getMessage(), e);
    }

    return null;
  }

  /**
   * Extracts obuiapp_process_id from nested params structure.
   *
   * @param paramsJson the params JSONObject that may contain nested params
   * @return the extracted obuiapp_process_id or null if not found or parsing fails
   */
  private String extractFromNestedParams(JSONObject paramsJson) {
    try {
      JSONObject nestedParamsJson = new JSONObject(paramsJson.getString(PARAM_PARAMS));
      return nestedParamsJson.optString(PARAM_OBUIAPP_PROCESS_ID);
    } catch (Exception e) {
      log.error(ERROR_PARSING_PARAMS_JSON, e.getMessage(), e);
      return null;
    }
  }

  /**
   * Parses nested params JSON object if it exists.
   *
   * @param paramsJson the JSONObject that may contain nested params
   * @return the nested JSONObject if parsing succeeds, or the original paramsJson if no nested params or parsing fails
   */
  private JSONObject parseNestedParamsIfExists(JSONObject paramsJson) {
    if (paramsJson.has(PARAM_PARAMS)) {
      try {
        JSONObject nestedParams = new JSONObject(paramsJson.getString(PARAM_PARAMS));
        return parseNestedParamsIfExists(nestedParams);
      } catch (Exception e) {
        log.error(ERROR_PARSING_PARAMS_JSON, e.getMessage(), e);
      }
    }
    return paramsJson;
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
        for (Iterator<String> it = json.keys(); it.hasNext(); ) {
          String key = it.next();
          String value = json.getString(key);
          // value could be a json, format it recursively adding spaces on each recursion
          if (value.startsWith("{") && value.endsWith("}")) {
            value = prettyDescription(value).replace("\n", "\n  ");
          }
          pretty
              .append(key.toUpperCase()).append(": ").append(value).append("\n");
        }
      } catch (Exception e) {
        log.error("Error parsing log description JSON: {}", e.getMessage(), e);
        return description;
      }
    }
    if (StringUtils.isEmpty(pretty.toString())) {
      return description;
    }
    return pretty.toString();
  }

  @Override
  protected Class<?> getInputClass() {
    return JSONObject.class;
  }
}
