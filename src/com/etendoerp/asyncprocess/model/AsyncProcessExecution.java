package com.etendoerp.asyncprocess.model;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;


public class AsyncProcessExecution implements Comparable<AsyncProcessExecution> {

  private String id;
  private String asyncProcessId;
  private String log;
  private String description;
  private String params;

  @JsonFormat(shape = JsonFormat.Shape.STRING,
      pattern = "dd-MM-yyyy hh:mm:ss")
  private Date time;
  private AsyncProcessState state = AsyncProcessState.ACCEPTED;

  @Override
  public int compareTo(AsyncProcessExecution o) {
    var r = o.time.compareTo(this.time);
    if (r == 0) return o.id == null ? -1 : o.id.compareTo(this.id);
    return r;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getAsyncProcessId() {
    return asyncProcessId;
  }

  public void setAsyncProcessId(String asyncProcessId) {
    this.asyncProcessId = asyncProcessId;
  }

  public String getLog() {
    return log;
  }

  public void setLog(String log) {
    this.log = log;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getParams() {
    return params;
  }

  public void setParams(String params) {
    this.params = params;
  }

  public Date getTime() {
    return time;
  }

  public void setTime(Date time) {
    this.time = time;
  }

  public AsyncProcessState getState() {
    return state;
  }

  public void setState(AsyncProcessState state) {
    this.state = state;
  }
}
