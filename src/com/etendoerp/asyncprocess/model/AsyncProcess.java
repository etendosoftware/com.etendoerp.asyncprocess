package com.etendoerp.asyncprocess.model;

import java.util.Date;
import java.util.SortedSet;
import java.util.TreeSet;

import com.fasterxml.jackson.annotation.JsonFormat;


public class AsyncProcess {

  private String id;
  @JsonFormat(shape = JsonFormat.Shape.STRING,
      pattern = "dd-MM-yyyy hh:mm:ss")
  private Date lastUpdate;
  private String description;
  private AsyncProcessState state = AsyncProcessState.WAITING;
  private SortedSet<AsyncProcessExecution> executions = new TreeSet<>();

  public AsyncProcess process(AsyncProcessExecution asyncProcessExecution) {
    addExecution(asyncProcessExecution);
    this.id = asyncProcessExecution.getAsyncProcessId();
    this.lastUpdate = asyncProcessExecution.getTime();
    this.state = asyncProcessExecution.getState();
    return this;
  }

  private void addExecution(AsyncProcessExecution transactionClone) {
    executions.add(transactionClone);
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Date getLastUpdate() {
    return lastUpdate;
  }

  public void setLastUpdate(Date lastUpdate) {
    this.lastUpdate = lastUpdate;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public AsyncProcessState getState() {
    return state;
  }

  public void setState(AsyncProcessState state) {
    this.state = state;
  }

  public SortedSet<AsyncProcessExecution> getExecutions() {
    return executions;
  }

  public void setExecutions(SortedSet<AsyncProcessExecution> executions) {
    this.executions = executions;
  }
}
