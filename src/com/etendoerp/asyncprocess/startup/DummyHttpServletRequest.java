package com.etendoerp.asyncprocess.startup;

import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import jakarta.servlet.http.HttpUpgradeHandler;
import jakarta.servlet.http.Part;
import java.util.Collections;

/**
 * Lightweight dummy implementation of {@link jakarta.servlet.http.HttpServletRequest} intended
 * for use in asynchronous/background contexts where a full servlet request is not available.
 *
 * <p>This class provides safe, minimal default values for commonly used methods so that code
 * which expects an {@code HttpServletRequest} can execute without a live HTTP request — for
 * example when constructing messages or simulating a request in asynchronous processing.</p>
 *
 * <p>Behavior and limitations:
 * <ul>
 *   <li>Most mutation and session-related operations are no-ops and return null or empty
 *       collections.</li>
 *   <li>Default values are intentionally simple and deterministic (e.g. {@link #getMethod()}
 *       returns "POST", {@link #getRequestURI()} returns "/async-process").</li>
 *   <li>This class is not a full-featured implementation and must not be used for real HTTP
 *       request processing.</li>
 * </ul>
 * </p>
 *
 * <p>Implementation is split across three classes to comply with the 35-method class limit:
 * {@link AbstractDummyServletRequest} → {@link AbstractDummyHttpServletRequest} →
 * {@code DummyHttpServletRequest}.</p>
 */
class DummyHttpServletRequest extends AbstractDummyHttpServletRequest {

  @Override
  public String getRequestedSessionId() {
    return null;
  }

  /** Returns a fixed request URI used by asynchronous processing. Default: "/async-process". */
  @Override
  public String getRequestURI() {
    return "/async-process";
  }

  /**
   * Returns a simple request URL representation.
   * Default: "http://localhost:8080/etendo".
   */
  @Override
  public StringBuffer getRequestURL() {
    return new StringBuffer("http://localhost:8080/etendo");
  }

  /** Returns the servlet path for the simulated request. Default: "/async-process". */
  @Override
  public String getServletPath() {
    return "/async-process";
  }

  @Override
  public HttpSession getSession(boolean create) {
    return null;
  }

  @Override
  public HttpSession getSession() {
    return null;
  }

  @Override
  public String changeSessionId() {
    return null;
  }

  @Override
  public boolean isRequestedSessionIdValid() {
    return false;
  }

  @Override
  public boolean isRequestedSessionIdFromCookie() {
    return false;
  }

  @Override
  public boolean isRequestedSessionIdFromURL() {
    return false;
  }

  @Override
  public boolean isRequestedSessionIdFromUrl() {
    return false;
  }

  @Override
  public boolean authenticate(HttpServletResponse response) {
    return false;
  }

  @Override
  public void login(String username, String password) {
  }

  @Override
  public void logout() {
  }

  @Override
  public java.util.Collection<Part> getParts() {
    return Collections.emptyList();
  }

  @Override
  public Part getPart(String name) {
    return null;
  }

  @Override
  public <T extends HttpUpgradeHandler> T upgrade(Class<T> handlerClass) {
    return null;
  }
}
