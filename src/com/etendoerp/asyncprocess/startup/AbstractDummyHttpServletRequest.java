package com.etendoerp.asyncprocess.startup;

import jakarta.servlet.AsyncContext;
import jakarta.servlet.ServletConnection;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import java.security.Principal;
import java.util.Collections;
import java.util.Enumeration;

/**
 * Second tier of the dummy {@link HttpServletRequest} hierarchy. Extends
 * {@link AbstractDummyServletRequest} with async startup methods, Servlet 6.0 connection
 * introspection, and the HTTP request-line / header / identity methods.
 *
 * <p>{@link DummyHttpServletRequest} completes the contract by adding session and
 * authentication methods.</p>
 */
abstract class AbstractDummyHttpServletRequest extends AbstractDummyServletRequest {

  @Override
  public AsyncContext startAsync() {
    return null;
  }

  @Override
  public AsyncContext startAsync(jakarta.servlet.ServletRequest servletRequest,
      jakarta.servlet.ServletResponse servletResponse) {
    return null;
  }

  @Override
  public String getRequestId() {
    return "";
  }

  @Override
  public String getProtocolRequestId() {
    return "";
  }

  @Override
  public ServletConnection getServletConnection() {
    return null;
  }

  @Override
  public String getAuthType() {
    return null;
  }

  @Override
  public Cookie[] getCookies() {
    return new Cookie[0];
  }

  @Override
  public long getDateHeader(String name) {
    return -1;
  }

  @Override
  public String getHeader(String name) {
    return null;
  }

  @Override
  public Enumeration<String> getHeaders(String name) {
    return Collections.emptyEnumeration();
  }

  @Override
  public Enumeration<String> getHeaderNames() {
    return Collections.emptyEnumeration();
  }

  @Override
  public int getIntHeader(String name) {
    return -1;
  }

  /** Returns the simulated HTTP method. Default: "POST". */
  @Override
  public String getMethod() {
    return "POST";
  }

  @Override
  public String getPathInfo() {
    return null;
  }

  @Override
  public String getPathTranslated() {
    return null;
  }

  /** Returns the servlet context path for the simulated request. Default: empty string. */
  @Override
  public String getContextPath() {
    return "";
  }

  @Override
  public String getQueryString() {
    return null;
  }

  @Override
  public String getRemoteUser() {
    return null;
  }

  @Override
  public boolean isUserInRole(String role) {
    return false;
  }

  @Override
  public Principal getUserPrincipal() {
    return null;
  }
}
