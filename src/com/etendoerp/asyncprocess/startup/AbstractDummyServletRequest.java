package com.etendoerp.asyncprocess.startup;

import jakarta.servlet.AsyncContext;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.RequestDispatcher;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Locale;
import java.util.Map;

/**
 * First tier of the dummy {@link HttpServletRequest} hierarchy. Implements the core
 * {@link jakarta.servlet.ServletRequest} methods: content, parameters, attributes, network
 * addressing, locale, security, and async lifecycle.
 *
 * <p>Subclasses add Servlet 6.0 extensions and the full HTTP-specific contract.</p>
 */
abstract class AbstractDummyServletRequest implements HttpServletRequest {

  @Override
  public Object getAttribute(String name) {
    return null;
  }

  @Override
  public Enumeration<String> getAttributeNames() {
    return Collections.emptyEnumeration();
  }

  @Override
  public void setAttribute(String name, Object o) {
  }

  @Override
  public void removeAttribute(String name) {
  }

  @Override
  public String getCharacterEncoding() {
    return "UTF-8";
  }

  @Override
  public void setCharacterEncoding(String env) throws UnsupportedEncodingException {
  }

  @Override
  public int getContentLength() {
    return -1;
  }

  @Override
  public long getContentLengthLong() {
    return -1L;
  }

  @Override
  public String getContentType() {
    return "application/json";
  }

  @Override
  public ServletInputStream getInputStream() {
    return null;
  }

  @Override
  public BufferedReader getReader() {
    return null;
  }

  @Override
  public String getParameter(String name) {
    return null;
  }

  @Override
  public Enumeration<String> getParameterNames() {
    return Collections.emptyEnumeration();
  }

  @Override
  public String[] getParameterValues(String name) {
    return null;
  }

  @Override
  public Map<String, String[]> getParameterMap() {
    return Collections.emptyMap();
  }

  @Override
  public String getProtocol() {
    return "HTTP/1.1";
  }

  @Override
  public String getScheme() {
    return "http";
  }

  /** Returns the host name used in the dummy request environment. Default: "localhost". */
  @Override
  public String getServerName() {
    return "localhost";
  }

  /** Returns the server port used in the dummy request environment. Default: 8080. */
  @Override
  public int getServerPort() {
    return 8080;
  }

  /** Returns the remote IP address associated with this dummy request. Default: "127.0.0.1". */
  @Override
  public String getRemoteAddr() {
    return "127.0.0.1";
  }

  @Override
  public String getRemoteHost() {
    return "localhost";
  }

  @Override
  public int getRemotePort() {
    return 0;
  }

  @Override
  public String getLocalAddr() {
    return "127.0.0.1";
  }

  @Override
  public String getLocalName() {
    return "localhost";
  }

  @Override
  public int getLocalPort() {
    return 8080;
  }

  @Override
  public Locale getLocale() {
    return Locale.getDefault();
  }

  @Override
  public Enumeration<Locale> getLocales() {
    return Collections.enumeration(Arrays.asList(Locale.getDefault()));
  }

  @Override
  public boolean isSecure() {
    return false;
  }

  @Override
  public RequestDispatcher getRequestDispatcher(String path) {
    return null;
  }

  @Override
  public ServletContext getServletContext() {
    return null;
  }

  @Override
  public boolean isAsyncStarted() {
    return false;
  }

  @Override
  public boolean isAsyncSupported() {
    return false;
  }

  @Override
  public AsyncContext getAsyncContext() {
    return null;
  }

  @Override
  public DispatcherType getDispatcherType() {
    return DispatcherType.REQUEST;
  }
}
