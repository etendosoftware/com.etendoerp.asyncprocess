package com.etendoerp.asyncprocess.startup;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.security.Principal;
import java.util.Enumeration;

/**
 * A dummy implementation of HttpServletRequest for async processing context.
 * This class provides minimal implementations of the methods in HttpServletRequest.
 */
class DummyHttpServletRequest implements HttpServletRequest {
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
    return java.util.Collections.emptyEnumeration();
  }

  @Override
  public Enumeration<String> getHeaderNames() {
    return java.util.Collections.emptyEnumeration();
  }

  @Override
  public int getIntHeader(String name) {
    return -1;
  }

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

  @Override
  public String getRequestedSessionId() {
    return null;
  }

  @Override
  public String getRequestURI() {
    return "/async-process";
  }

  @Override
  public StringBuffer getRequestURL() {
    return new StringBuffer("http://localhost:8080/etendo");
  }

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
  public java.util.Collection<javax.servlet.http.Part> getParts() {
    return java.util.Collections.emptyList();
  }

  @Override
  public javax.servlet.http.Part getPart(String name) {
    return null;
  }

  @Override
  public <T extends javax.servlet.http.HttpUpgradeHandler> T upgrade(Class<T> handlerClass) {
    return null;
  }

  @Override
  public Object getAttribute(String name) {
    return null;
  }

  @Override
  public Enumeration<String> getAttributeNames() {
    return java.util.Collections.emptyEnumeration();
  }

  @Override
  public String getCharacterEncoding() {
    return "UTF-8";
  }

  @Override
  public void setCharacterEncoding(String env) {
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
  public javax.servlet.ServletInputStream getInputStream() {
    return null;
  }

  @Override
  public String getParameter(String name) {
    return null;
  }

  @Override
  public Enumeration<String> getParameterNames() {
    return java.util.Collections.emptyEnumeration();
  }

  @Override
  public String[] getParameterValues(String name) {
    return null;
  }

  @Override
  public java.util.Map<String, String[]> getParameterMap() {
    return java.util.Collections.emptyMap();
  }

  @Override
  public String getProtocol() {
    return "HTTP/1.1";
  }

  @Override
  public String getScheme() {
    return "http";
  }

  @Override
  public String getServerName() {
    return "localhost";
  }

  @Override
  public int getServerPort() {
    return 8080;
  }

  @Override
  public java.io.BufferedReader getReader() {
    return null;
  }

  @Override
  public String getRemoteAddr() {
    return "127.0.0.1";
  }

  @Override
  public String getRemoteHost() {
    return "localhost";
  }

  @Override
  public void setAttribute(String name, Object o) {
  }

  @Override
  public void removeAttribute(String name) {
  }

  @Override
  public java.util.Locale getLocale() {
    return java.util.Locale.getDefault();
  }

  @Override
  public Enumeration<java.util.Locale> getLocales() {
    return java.util.Collections.enumeration(
        java.util.Arrays.asList(java.util.Locale.getDefault()));
  }

  @Override
  public boolean isSecure() {
    return false;
  }

  @Override
  public javax.servlet.RequestDispatcher getRequestDispatcher(String path) {
    return null;
  }

  @Override
  public String getRealPath(String path) {
    return null;
  }

  @Override
  public int getRemotePort() {
    return 0;
  }

  @Override
  public String getLocalName() {
    return "localhost";
  }

  @Override
  public String getLocalAddr() {
    return "127.0.0.1";
  }

  @Override
  public int getLocalPort() {
    return 8080;
  }

  @Override
  public javax.servlet.ServletContext getServletContext() {
    return null;
  }

  @Override
  public javax.servlet.AsyncContext startAsync() {
    return null;
  }

  @Override
  public javax.servlet.AsyncContext startAsync(javax.servlet.ServletRequest servletRequest,
      javax.servlet.ServletResponse servletResponse) {
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
  public javax.servlet.AsyncContext getAsyncContext() {
    return null;
  }

  @Override
  public javax.servlet.DispatcherType getDispatcherType() {
    return javax.servlet.DispatcherType.REQUEST;
  }
}
