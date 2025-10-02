package com.etendoerp.asyncprocess.startup;

import org.junit.jupiter.api.Test;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.ServletException;
import java.io.IOException;
import java.util.Collections;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the {@link DummyHttpServletRequest} class.
 * <p>
 * This test class verifies the default behavior of the DummyHttpServletRequest implementation,
 * ensuring that all methods return expected default values or perform no-ops as required by the interface contract.
 * The tests cover HTTP method, headers, session, authentication, parameters, attributes, locale, and async support.
 */
class DummyHttpServletRequestTest {

    public static final String LOCALHOST = "localhost";

    /**
     * Tests the default values and no-op behavior of all methods in DummyHttpServletRequest.
     * <p>
     * This test ensures that the request returns the expected defaults for HTTP method, headers, session,
     * authentication, parameters, attributes, locale, and async support, and that no exceptions are thrown
     * for no-op methods.
     */
    @Test
    void testDefaults() {
        HttpServletRequest req = new DummyHttpServletRequest();
        assertNull(req.getAuthType());
        assertEquals(0, req.getCookies().length);
        assertEquals(-1, req.getDateHeader("any"));
        assertNull(req.getHeader("any"));
        assertFalse(req.getHeaders("any").hasMoreElements());
        assertFalse(req.getHeaderNames().hasMoreElements());
        assertEquals(-1, req.getIntHeader("any"));
        assertEquals("POST", req.getMethod());
        assertNull(req.getPathInfo());
        assertNull(req.getPathTranslated());
        assertEquals("", req.getContextPath());
        assertNull(req.getQueryString());
        assertNull(req.getRemoteUser());
        assertFalse(req.isUserInRole("role"));
        assertNull(req.getUserPrincipal());
        assertNull(req.getRequestedSessionId());
        assertEquals("/async-process", req.getRequestURI());
        assertEquals("http://localhost:8080/etendo", req.getRequestURL().toString());
        assertEquals("/async-process", req.getServletPath());
        assertNull(req.getSession(false));
        assertNull(req.getSession());
        assertNull(req.changeSessionId());
        assertFalse(req.isRequestedSessionIdValid());
        assertFalse(req.isRequestedSessionIdFromCookie());
        assertFalse(req.isRequestedSessionIdFromURL());
        assertFalse(req.isRequestedSessionIdFromUrl());
        assertDoesNotThrow(() -> {
            assertFalse(req.authenticate(null));
        });
        assertDoesNotThrow(() -> req.login("user", "pass"));
        assertDoesNotThrow(req::logout);
        assertDoesNotThrow(() -> req.getParts());
        assertDoesNotThrow(() -> req.getPart("any"));
        assertDoesNotThrow(() -> req.upgrade(null));
        assertNull(req.getAttribute("any"));
        assertFalse(req.getAttributeNames().hasMoreElements());
        assertEquals("application/json", req.getContentType());
        assertEquals("UTF-8", req.getCharacterEncoding());
        assertEquals(-1, req.getContentLength());
        assertEquals(-1L, req.getContentLengthLong());
        assertDoesNotThrow(() -> req.getInputStream());
        assertNull(req.getParameter("any"));
        assertFalse(req.getParameterNames().hasMoreElements());
        assertNull(req.getParameterValues("any"));
        assertEquals(Collections.emptyMap(), req.getParameterMap());
        assertEquals("HTTP/1.1", req.getProtocol());
        assertEquals("http", req.getScheme());
        assertEquals(LOCALHOST, req.getServerName());
        assertEquals(8080, req.getServerPort());
        assertDoesNotThrow(() -> req.getReader());
        assertEquals("127.0.0.1", req.getRemoteAddr());
        assertEquals(LOCALHOST, req.getRemoteHost());
        assertDoesNotThrow(() -> req.setAttribute("a", 1));
        assertDoesNotThrow(() -> req.removeAttribute("a"));
        assertEquals(Locale.getDefault(), req.getLocale());
        assertTrue(req.getLocales().hasMoreElements());
        assertFalse(req.isSecure());
        assertNull(req.getRequestDispatcher("/any"));
        assertNull(req.getRealPath("/any"));
        assertEquals(0, req.getRemotePort());
        assertEquals(LOCALHOST, req.getLocalName());
        assertEquals("127.0.0.1", req.getLocalAddr());
        assertEquals(8080, req.getLocalPort());
        assertNull(req.getServletContext());
        assertDoesNotThrow(() -> req.startAsync());
        assertDoesNotThrow(() -> req.startAsync(null, null));
        assertFalse(req.isAsyncStarted());
        assertFalse(req.isAsyncSupported());
    }
}
