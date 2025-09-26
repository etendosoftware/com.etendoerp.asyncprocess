package com.etendoerp.asyncprocess.action;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;

import javax.enterprise.inject.Instance;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.codehaus.jettison.json.JSONObject;
import org.hibernate.criterion.Criterion;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.service.OBCriteria;
import org.openbravo.dal.service.OBDal;

import com.etendoerp.asyncprocess.data.LogHeader;
import com.smf.jobs.ActionResult;

/**
 * Unit tests for the {@link LogPersistorProcessor} class.
 * <p>
 * This test class uses Mockito and JUnit 5 to mock dependencies and verify the behavior of the LogPersistorProcessor.
 * It focuses on testing the action method with minimal parameters and ensures that the processor works as expected
 * when interacting with mocked dependencies such as OBDal, OBContext, and hooks.
 */
class LogPersistorProcessorTest {

  /**
   * Mocked CDI Instance for hooks injection.
   */
  @Mock
  private Instance hooks;

  /**
   * The processor under test, with mocks injected.
   */
  @InjectMocks
  private LogPersistorProcessor processor;

  /**
   * Initializes mocks and injects the mock hooks instance into the processor before each test.
   *
   * @throws Exception
   *     if reflection fails
   */
  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    processor = new LogPersistorProcessor();
    // Inject the mock into the private field using reflection
    Field hooksField = LogPersistorProcessor.class.getDeclaredField("hooks");
    hooksField.setAccessible(true);
    hooksField.set(processor, hooks);
  }

  /**
   * Tests the action method of LogPersistorProcessor with minimal parameters.
   * <p>
   * This test mocks the required static and instance methods to avoid NullPointerExceptions and verifies
   * that the action method returns a non-null result when provided with minimal valid input.
   *
   * @throws Exception
   *     if any error occurs during the test
   */
  @Test
  void testActionWithMinimalParams() throws Exception {
    // Mock hooks.iterator() to avoid NullPointerException
    java.util.Iterator mockIterator = mock(java.util.Iterator.class);
    when(hooks.iterator()).thenReturn(mockIterator);
    when(mockIterator.hasNext()).thenReturn(false);

    // Build the params JSON as a nested string for Debezium
    JSONObject after = new JSONObject();
    after.put("assigned_user", "100");
    after.put("assigned_role", "0");
    after.put("ad_client_id", "0");
    after.put("ad_org_id", "0");
    JSONObject debeziumParams = new JSONObject();
    debeziumParams.put("after", after);
    JSONObject params = new JSONObject();
    params.put("asyncProcessId", "test-id");
    params.put("params", debeziumParams.toString());
    params.put("state", "SUCCESS");
    MutableBoolean isStopped = new MutableBoolean(false);

    // Static mock for OBContext and OBDal
    try (MockedStatic<OBContext> obContextMock = org.mockito.Mockito.mockStatic(OBContext.class);
         MockedStatic<OBDal> obDalMock = org.mockito.Mockito.mockStatic(OBDal.class)) {
      OBContext obContext = mock(OBContext.class);
      obContextMock.when(OBContext::getOBContext).thenReturn(obContext);
      obContextMock.when(() -> OBContext.setOBContext(anyString(), anyString(), anyString(), anyString())).then(
          invocation -> null);
      when(obContext.isInAdministratorMode()).thenReturn(true);

      OBDal obDal = mock(OBDal.class);
      obDalMock.when(OBDal::getInstance).thenReturn(obDal);
      doNothing().when(obDal).save(any());
      doNothing().when(obDal).commitAndClose();
      doNothing().when(obDal).flush();
      com.etendoerp.asyncprocess.data.LogHeader logHeaderSpy = spy(new com.etendoerp.asyncprocess.data.LogHeader());
      doReturn(new java.util.ArrayList<>()).when(logHeaderSpy).getETAPLogList();
      doNothing().when(obDal).save(logHeaderSpy);

      com.etendoerp.asyncprocess.data.Log logSpy = spy(new com.etendoerp.asyncprocess.data.Log());
      doNothing().when(obDal).save(logSpy);

      OBCriteria<LogHeader> obCriteria = mock(OBCriteria.class);
      when(obDal.createCriteria(eq(LogHeader.class))).thenReturn(obCriteria);
      when(obCriteria.add(any(Criterion.class))).thenReturn(obCriteria);
      when(obCriteria.setMaxResults(anyInt())).thenReturn(obCriteria);
      when(obCriteria.uniqueResult()).thenReturn(null);

      ActionResult result = processor.action(params, isStopped);
      assertNotNull(result);
    }
  }
}
