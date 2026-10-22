package com.etendoerp.asyncprocess.startup;

import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.KAFKA_ENABLE_KEY;
import static com.etendoerp.asyncprocess.AsyncProcessTestConstants.KAFKA_ENABLE_VALUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.openbravo.base.exception.OBException;
import org.openbravo.base.session.OBPropertiesProvider;
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.service.OBCriteria;
import org.openbravo.dal.service.OBDal;

import com.smf.jobs.model.Job;

/**
 * Verifies startup eligibility and informational logging without connecting to Kafka.
 */
@ExtendWith(MockitoExtension.class)
class AsyncProcessStartupEligibilityTest {
  @Mock
  private OBDal mockOBDal;
  @Mock
  private OBCriteria<Job> mockCriteria;
  @Mock
  private Job mockJob;
  @Mock
  private OBPropertiesProvider mockPropertiesProvider;
  @Mock
  private Properties mockProperties;
  private final AsyncProcessStartup asyncProcessStartup = new AsyncProcessStartup();
  private MockedStatic<OBDal> mockedOBDal;
  private MockedStatic<OBContext> mockedOBContext;
  private MockedStatic<OBPropertiesProvider> mockedPropertiesProvider;

  /**
   * Isolates database access, configuration and administrator context for each test.
   */
  @BeforeEach
  void setUp() {
    mockedOBDal = mockStatic(OBDal.class);
    mockedOBContext = mockStatic(OBContext.class);
    mockedPropertiesProvider = mockStatic(OBPropertiesProvider.class);
    mockedOBDal.when(OBDal::getInstance).thenReturn(mockOBDal);
  }

  /**
   * Releases static mocks after each test.
   */
  @AfterEach
  void tearDown() {
    mockedPropertiesProvider.close();
    mockedOBContext.close();
    mockedOBDal.close();
  }

  /**
   * Supplies Kafka properties for scenarios with asynchronous jobs.
   */
  private void configureProperties() {
    mockedPropertiesProvider.when(OBPropertiesProvider::getInstance).thenReturn(mockPropertiesProvider);
    when(mockPropertiesProvider.getOpenbravoProperties()).thenReturn(mockProperties);
  }

  /**
   * Skips startup when there are no asynchronous jobs and restores the administrator context.
   */
  @Test
  void testShouldStartWithoutAsyncJobs() {
    when(mockOBDal.createCriteria(Job.class)).thenReturn(mockCriteria);
    when(mockCriteria.list()).thenReturn(Collections.emptyList());

    assertFalse(asyncProcessStartup.shouldStart());

    mockedOBContext.verify(OBContext::setAdminMode);
    mockedOBContext.verify(OBContext::restorePreviousMode);
    verify(mockPropertiesProvider, never()).getOpenbravoProperties();
  }

  /**
   * Reports disabled Kafka at INFO and skips startup even when asynchronous jobs exist.
   */
  @Test
  void testShouldStartWithKafkaDisabledLogsAtInfo() {
    configureProperties();
    when(mockOBDal.createCriteria(Job.class)).thenReturn(mockCriteria);
    when(mockCriteria.list()).thenReturn(Collections.singletonList(mockJob));
    when(mockProperties.containsKey(KAFKA_ENABLE_KEY)).thenReturn(true);
    when(mockProperties.getProperty(KAFKA_ENABLE_KEY, KAFKA_ENABLE_VALUE)).thenReturn(KAFKA_ENABLE_VALUE);

    Logger logger = (Logger)
        LogManager.getLogger(AsyncProcessStartup.class);
    Level previousLevel = logger.getLevel();
    Appender appender = mock(Appender.class);
    when(appender.getName()).thenReturn("startup-test");
    when(appender.isStarted()).thenReturn(true);
    List<LogEvent> events = new ArrayList<>();
    doAnswer(invocation -> {
      LogEvent event = invocation.getArgument(0);
      events.add(event.toImmutable());
      return null;
    }).when(appender).append(any());
    logger.addAppender(appender);
    logger.setLevel(Level.INFO);
    try {
      assertFalse(asyncProcessStartup.shouldStart());

      assertEquals(3, events.size());
      assertTrue(events.stream().allMatch(event -> event.getLevel() == Level.INFO));
      assertTrue(events.get(0).getMessage().getFormattedMessage().contains("Kafka is disabled"));
      assertTrue(events.get(1).getMessage().getFormattedMessage().contains(KAFKA_ENABLE_KEY));
      assertTrue(events.get(2).getMessage().getFormattedMessage().contains("./gradlew setup smartbuild"));
      mockedOBContext.verify(OBContext::restorePreviousMode);
    } finally {
      logger.removeAppender(appender);
      logger.setLevel(previousLevel);
    }
  }

  /**
   * Treats a missing Kafka enable property as disabled and restores the administrator context.
   */
  @Test
  void testShouldStartWithKafkaPropertyMissing() {
    configureProperties();
    when(mockOBDal.createCriteria(Job.class)).thenReturn(mockCriteria);
    when(mockCriteria.list()).thenReturn(Collections.singletonList(mockJob));
    when(mockProperties.containsKey(KAFKA_ENABLE_KEY)).thenReturn(false);

    assertFalse(asyncProcessStartup.shouldStart());

    mockedOBContext.verify(OBContext::restorePreviousMode);
  }

  /**
   * Allows startup when asynchronous jobs exist and Kafka is enabled.
   */
  @Test
  void testShouldStartWithKafkaEnabled() {
    configureProperties();
    when(mockOBDal.createCriteria(Job.class)).thenReturn(mockCriteria);
    when(mockCriteria.list()).thenReturn(Collections.singletonList(mockJob));
    when(mockProperties.containsKey(KAFKA_ENABLE_KEY)).thenReturn(true);
    when(mockProperties.getProperty(KAFKA_ENABLE_KEY, KAFKA_ENABLE_VALUE)).thenReturn("true");

    assertTrue(asyncProcessStartup.shouldStart());

    mockedOBContext.verify(OBContext::restorePreviousMode);
  }

  /**
   * Restores the administrator context if querying asynchronous jobs fails.
   */
  @Test
  void testShouldStartRestoresContextOnQueryFailure() {
    when(mockOBDal.createCriteria(Job.class)).thenReturn(mockCriteria);
    OBException failure = new OBException("Unable to query asynchronous jobs");
    when(mockCriteria.list()).thenThrow(failure);

    assertSame(failure, assertThrows(OBException.class, () -> asyncProcessStartup.shouldStart()));

    mockedOBContext.verify(OBContext::restorePreviousMode);
  }

}
