package com.etendoerp.asyncprocess.startup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

import com.etendoerp.asyncprocess.recovery.ConsumerRecoveryManager;

/**
 * Verifies classloader restoration during startup callbacks and manual consumer recovery.
 */
class AsyncProcessStartupLifecycleTest {
  private static final String RESULT = "completed";
  private final AsyncProcessStartup startup = new AsyncProcessStartup();

  /**
   * Preserves the application classloader when it is already installed.
   *
   * @throws ReflectiveOperationException if the startup callback cannot be invoked
   */
  @Test
  void testCallbackWithApplicationClassLoader() throws ReflectiveOperationException {
    verifyCallbackClassLoader(AsyncProcessStartup.class.getClassLoader());
  }

  /**
   * Installs the application classloader and restores a missing context classloader.
   *
   * @throws ReflectiveOperationException if the startup callback cannot be invoked
   */
  @Test
  void testCallbackWithMissingClassLoader() throws ReflectiveOperationException {
    verifyCallbackClassLoader(null);
  }

  /**
   * Installs the application classloader and restores a different context classloader.
   *
   * @throws ReflectiveOperationException if the startup callback cannot be invoked
   */
  @Test
  void testCallbackWithDifferentClassLoader() throws ReflectiveOperationException {
    verifyCallbackClassLoader(new ClassLoader(AsyncProcessStartup.class.getClassLoader()) { });
  }

  /**
   * Restores the original classloader when the callback fails.
   */
  @Test
  void testCallbackFailureRestoresClassLoader() {
    Thread thread = Thread.currentThread();
    ClassLoader original = thread.getContextClassLoader();
    IllegalStateException failure = new IllegalStateException("Callback failed");
    try {
      thread.setContextClassLoader(null);
      InvocationTargetException thrown = assertThrows(InvocationTargetException.class,
          () -> invokeCallback(() -> {
            assertSame(AsyncProcessStartup.class.getClassLoader(), thread.getContextClassLoader());
            throw failure;
          }));
      assertSame(failure, thrown.getCause());
      assertNull(thread.getContextClassLoader());
    } finally {
      thread.setContextClassLoader(original);
    }
  }

  /**
   * Delegates successful manual recovery to the configured recovery manager.
   *
   * @throws ReflectiveOperationException if the recovery manager field cannot be accessed
   */
  @Test
  void testManualConsumerRecoverySucceeds() throws ReflectiveOperationException {
    ConsumerRecoveryManager recovery = mock(ConsumerRecoveryManager.class);
    Field field = AsyncProcessStartup.class.getDeclaredField("recoveryManager");
    field.setAccessible(true);
    field.set(startup, recovery);

    startup.forceConsumerRecovery("consumer-test");

    verify(recovery).forceRecoverConsumer("consumer-test");
  }

  /**
   * Checks the classloader visible to a callback and restoration after completion.
   *
   * @param context the classloader installed before running the callback
   * @throws ReflectiveOperationException if the startup callback cannot be invoked
   */
  private void verifyCallbackClassLoader(ClassLoader context) throws ReflectiveOperationException {
    Thread thread = Thread.currentThread();
    ClassLoader original = thread.getContextClassLoader();
    try {
      thread.setContextClassLoader(context);
      Object result = invokeCallback(() -> {
        assertSame(AsyncProcessStartup.class.getClassLoader(), thread.getContextClassLoader());
        return RESULT;
      });
      assertEquals(RESULT, result);
      assertSame(context, thread.getContextClassLoader());
    } finally {
      thread.setContextClassLoader(original);
    }
  }

  /**
   * Invokes the startup wrapper without starting Kafka or background services.
   *
   * @param callback the operation to run with the application classloader
   * @return the callback result
   * @throws ReflectiveOperationException if reflection or the callback fails
   */
  private Object invokeCallback(Supplier<?> callback) throws ReflectiveOperationException {
    Method method = AsyncProcessStartup.class.getDeclaredMethod("executeWithClassLoaderContext", Supplier.class);
    method.setAccessible(true);
    return method.invoke(startup, callback);
  }
}
