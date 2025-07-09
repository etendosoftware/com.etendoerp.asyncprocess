package com.etendoerp.asyncprocess.retry;

/**
 * Interface que define la política de reintentos para procesos asíncronos
 */
public interface RetryPolicy {
  /**
   * Determina si se debe reintentar la operación
   * @param attemptNumber Número de intento actual
   * @return true si se debe reintentar, false en caso contrario
   */
  boolean shouldRetry(int attemptNumber);

  /**
   * Obtiene el tiempo de espera antes del siguiente reintento
   * @param attemptNumber Número de intento actual
   * @return Tiempo en milisegundos a esperar antes del siguiente reintento
   */
  long getRetryDelay(int attemptNumber);
}
