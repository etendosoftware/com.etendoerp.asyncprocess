package com.etendoerp.asyncprocess.hooks;

import com.etendoerp.asyncprocess.data.LogHeader;

/**
 * Extension point used to determine whether a given log record should be persisted
 * by the LogPersistor component.
 *
 * <p>Implementations of this hook inspect the provided {@link LogHeader} and return
 * true when the log record matches criteria that require persistence (for example,
 * specific log types, origins, or severity). Returning false indicates the record
 * should be ignored by the persistor.</p>
 */
public interface LogPersistorIdentifierHook {
  /**
   * Inspect the supplied log header and decide if the corresponding record must be
   * persisted.
   *
   * @param header metadata describing the log record (source, type, timestamps, etc.)
   * @return true if the record should be persisted by the LogPersistor, false to skip it
   */
  boolean identifyLogRecord(LogHeader header);
}
