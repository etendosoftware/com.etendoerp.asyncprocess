package com.etendoerp.asyncprocess.hooks;

import com.etendoerp.asyncprocess.data.LogHeader;

public interface LogPersistorIdentifierHook {
  boolean identifyLogRecord(LogHeader header);
}
