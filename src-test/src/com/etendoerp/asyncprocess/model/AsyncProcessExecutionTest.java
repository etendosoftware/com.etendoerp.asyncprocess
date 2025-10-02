package com.etendoerp.asyncprocess.model;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Date;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

class AsyncProcessExecutionTest {

  @Test
  void testGettersAndSetters() {
    AsyncProcessExecution exec = new AsyncProcessExecution();
    String id = "id-1";
    String processId = "proc-1";
    String log = "some log";
    String desc = "description";
    String params = "param=value";
    Date time = new Date();
    AsyncProcessState state = AsyncProcessState.STARTED;

    exec.setId(id);
    exec.setAsyncProcessId(processId);
    exec.setLog(log);
    exec.setDescription(desc);
    exec.setParams(params);
    exec.setTime(time);
    exec.setState(state);

    assertEquals(id, exec.getId());
    assertEquals(processId, exec.getAsyncProcessId());
    assertEquals(log, exec.getLog());
    assertEquals(desc, exec.getDescription());
    assertEquals(params, exec.getParams());
    assertEquals(time, exec.getTime());
    assertEquals(state, exec.getState());
  }

  @Test
  void testCompareToDifferentTimes() throws InterruptedException {
    AsyncProcessExecution older = new AsyncProcessExecution();
    older.setId("A");
    older.setTime(new Date(System.currentTimeMillis() - 1000));

    AsyncProcessExecution newer = new AsyncProcessExecution();
    newer.setId("B");
    newer.setTime(new Date());

    // newer should come before older (descending time) -> older.compareTo(newer) > 0
    assertTrue(older.compareTo(newer) > 0, "Older should be considered greater than newer (descending order)");
    assertTrue(newer.compareTo(older) < 0, "Newer should be considered less than older");

    SortedSet<AsyncProcessExecution> set = new TreeSet<>();
    set.add(older);
    set.add(newer);
    // First element should be 'newer'
    assertEquals("B", set.first().getId());
  }

  @Test
  void testCompareToSameTimeReversedIdOrder() {
    Date same = new Date();
    AsyncProcessExecution e1 = new AsyncProcessExecution();
    e1.setId("alpha");
    e1.setTime(same);
    AsyncProcessExecution e2 = new AsyncProcessExecution();
    e2.setId("beta");
    e2.setTime(same);

    // For equal time, ordering uses reverse id (o.id.compareTo(this.id))
    // e1.compareTo(e2) => returns e2.id.compareTo(e1.id) => "beta" vs "alpha" (>0)
    assertTrue(e1.compareTo(e2) > 0);
    assertTrue(e2.compareTo(e1) < 0);

    SortedSet<AsyncProcessExecution> set = new TreeSet<>();
    set.add(e1);
    set.add(e2);
    // First should be beta (reverse lexicographical)
    assertEquals("beta", set.first().getId());
  }

  @Test
  void testEqualsAndHashCodeDefaultBehavior() {
    AsyncProcessExecution e1 = new AsyncProcessExecution();
    AsyncProcessExecution e2 = new AsyncProcessExecution();
    assertNotEquals(e1, e2);
    assertEquals(e1, e1);
    assertEquals(e1.hashCode(), e1.hashCode());
  }
}
