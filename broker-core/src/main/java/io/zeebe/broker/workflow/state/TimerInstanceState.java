/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.workflow.state;

import io.zeebe.db.ColumnFamily;
import io.zeebe.db.ZeebeDb;
import io.zeebe.db.impl.ZbByte;
import io.zeebe.db.impl.ZbCompositeKey;
import io.zeebe.db.impl.ZbLong;
import io.zeebe.db.impl.rocksdb.ZbColumnFamilies;
import java.util.function.Consumer;

public class TimerInstanceState {

  private final ColumnFamily<ZbCompositeKey<ZbLong, ZbLong>, TimerInstance>
      timerInstanceColumnFamily;
  private final TimerInstance timerInstance;
  private final ZbLong timerKey;
  private final ZbLong elementInstanceKey;
  private final ZbCompositeKey<ZbLong, ZbLong> elementAndTimerKey;

  private final ColumnFamily<ZbCompositeKey<ZbLong, ZbCompositeKey<ZbLong, ZbLong>>, ZbByte>
      dueDateColumnFamily;
  private final ZbLong dueDateKey;
  private final ZbCompositeKey<ZbLong, ZbCompositeKey<ZbLong, ZbLong>>
      dueDateAndElementInstanceKeyTimerKey;
  private final ZeebeDb<ZbColumnFamilies> zeebeDb;
  private final ZbByte nil;

  private long nextDueDate;

  public TimerInstanceState(ZeebeDb<ZbColumnFamilies> zeebeDb) {
    this.zeebeDb = zeebeDb;

    timerInstance = new TimerInstance();
    elementAndTimerKey = new ZbCompositeKey<>();
    timerKey = new ZbLong();
    elementInstanceKey = new ZbLong();
    timerInstanceColumnFamily =
        zeebeDb.createColumnFamily(ZbColumnFamilies.TIMERS, elementAndTimerKey, timerInstance);

    dueDateAndElementInstanceKeyTimerKey = new ZbCompositeKey<>();
    nil = new ZbByte();
    nil.wrapByte((byte) 1);
    dueDateColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.TIMER_DUE_DATES, dueDateAndElementInstanceKeyTimerKey, nil);
    dueDateKey = new ZbLong();
  }

  public void put(TimerInstance timer) {
    zeebeDb.batch(
        () -> {
          timerKey.wrapLong(timer.getKey());
          elementInstanceKey.wrapLong(timer.getElementInstanceKey());
          elementAndTimerKey.wrapKeys(elementInstanceKey, timerKey);

          timerInstanceColumnFamily.put(elementAndTimerKey, timer);

          dueDateKey.wrapLong(timer.getDueDate());
          dueDateAndElementInstanceKeyTimerKey.wrapKeys(dueDateKey, elementAndTimerKey);
          dueDateColumnFamily.put(dueDateAndElementInstanceKeyTimerKey, nil);
        });
  }

  public long findTimersWithDueDateBefore(final long timestamp, TimerVisitor consumer) {
    nextDueDate = -1L;

    dueDateColumnFamily.whileTrue(
        (key, nil) -> {
          final ZbLong dueDate = key.getFirst();

          boolean consumed = false;
          if (dueDate.getValue() <= timestamp) {
            final ZbCompositeKey<ZbLong, ZbLong> elementAndTimerKey = key.getSecond();
            final TimerInstance timerInstance = timerInstanceColumnFamily.get(elementAndTimerKey);
            consumed = consumer.visit(timerInstance);
          }

          if (!consumed) {
            nextDueDate = dueDate.getValue();
          }
          return consumed;
        });

    return nextDueDate;
  }

  /**
   * NOTE: the timer instance given to the consumer is shared and will be mutated on the next
   * iteration.
   */
  public void forEachTimerForElementInstance(
      long elementInstanceKey, Consumer<TimerInstance> action) {
    this.elementInstanceKey.wrapLong(elementInstanceKey);

    timerInstanceColumnFamily.whileEqualPrefix(
        this.elementInstanceKey, (key, value) -> action.accept(value));
  }

  public TimerInstance get(long elementInstanceKey, long timerKey) {
    this.elementInstanceKey.wrapLong(elementInstanceKey);
    this.timerKey.wrapLong(timerKey);
    elementAndTimerKey.wrapKeys(this.elementInstanceKey, this.timerKey);

    return timerInstanceColumnFamily.get(elementAndTimerKey);
  }

  public void remove(TimerInstance timer) {

    zeebeDb.batch(
        () -> {
          elementInstanceKey.wrapLong(timer.getElementInstanceKey());
          timerKey.wrapLong(timer.getKey());
          elementAndTimerKey.wrapKeys(elementInstanceKey, timerKey);
          timerInstanceColumnFamily.delete(elementAndTimerKey);

          dueDateKey.wrapLong(timer.getDueDate());
          dueDateAndElementInstanceKeyTimerKey.wrapKeys(dueDateKey, elementAndTimerKey);
          dueDateColumnFamily.delete(dueDateAndElementInstanceKeyTimerKey);
        });
  }

  @FunctionalInterface
  public interface TimerVisitor {
    boolean visit(final TimerInstance timer);
  }
}
