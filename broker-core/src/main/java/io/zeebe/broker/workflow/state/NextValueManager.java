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

import io.zeebe.broker.logstreams.state.ZbColumnFamilies;
import io.zeebe.db.ColumnFamily;
import io.zeebe.db.ZeebeDb;
import io.zeebe.db.impl.ZbLong;
import io.zeebe.db.impl.ZbString;

public class NextValueManager {

  private static final int INITIAL_VALUE = 0;

  private final long initialValue;

  private final ColumnFamily<ZbString, ZbLong> nextValueColumnFamily;
  private final ZbString nextValueKey;

  public NextValueManager(ZeebeDb<ZbColumnFamilies> zeebeDb, ZbColumnFamilies columnFamily) {
    this(INITIAL_VALUE, zeebeDb, columnFamily);
  }

  public NextValueManager(
      long initialValue, ZeebeDb<ZbColumnFamilies> zeebeDb, ZbColumnFamilies columnFamily) {
    this.initialValue = initialValue;

    nextValueKey = new ZbString();
    nextValueColumnFamily = zeebeDb.createColumnFamily(columnFamily, nextValueKey, new ZbLong());
  }

  public long getNextValue(String key) {
    nextValueKey.wrapString(key);

    final ZbLong zbLong = nextValueColumnFamily.get(nextValueKey);

    long previousKey = initialValue;
    if (zbLong.isFilled()) {
      previousKey = zbLong.getValue();
    }

    final long nextKey = previousKey + 1;
    zbLong.wrapLong(nextKey);
    nextValueColumnFamily.put(nextValueKey, zbLong);

    return nextKey;
  }
}
