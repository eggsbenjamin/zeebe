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
import io.zeebe.db.impl.ZbLong;
import io.zeebe.db.impl.ZbString;

public class NextValueManager {

  private static final int INITIAL_VALUE = 0;

  private final long initialValue;
  private final ZbString zbString = new ZbString();

  public NextValueManager() {
    this(INITIAL_VALUE);
  }

  public NextValueManager(long initialValue) {
    this.initialValue = initialValue;
  }

  public long getNextValue(ColumnFamily<ZbString, ZbLong> columnFamily, String key) {
    zbString.wrapString(key);

    final ZbLong zbLong = columnFamily.get(zbString);

    long previousKey = initialValue;
    if (zbLong.isFilled()) {
      previousKey = zbLong.getValue();
    }

    final long nextKey = previousKey + 1;
    zbLong.wrapLong(nextKey);
    columnFamily.put(zbString, zbLong);

    return nextKey;
  }
}
