/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.db.impl;

import io.zeebe.db.ColumnFamily;
import io.zeebe.db.ZeebeDb;
import io.zeebe.db.impl.rocksdb.ZeebeRocksDbFactory;
import java.io.File;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** */
public class ZeebeDbTest {

  public ZeebeDb zeebeDb;

  @Before
  public void setUp() {
    zeebeDb =
        ZeebeRocksDbFactory.newFactory(DefaultColumnFamily.class).createDb(new File("/tmp/data"));
  }

  @After
  public void close() throws Exception {
    zeebeDb.close();
  }

  @Test
  public void shouldStoreValueWithColumnFamily() {
    // given db
    final ZbLong longKey = new ZbLong();
    longKey.wrapLong(2);
    final ZbLong longValue = new ZbLong();
    longValue.wrapLong(1);

    final ColumnFamily<ZbLong, ZbLong> columnFamily =
        zeebeDb.createColumnFamily(DefaultColumnFamily.DEFAULT, longKey, longValue);

    // when
    columnFamily.put(longKey, longValue);

    zeebeDb.batch(() -> columnFamily.put(longKey, longValue));

    // then

  }
}
