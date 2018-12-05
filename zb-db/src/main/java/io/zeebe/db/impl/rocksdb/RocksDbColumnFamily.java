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
package io.zeebe.db.impl.rocksdb;

import io.zeebe.db.ColumnFamily;
import io.zeebe.db.ZbKey;
import io.zeebe.db.ZbValue;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/** */
public class RocksDbColumnFamily<KeyType extends ZbKey, ValueType extends ZbValue>
    implements ColumnFamily<KeyType, ValueType> {

  private final ZbRocksDb zbRocksDb;
  private final ValueType valueInstance;
  private final long handle;

  public RocksDbColumnFamily(
      ZbRocksDb zbRocksDb, ZbColumnFamilies columnFamily, ValueType valueInstance) {
    this.zbRocksDb = zbRocksDb;
    handle = zbRocksDb.getColumnFamilyHandle(columnFamily);
    this.valueInstance = valueInstance;
  }

  @Override
  public void put(KeyType key, ValueType value) {
    zbRocksDb.put(handle, key, value);
  }

  @Override
  public ValueType get(KeyType key) {
    return null;
  }

  public void foreach(Consumer<ValueType> consumer) {}

  @Override
  public void whileTrue(BiFunction<KeyType, ValueType, Boolean> iterator) {}

  @Override
  public void whileEqualPrefix(ZbKey keyPrefixType, BiConsumer<KeyType, ValueType> consumer) {}

  @Override
  public void delete(KeyType key) {}
}
