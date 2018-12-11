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
import io.zeebe.db.ZeebeDb;
import java.lang.reflect.Field;
import java.util.EnumMap;
import java.util.List;
import org.agrona.ExpandableArrayBuffer;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksObject;
import org.rocksdb.WriteOptions;

public class ZbRocksDb<ColumnFamilyNames extends Enum<ColumnFamilyNames>> extends RocksDB
    implements ZeebeDb<ColumnFamilyNames> {

  private static final Field NATIVE_HANDLE_FIELD;

  static {
    RocksDB.loadLibrary();

    try {
      NATIVE_HANDLE_FIELD = RocksObject.class.getDeclaredField("nativeHandle_");
      NATIVE_HANDLE_FIELD.setAccessible(true);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }

  private final List<AutoCloseable> closables;
  private final Class<ColumnFamilyNames> columnFamilyNamesClass;
  private ZbRocksBatch batch;

  public static <ColumnFamilyNames extends Enum<ColumnFamilyNames>>
      ZbRocksDb<ColumnFamilyNames> openZbDb(
          final DBOptions options,
          final String path,
          final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
          final List<AutoCloseable> closables,
          Class<ColumnFamilyNames> columnFamilyTypeClass)
          throws RocksDBException {
    final EnumMap<ColumnFamilyNames, Long> columnFamilyMap = new EnumMap<>(columnFamilyTypeClass);

    final byte[][] cfNames = new byte[columnFamilyDescriptors.size()][];
    final long[] cfOptionHandles = new long[columnFamilyDescriptors.size()];
    for (int i = 0; i < columnFamilyDescriptors.size(); i++) {
      final ColumnFamilyDescriptor cfDescriptor = columnFamilyDescriptors.get(i);
      cfNames[i] = cfDescriptor.getName();
      cfOptionHandles[i] = getNativeHandle(cfDescriptor.getOptions());
    }

    final long[] handles = open(getNativeHandle(options), path, cfNames, cfOptionHandles);

    final ColumnFamilyNames[] enumConstants = columnFamilyTypeClass.getEnumConstants();
    for (int i = 1; i < handles.length; i++) {
      columnFamilyMap.put(enumConstants[i - 1], handles[i]);
    }

    final ZbRocksDb<ColumnFamilyNames> db =
        new ZbRocksDb<ColumnFamilyNames>(
            handles[0], columnFamilyMap, closables, columnFamilyTypeClass);
    db.storeOptionsInstance(options);

    return db;
  }

  // we can also simply use one buffer
  private final ExpandableArrayBuffer keyBuffer = new ExpandableArrayBuffer();
  private final ExpandableArrayBuffer valueBuffer = new ExpandableArrayBuffer();

  private final EnumMap<ColumnFamilyNames, Long> columnFamilyMap;

  protected ZbRocksDb(
      long l,
      EnumMap<ColumnFamilyNames, Long> columnFamilyMap,
      List<AutoCloseable> closables,
      Class<ColumnFamilyNames> columnFamilyNamesClass) {
    super(l);
    this.columnFamilyMap = columnFamilyMap;
    this.closables = closables;
    this.columnFamilyNamesClass = columnFamilyNamesClass;
  }

  @Override
  public <KeyType extends ZbKey, ValueType extends ZbValue>
      ColumnFamily<KeyType, ValueType> createColumnFamily(
          ColumnFamilyNames columnFamily, KeyType keyInstance, ValueType valueInstance) {
    return new RocksDbColumnFamily<>(this, columnFamily, valueInstance);
  }

  protected void put(long columnFamilyHandle, ZbKey key, ZbValue value) {
    key.write(keyBuffer, 0);
    value.write(valueBuffer, 0);

    try {

      if (isInBatch()) {
        batch.put(columnFamilyHandle, key, value);
      } else {
        put(
            nativeHandle_,
            keyBuffer.byteArray(),
            0,
            key.getLength(),
            valueBuffer.byteArray(),
            0,
            value.getLength(),
            columnFamilyHandle);
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private boolean isInBatch() {
    return batch != null;
  }

  public void batch(Runnable operations) {
    try (WriteOptions options = new WriteOptions()) {
      batch = new ZbRocksBatch();

      operations.run();
      write(options, batch);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      if (batch != null) {
        batch.close();
        batch = null;
      }
    }
  }

  static long getNativeHandle(final RocksObject object) {
    try {
      return (long) NATIVE_HANDLE_FIELD.get(object);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  public long getColumnFamilyHandle(ColumnFamilyNames columnFamily) {
    return columnFamilyMap.get(columnFamily);
  }

  @Override
  public void close() {
    closables.forEach(
        closable -> {
          try {
            closable.close();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

    super.close();
  }
}
