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

import io.zeebe.db.ZeebeDbFactory;
import io.zeebe.util.ByteValue;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ChecksumType;
import org.rocksdb.ClockCache;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.Filter;
import org.rocksdb.MemTableConfig;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SkipListMemTableConfig;
import org.rocksdb.TableFormatConfig;

public final class ZeebeRocksDbFactory<ColumnFamilyType extends Enum<ColumnFamilyType>>
    implements ZeebeDbFactory<ColumnFamilyType> {

  static {
    RocksDB.loadLibrary();
  }

  private final Class<ColumnFamilyType> columnFamilyTypeClass;

  private ZeebeRocksDbFactory(Class<ColumnFamilyType> columnFamilyTypeClass) {
    this.columnFamilyTypeClass = columnFamilyTypeClass;
  }

  public static <ColumnFamilyType extends Enum<ColumnFamilyType>>
      ZeebeRocksDbFactory<ColumnFamilyType> newFactory(
          Class<ColumnFamilyType> columnFamilyTypeClass) {
    return new ZeebeRocksDbFactory(columnFamilyTypeClass);
  }

  @Override
  public ZbRocksDb<ColumnFamilyType> createDb(File pathName) {
    return open(
        pathName,
        Arrays.stream(columnFamilyTypeClass.getEnumConstants())
            .map(c -> c.name().toLowerCase().getBytes())
            .collect(Collectors.toList()));
  }

  protected ZbRocksDb open(final File dbDirectory, List<byte[]> columnFamilyNames) {

    ZbRocksDb db = null;
    try {
      final List<AutoCloseable> closeables = new ArrayList<>();
      final ColumnFamilyOptions columnFamilyOptions = createColumnFamilyOptions(closeables);
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          createFamilyDescriptors(columnFamilyNames, columnFamilyOptions);

      final DBOptions dbOptions =
          new DBOptions()
              .setEnv(getDbEnv())
              .setCreateMissingColumnFamilies(true)
              .setErrorIfExists(false)
              .setCreateIfMissing(true);
      closeables.add(dbOptions);

      db =
          ZbRocksDb.openZbDb(
              dbOptions,
              dbDirectory.getAbsolutePath(),
              columnFamilyDescriptors,
              closeables,
              columnFamilyTypeClass);
    } catch (final RocksDBException ex) {
      if (db != null) {
        try {
          db.close();
        } catch (Exception e) {
          throw new RuntimeException(ex);
        }
      }
      throw new RuntimeException(ex);
    }
    return db;
  }

  private List<ColumnFamilyDescriptor> createFamilyDescriptors(
      List<byte[]> columnFamilyNames, ColumnFamilyOptions columnFamilyOptions) {
    final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();

    if (columnFamilyNames != null && columnFamilyNames.size() > 0) {
      final Set<Integer> duplicateCheck = new HashSet<>();
      for (byte[] name : columnFamilyNames) {
        final boolean isDuplicate = !duplicateCheck.add(Arrays.hashCode(name));
        if (isDuplicate) {
          throw new IllegalStateException(
              String.format(
                  "Expect to have no duplicate column family name, got '%s' as duplicate.",
                  new String(name)));
        }

        final ColumnFamilyDescriptor columnFamilyDescriptor =
            new ColumnFamilyDescriptor(name, columnFamilyOptions);
        columnFamilyDescriptors.add(columnFamilyDescriptor);
      }
    }
    return columnFamilyDescriptors;
  }

  protected ColumnFamilyOptions createColumnFamilyOptions(List<AutoCloseable> closeables) {
    final Filter filter = new BloomFilter();
    closeables.add(filter);

    final Cache cache = new ClockCache(ByteValue.ofMegabytes(16).toBytes(), 10);
    closeables.add(cache);

    final TableFormatConfig sstTableConfig =
        new BlockBasedTableConfig()
            .setBlockCache(cache)
            .setBlockSize(ByteValue.ofKilobytes(16).toBytes())
            .setChecksumType(ChecksumType.kCRC32c)
            .setFilter(filter);
    final MemTableConfig memTableConfig = new SkipListMemTableConfig();

    final ColumnFamilyOptions columnFamilyOptions =
        new ColumnFamilyOptions()
            .optimizeUniversalStyleCompaction()
            .setWriteBufferSize(ByteValue.ofMegabytes(64).toBytes())
            .setMemTableConfig(memTableConfig)
            .setTableFormatConfig(sstTableConfig);
    closeables.add(columnFamilyOptions);
    return columnFamilyOptions;
  }

  protected Env getDbEnv() {
    return Env.getDefault();
  }
}
