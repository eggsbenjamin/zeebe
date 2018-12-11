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
package io.zeebe.logstreams.state;

import io.zeebe.db.ZeebeDb;
import io.zeebe.db.impl.rocksdb.ZbRocksDb;
import io.zeebe.db.impl.rocksdb.ZeebeRocksDbFactory;
import io.zeebe.logstreams.impl.Loggers;
import io.zeebe.logstreams.spi.SnapshotController;
import io.zeebe.util.FileUtil;
import java.io.File;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import org.rocksdb.Checkpoint;
import org.slf4j.Logger;

/** Controls how snapshot/recovery operations are performed on a StateController */
public class StateSnapshotController implements SnapshotController {
  private static final Logger LOG = Loggers.ROCKSDB_LOGGER;

  private final StateStorage storage;
  private final ZeebeRocksDbFactory zeebeRocksDbFactory;
  private ZbRocksDb db;

  public StateSnapshotController(
      final ZeebeRocksDbFactory rocksDbFactory, final StateStorage storage) {
    zeebeRocksDbFactory = rocksDbFactory;
    this.storage = storage;
  }

  @Override
  public void takeSnapshot(final StateSnapshotMetadata metadata) throws Exception {
    if (exists(metadata)) {
      return;
    }

    final File snapshotDir = storage.getSnapshotDirectoryFor(metadata);
    try (Checkpoint checkpoint = Checkpoint.create(db)) {
      checkpoint.createCheckpoint(snapshotDir.getAbsolutePath());
    }
  }

  @Override
  public StateSnapshotMetadata recover(
      long commitPosition, int term, Predicate<StateSnapshotMetadata> filter) throws Exception {
    final File runtimeDirectory = storage.getRuntimeDirectory();
    final List<StateSnapshotMetadata> snapshots = storage.listRecoverable(commitPosition);
    StateSnapshotMetadata recoveredMetadata = null;

    if (!snapshots.isEmpty()) {
      recoveredMetadata =
          snapshots
              .stream()
              .sorted(Comparator.reverseOrder())
              .filter(filter)
              .findFirst()
              .orElse(null);
    }

    if (runtimeDirectory.exists()) {
      FileUtil.deleteFolder(runtimeDirectory.getAbsolutePath());
    }

    if (recoveredMetadata != null) {
      final File snapshotPath = storage.getSnapshotDirectoryFor(recoveredMetadata);
      copySnapshot(runtimeDirectory, snapshotPath);
    }

    return recoveredMetadata;
  }

  @Override
  public ZeebeDb openDb() {
    db = zeebeRocksDbFactory.createDb(storage.getRuntimeDirectory());
    return db;
  }

  @Override
  public void purgeAll(Predicate<StateSnapshotMetadata> matcher) throws Exception {
    final List<StateSnapshotMetadata> others = storage.list(matcher);

    for (final StateSnapshotMetadata other : others) {
      FileUtil.deleteFolder(storage.getSnapshotDirectoryFor(other).getAbsolutePath());
      LOG.trace("Purged snapshot {}", other);
    }
  }

  private boolean exists(final StateSnapshotMetadata metadata) {
    return storage.getSnapshotDirectoryFor(metadata).exists();
  }

  private void copySnapshot(File runtimeDirectory, File snapshotPath) throws Exception {
    Files.copy(snapshotPath.toPath(), runtimeDirectory.toPath());
  }
}
