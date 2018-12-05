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

import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.workflow.state.StoredRecord.Purpose;
import io.zeebe.db.ColumnFamily;
import io.zeebe.db.ZeebeDb;
import io.zeebe.db.impl.ZbByte;
import io.zeebe.db.impl.ZbCompositeKey;
import io.zeebe.db.impl.ZbLong;
import io.zeebe.db.impl.ZbNil;
import io.zeebe.db.impl.rocksdb.ZbColumnFamilies;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.agrona.ExpandableArrayBuffer;

public class ElementInstanceState {
  private final Map<Long, ElementInstance> cachedInstances = new HashMap<>();

  private final ColumnFamily<ZbCompositeKey<ZbLong, ZbLong>, ZbNil> parentChildColumnFamily;
  private final ZbCompositeKey<ZbLong, ZbLong> parentChildKey;
  private final ZbLong parentKey;

  private final ZbLong elementInstanceKey;
  private final ElementInstance elementInstance;
  private final ColumnFamily<ZbLong, ElementInstance> elementInstanceColumnFamily;

  private final ZbLong tokenKey;
  private final StoredRecord storedRecord;
  private final ColumnFamily<ZbLong, StoredRecord> tokenColumnFamily;

  private final ZbLong tokenParentKey;
  private final ZbCompositeKey<ZbCompositeKey<ZbLong, ZbByte>, ZbLong> tokenParentStateTokenKey;
  private final ZbByte stateKey;
  private final ZbCompositeKey<ZbLong, ZbByte> tokenParentStateKey;
  private final ColumnFamily<ZbCompositeKey<ZbCompositeKey<ZbLong, ZbByte>, ZbLong>, ZbNil>
      tokenParentChildColumnFamily;

  public ElementInstanceState(ZeebeDb<ZbColumnFamilies> zeebeDb) {

    parentChildKey = new ZbCompositeKey<>();
    parentKey = new ZbLong();
    parentChildColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.ELEMENT_INSTANCE_PARENT_CHILD, parentChildKey, ZbNil.INSTANCE);

    elementInstanceKey = new ZbLong();
    elementInstance = new ElementInstance();
    elementInstanceColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.ELEMENT_INSTANCE_KEY, elementInstanceKey, elementInstance);

    tokenKey = new ZbLong();
    storedRecord = new StoredRecord();
    tokenColumnFamily =
        zeebeDb.createColumnFamily(ZbColumnFamilies.TOKEN_EVENTS, tokenKey, storedRecord);

    tokenParentKey = new ZbLong();
    tokenParentStateTokenKey = new ZbCompositeKey<>();
    stateKey = new ZbByte();
    tokenParentStateKey = new ZbCompositeKey<>();
    tokenParentChildColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.TOKEN_EVENTS, tokenParentStateTokenKey, ZbNil.INSTANCE);
  }

  public ElementInstance newInstance(
      long key, WorkflowInstanceRecord value, WorkflowInstanceIntent state) {
    return newInstance(null, key, value, state);
  }

  public ElementInstance newInstance(
      ElementInstance parent,
      long key,
      WorkflowInstanceRecord value,
      WorkflowInstanceIntent state) {

    final ElementInstance instance;
    if (parent == null) {
      instance = new ElementInstance(key, state, value);
    } else {
      instance = new ElementInstance(key, parent, state, value);
    }
    cachedInstances.put(key, instance);

    return instance;
  }

  private void writeElementInstance(ElementInstance instance) {
    elementInstanceKey.wrapLong(elementInstance.getKey());
    parentKey.wrapLong(elementInstance.getParentKey());
    parentChildKey.wrapKeys(parentKey, elementInstanceKey);

    elementInstanceColumnFamily.put(elementInstanceKey, instance);
    parentChildColumnFamily.put(parentChildKey, ZbNil.INSTANCE);
  }

  private final ExpandableArrayBuffer copyBuffer = new ExpandableArrayBuffer();

  public ElementInstance getInstance(long key) {
    return cachedInstances.computeIfAbsent(
        key,
        k -> {
          elementInstanceKey.wrapLong(key);
          final ElementInstance elementInstance =
              elementInstanceColumnFamily.get(elementInstanceKey);

          if (elementInstance != null) {
            elementInstance.write(copyBuffer, 0);

            final ElementInstance copiedElementInstance = new ElementInstance();
            copiedElementInstance.wrap(copyBuffer, 0, elementInstance.getLength());
            return copiedElementInstance;
          }
          return null;
        });
  }

  public void removeInstance(long key) {
    final ElementInstance instance = getInstance(key);

    if (instance != null) {
      elementInstanceKey.wrapLong(key);
      parentKey.wrapLong(instance.getKey());
      parentChildKey.wrapKeys(parentKey, elementInstanceKey);

      parentChildColumnFamily.delete(parentChildKey);
      elementInstanceColumnFamily.delete(elementInstanceKey);
      cachedInstances.remove(key);

      tokenParentChildColumnFamily.whileEqualPrefix(
          elementInstanceKey,
          (compositeKey, nil) -> {
            tokenParentChildColumnFamily.delete(compositeKey);
            tokenColumnFamily.delete(compositeKey.getSecond());
          });

      final long parentKey = instance.getParentKey();
      if (parentKey > 0) {
        final ElementInstance parentInstance = getInstance(parentKey);
        parentInstance.decrementChildCount();
      }
    }
  }

  public StoredRecord getTokenEvent(long tokenKey) {
    this.tokenKey.wrapLong(tokenKey);
    return tokenColumnFamily.get(this.tokenKey);
  }

  void updateInstance(ElementInstance scopeInstance) {
    writeElementInstance(scopeInstance);
  }

  public List<ElementInstance> getChildren(long parentKey) {
    final List<ElementInstance> children = new ArrayList<>();
    final ElementInstance parentInstance = getInstance(parentKey);
    if (parentInstance != null) {
      this.parentKey.wrapLong(parentKey);

      parentChildColumnFamily.whileEqualPrefix(
          this.parentKey,
          (key, value) -> {
            final ZbLong childKey = key.getSecond();
            final ElementInstance childInstance = getInstance(childKey.getValue());
            children.add(childInstance);
          });
    }
    return children;
  }

  public void consumeToken(long scopeKey) {
    final ElementInstance elementInstance = getInstance(scopeKey);
    if (elementInstance != null) {
      elementInstance.consumeToken();
    }
  }

  public void spawnToken(long scopeKey) {
    final ElementInstance elementInstance = getInstance(scopeKey);
    if (elementInstance != null) {
      elementInstance.spawnToken();
    }
  }

  public void storeTokenEvent(
      long scopeKey, TypedRecord<WorkflowInstanceRecord> record, Purpose purpose) {
    final IndexedRecord indexedRecord =
        new IndexedRecord(
            record.getKey(),
            (WorkflowInstanceIntent) record.getMetadata().getIntent(),
            record.getValue());
    final StoredRecord storedRecord = new StoredRecord(indexedRecord, purpose);

    setTokenKeys(scopeKey, record.getKey(), purpose);

    tokenColumnFamily.put(tokenKey, storedRecord);
    tokenParentChildColumnFamily.put(tokenParentStateTokenKey, ZbNil.INSTANCE);
  }

  public void removeStoredRecord(long scopeKey, long recordKey, Purpose purpose) {
    setTokenKeys(scopeKey, recordKey, purpose);

    tokenColumnFamily.delete(tokenKey);
    tokenParentChildColumnFamily.delete(tokenParentStateTokenKey);
  }

  private void setTokenKeys(long scopeKey, long recordKey, Purpose purpose) {
    tokenParentKey.wrapLong(scopeKey);
    stateKey.wrapByte((byte) purpose.ordinal());
    tokenKey.wrapLong(recordKey);

    tokenParentStateKey.wrapKeys(tokenParentKey, stateKey);
    tokenParentStateTokenKey.wrapKeys(tokenParentStateKey, tokenKey);
  }

  public List<IndexedRecord> getDeferredTokens(long scopeKey) {
    return collectTokenEvents(scopeKey, Purpose.DEFERRED_TOKEN);
  }

  public IndexedRecord getFailedToken(long key) {
    final StoredRecord tokenEvent = getTokenEvent(key);
    if (tokenEvent != null && tokenEvent.getPurpose() == Purpose.FAILED_TOKEN) {
      return tokenEvent.getRecord();
    } else {
      return null;
    }
  }

  public void updateFailedToken(IndexedRecord indexedRecord) {
    final StoredRecord storedRecord = new StoredRecord(indexedRecord, Purpose.FAILED_TOKEN);
    tokenKey.wrapLong(indexedRecord.getKey());
    tokenColumnFamily.put(tokenKey, storedRecord);
  }

  public List<IndexedRecord> getFinishedTokens(long scopeKey) {
    return collectTokenEvents(scopeKey, Purpose.FINISHED_TOKEN);
  }

  private List<IndexedRecord> collectTokenEvents(long scopeKey, Purpose purpose) {
    final List<IndexedRecord> records = new ArrayList<>();
    visitTokens(scopeKey, purpose, records::add);
    return records;
  }

  @FunctionalInterface
  public interface TokenVisitor {
    void visitToken(IndexedRecord indexedRecord);
  }

  public void visitFailedTokens(long scopeKey, TokenVisitor visitor) {
    visitTokens(scopeKey, Purpose.FAILED_TOKEN, visitor);
  }

  private void visitTokens(long scopeKey, Purpose purpose, TokenVisitor visitor) {

    tokenParentKey.wrapLong(scopeKey);
    stateKey.wrapByte((byte) purpose.ordinal());
    tokenParentStateKey.wrapKeys(tokenParentKey, stateKey);

    tokenParentChildColumnFamily.whileEqualPrefix(
        tokenParentStateKey,
        (compositeKey, value) -> {
          final ZbLong tokenKey = compositeKey.getSecond();
          final StoredRecord tokenEvent = tokenColumnFamily.get(tokenKey);
          if (tokenEvent != null) {
            visitor.visitToken(tokenEvent.getRecord());
          }
        });
  }

  public void flushDirtyState() {
    for (Entry<Long, ElementInstance> entry : cachedInstances.entrySet()) {
      updateInstance(entry.getValue());
    }
    cachedInstances.clear();
  }
}
