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
package io.zeebe.db;

/**
 * The zeebe database, to store key value pairs in different column families. The column families
 * are defined via the specified {@link ColumnFamilyType} enum.
 *
 * <p>To access and store key-value pairs in a specific column family the user needs to create a
 * ColumnFamily instance via {@link #createColumnFamily(Enum, ZbKey, ZbValue)}. If the column family
 * instances are created they are type save, which makes it possible that only the defined key and
 * value types are stored in the column family.
 *
 * @param <ColumnFamilyType>
 */
public interface ZeebeDb<ColumnFamilyType extends Enum> extends AutoCloseable {

  /**
   * Runs the commands like delete, put etc. in a batch operation. Access of different column
   * families inside this batch are possible.
   *
   * @param operations the operations
   */
  void batch(Runnable operations);

  /**
   * Creates an instance of a specific column family to access and store key-value pairs in that
   * column family. The key and value instances are used to ensure type safety.
   *
   * <p>If the column family instance is created only the defined key and value types can be stored
   * in the column family.
   *
   * @param columnFamily the enum instance of the column family
   * @param keyInstance this instance defines the type of the column family key type
   * @param valueInstance this instance defines the type of the column family value type
   * @param <KeyType> the key type of the column family
   * @param <ValueType> the value type of the column family
   * @return the created column family instance
   */
  <KeyType extends ZbKey, ValueType extends ZbValue>
      ColumnFamily<KeyType, ValueType> createColumnFamily(
          ColumnFamilyType columnFamily, KeyType keyInstance, ValueType valueInstance);
}
