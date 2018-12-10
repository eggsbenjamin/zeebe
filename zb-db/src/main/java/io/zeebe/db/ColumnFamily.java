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

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public interface ColumnFamily<KeyType extends ZbKey, ValueType extends ZbValue> {

  void put(KeyType key, ValueType value);

  /**
   * The corresponding stored value to the given key.
   *
   * @param key the key
   * @return the value, if the key was not found null
   */
  ValueType get(KeyType key);

  void foreach(Consumer<ValueType> consumer);

  void foreach(BiConsumer<KeyType, ValueType> consumer);

  void whileTrue(BiFunction<KeyType, ValueType, Boolean> iterator);

  void whileEqualPrefix(ZbKey keyPrefix, BiConsumer<KeyType, ValueType> consumer);

  void whileEqualPrefix(ZbKey keyPrefix, BiFunction<KeyType, ValueType, Boolean> consumer);

  void delete(KeyType dueDateAndElementInstanceKeyTimerKey);

  boolean exists(KeyType jobKey);
}
