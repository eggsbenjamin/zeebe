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

import io.zeebe.db.ZbKey;
import io.zeebe.db.ZbValue;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.agrona.ExpandableArrayBuffer;
import org.rocksdb.WriteBatch;

/** */
public class ZbRocksBatch extends WriteBatch {

  private static final Method PUT_METHOD;
  private static final Method DELETE_METHOD;

  static {
    try {
      PUT_METHOD =
          WriteBatch.class.getDeclaredMethod(
              "put", Long.TYPE, byte[].class, Integer.TYPE, byte[].class, Integer.TYPE, Long.TYPE);
      PUT_METHOD.setAccessible(true);

      DELETE_METHOD =
          WriteBatch.class.getDeclaredMethod(
              "delete", Long.TYPE, byte[].class, Integer.TYPE, Long.TYPE);
      DELETE_METHOD.setAccessible(true);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  private final ExpandableArrayBuffer keyBuffer = new ExpandableArrayBuffer();
  private final ExpandableArrayBuffer valueBuffer = new ExpandableArrayBuffer();

  public void put(long columnFamilyHandle, ZbKey key, ZbValue value) {
    key.write(keyBuffer, 0);
    value.write(valueBuffer, 0);

    try {
      PUT_METHOD.invoke(
          this,
          nativeHandle_,
          keyBuffer.byteArray(),
          key.getLength(),
          valueBuffer.byteArray(),
          value.getLength(),
          columnFamilyHandle);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }
}
