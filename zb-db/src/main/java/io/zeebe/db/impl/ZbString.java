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

import io.zeebe.db.ZbKey;
import io.zeebe.db.ZbValue;
import io.zeebe.util.EnsureUtil;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public class ZbString implements ZbKey, ZbValue {

  private byte[] bytes;

  public void wrapString(String string) {
    this.bytes = string.getBytes();
  }

  public void wrapBytes(byte[] bytes) {
    this.bytes = bytes;
  }

  public void wrapBuffer(DirectBuffer buffer) {
    EnsureUtil.ensureArrayBacked(buffer);
    bytes = buffer.byteArray();
  }

  @Override
  public void wrap(DirectBuffer directBuffer, int offset, int length) {
    bytes = new byte[length];
    directBuffer.getBytes(offset, bytes);
  }

  @Override
  public int getLength() {
    return bytes.length;
  }

  @Override
  public void write(MutableDirectBuffer mutableDirectBuffer, int offset) {
    mutableDirectBuffer.putBytes(offset, bytes);
  }

  @Override
  public String toString() {
    return new String(bytes);
  }
}
