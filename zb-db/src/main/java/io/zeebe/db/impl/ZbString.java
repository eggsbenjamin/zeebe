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
}
