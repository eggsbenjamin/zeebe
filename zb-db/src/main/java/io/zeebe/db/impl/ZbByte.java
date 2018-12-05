package io.zeebe.db.impl;

import io.zeebe.db.ZbKey;
import io.zeebe.db.ZbValue;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/** */
public class ZbByte implements ZbKey, ZbValue {

  private byte value;

  public void wrapByte(byte value) {
    this.value = value;
  }

  @Override
  public void wrap(DirectBuffer directBuffer, int offset, int length) {
    value = directBuffer.getByte(offset);
  }

  @Override
  public int getLength() {
    return Byte.BYTES;
  }

  @Override
  public void write(MutableDirectBuffer mutableDirectBuffer, int offset) {
    mutableDirectBuffer.putByte(offset, value);
  }
}
