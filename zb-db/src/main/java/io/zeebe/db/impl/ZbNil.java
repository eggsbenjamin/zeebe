package io.zeebe.db.impl;

import io.zeebe.db.ZbValue;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public final class ZbNil implements ZbValue {

  public static final ZbNil INSTANCE = new ZbNil();

  private final byte existenceByte = (byte) -1;

  private ZbNil() {}

  @Override
  public void wrap(DirectBuffer directBuffer, int offset, int length) {
    // nothing to do
  }

  @Override
  public int getLength() {
    return Byte.BYTES;
  }

  @Override
  public void write(MutableDirectBuffer mutableDirectBuffer, int offset) {
    mutableDirectBuffer.putByte(offset, existenceByte);
  }
}
