package io.zeebe.db.impl;

import io.zeebe.db.ZbKey;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public class ZbCompositeKey<FirstKey extends ZbKey, SecondKey extends ZbKey> implements ZbKey {

  private FirstKey firstKeyPart;
  private SecondKey secondKeyPart;

  public void wrapKeys(FirstKey firstKey, SecondKey secondKey) {
    this.firstKeyPart = firstKey;
    this.secondKeyPart = secondKey;
  }

  @Override
  public void wrap(DirectBuffer directBuffer, int offset, int length) {
    firstKeyPart.wrap(directBuffer, offset, length);
    final int firstKeyLength = firstKeyPart.getLength();
    secondKeyPart.wrap(directBuffer, offset + firstKeyLength, length - firstKeyLength);
  }

  @Override
  public int getLength() {
    return firstKeyPart.getLength() + secondKeyPart.getLength();
  }

  @Override
  public void write(MutableDirectBuffer mutableDirectBuffer, int offset) {
    firstKeyPart.write(mutableDirectBuffer, offset);
    final int firstKeyPartLength = firstKeyPart.getLength();
    secondKeyPart.write(mutableDirectBuffer, offset + firstKeyPartLength);
  }
}
