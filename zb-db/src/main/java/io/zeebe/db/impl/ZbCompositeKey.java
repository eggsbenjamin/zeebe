package io.zeebe.db.impl;

import io.zeebe.db.ZbKey;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public class ZbCompositeKey<FirstKeyType extends ZbKey, SecondKeyType extends ZbKey>
    implements ZbKey {

  private FirstKeyType firstKeyTypePart;
  private SecondKeyType secondKeyTypePart;

  public void wrapKeys(FirstKeyType firstKeyType, SecondKeyType secondKeyType) {
    this.firstKeyTypePart = firstKeyType;
    this.secondKeyTypePart = secondKeyType;
  }

  public FirstKeyType getFirst() {
    return firstKeyTypePart;
  }

  public SecondKeyType getSecond() {
    return secondKeyTypePart;
  }

  @Override
  public void wrap(DirectBuffer directBuffer, int offset, int length) {
    firstKeyTypePart.wrap(directBuffer, offset, length);
    final int firstKeyLength = firstKeyTypePart.getLength();
    secondKeyTypePart.wrap(directBuffer, offset + firstKeyLength, length - firstKeyLength);
  }

  @Override
  public int getLength() {
    return firstKeyTypePart.getLength() + secondKeyTypePart.getLength();
  }

  @Override
  public void write(MutableDirectBuffer mutableDirectBuffer, int offset) {
    firstKeyTypePart.write(mutableDirectBuffer, offset);
    final int firstKeyPartLength = firstKeyTypePart.getLength();
    secondKeyTypePart.write(mutableDirectBuffer, offset + firstKeyPartLength);
  }
}
