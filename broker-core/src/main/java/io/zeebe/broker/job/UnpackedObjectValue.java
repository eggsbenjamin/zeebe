package io.zeebe.broker.job;

import io.zeebe.db.ZbValue;
import io.zeebe.msgpack.UnpackedObject;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/** */
public class UnpackedObjectValue implements ZbValue {

  private UnpackedObject value;

  public void wrapObject(UnpackedObject value) {
    this.value = value;
  }

  @Override
  public void wrap(DirectBuffer buffer, int offset, int length) {
    value.wrap(buffer, offset, length);
  }

  @Override
  public int getLength() {
    return value.getLength();
  }

  @Override
  public void write(MutableDirectBuffer buffer, int offset) {
    value.write(buffer, offset);
  }

  public UnpackedObject getObject() {
    return value;
  }
}
