package io.zeebe.broker.workflow.state;

import io.zeebe.db.ZbValue;
import io.zeebe.msgpack.UnpackedObject;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/** */
public class WrapperUnpacked implements ZbValue {

  UnpackedObject object;

  public void wrap(UnpackedObject object) {
    this.object = object;
  }

  @Override
  public void wrap(DirectBuffer buffer, int offset, int length) {}

  @Override
  public int getLength() {
    return 0;
  }

  @Override
  public void write(MutableDirectBuffer buffer, int offset) {}
}
