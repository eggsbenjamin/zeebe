package io.zeebe.db.impl;

import static org.assertj.core.api.Assertions.assertThat;

import org.agrona.ExpandableArrayBuffer;
import org.junit.Test;

public class ZbByteTest {

  private final ZbByte zbByte = new ZbByte();

  @Test
  public void shouldWrapByte() {
    // given
    zbByte.wrapByte((byte) 255);

    // when
    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    zbByte.write(buffer, 0);

    // then
    assertThat(zbByte.getLength()).isEqualTo(Byte.BYTES);
    assertThat(zbByte.getValue()).isEqualTo((byte) 255);
    assertThat(buffer.getByte(0)).isEqualTo((byte) 255);
  }

  @Test
  public void shouldWrap() {
    // given
    final ExpandableArrayBuffer valueBuffer = new ExpandableArrayBuffer();
    valueBuffer.putByte(0, (byte) 255);
    zbByte.wrap(valueBuffer, 0, 1);

    // when
    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    zbByte.write(buffer, 0);

    // then
    assertThat(zbByte.getLength()).isEqualTo(Byte.BYTES);
    assertThat(zbByte.getValue()).isEqualTo((byte) 255);
    assertThat(buffer.getByte(0)).isEqualTo((byte) 255);
  }
}
