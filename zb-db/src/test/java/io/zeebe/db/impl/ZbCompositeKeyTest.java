package io.zeebe.db.impl;

import static io.zeebe.db.impl.ZeebeDbConstants.ZB_DB_BYTE_ORDER;
import static org.assertj.core.api.Assertions.assertThat;

import org.agrona.ExpandableArrayBuffer;
import org.junit.Test;

public class ZbCompositeKeyTest {

  @Test
  public void shouldWriteLongLongCompositeKey() {
    // given
    final ZbLong firstLong = new ZbLong();
    final ZbLong secondLong = new ZbLong();
    final ZbCompositeKey<ZbLong, ZbLong> compositeKey = new ZbCompositeKey<>();

    firstLong.wrapLong(23);
    secondLong.wrapLong(121);
    compositeKey.wrapKeys(firstLong, secondLong);

    // when
    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    compositeKey.write(buffer, 0);

    // then
    assertThat(compositeKey.getLength()).isEqualTo(Long.BYTES * 2);
    assertThat(buffer.getLong(0, ZB_DB_BYTE_ORDER)).isEqualTo(23);
    assertThat(buffer.getLong(Long.BYTES, ZB_DB_BYTE_ORDER)).isEqualTo(121);
  }

  @Test
  public void shouldWrapLongLongCompositeKey() {
    // given
    final ZbLong firstLong = new ZbLong();
    final ZbLong secondLong = new ZbLong();
    final ZbCompositeKey<ZbLong, ZbLong> compositeKey = new ZbCompositeKey<>();
    compositeKey.wrapKeys(firstLong, secondLong);

    // when
    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    buffer.putLong(0, 23, ZB_DB_BYTE_ORDER);
    buffer.putLong(Long.BYTES, 121, ZB_DB_BYTE_ORDER);
    compositeKey.wrap(buffer, 0, Long.BYTES * 2);

    // then
    assertThat(compositeKey.getLength()).isEqualTo(Long.BYTES * 2);
    assertThat(compositeKey.getFirst().getValue()).isEqualTo(23);
    assertThat(firstLong.getValue()).isEqualTo(23);
    assertThat(compositeKey.getSecond().getValue()).isEqualTo(121);
    assertThat(secondLong.getValue()).isEqualTo(121);
  }

  @Test
  public void shouldWriteStringLongCompositeKey() {
    // given
    final ZbString firstString = new ZbString();
    final ZbLong secondLong = new ZbLong();
    final ZbCompositeKey<ZbString, ZbLong> compositeKey = new ZbCompositeKey<>();

    firstString.wrapString("foo");
    secondLong.wrapLong(121);
    compositeKey.wrapKeys(firstString, secondLong);

    // when
    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    compositeKey.write(buffer, 0);

    // then
    assertThat(compositeKey.getLength()).isEqualTo(Long.BYTES + 3 + Integer.BYTES);

    assertThat(buffer.getInt(0, ZB_DB_BYTE_ORDER)).isEqualTo(3);
    final byte[] bytes = new byte[3];
    buffer.getBytes(Integer.BYTES, bytes);
    assertThat(bytes).isEqualTo("foo".getBytes());

    assertThat(buffer.getLong(Integer.BYTES + 3, ZB_DB_BYTE_ORDER)).isEqualTo(121);
  }

  @Test
  public void shouldWrapStringLongCompositeKey() {
    // given
    final ZbString firstString = new ZbString();
    final ZbLong secondLong = new ZbLong();
    final ZbCompositeKey<ZbString, ZbLong> compositeKey = new ZbCompositeKey<>();

    compositeKey.wrapKeys(firstString, secondLong);

    // when
    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    buffer.putInt(0, 3, ZB_DB_BYTE_ORDER);
    buffer.putBytes(Integer.BYTES, "foo".getBytes());
    buffer.putLong(3 + Integer.BYTES, 121, ZB_DB_BYTE_ORDER);
    compositeKey.wrap(buffer, 0, Long.BYTES + 3 + Integer.BYTES);

    // then
    assertThat(compositeKey.getLength()).isEqualTo(Long.BYTES + 3 + Integer.BYTES);
    assertThat(compositeKey.getFirst().toString()).isEqualTo("foo");
    assertThat(firstString.toString()).isEqualTo("foo");
    assertThat(compositeKey.getSecond().getValue()).isEqualTo(121);
    assertThat(secondLong.getValue()).isEqualTo(121);
  }

  @Test
  public void shouldWriteNestedCompositeKey() {
    // given
    final ZbString firstString = new ZbString();
    final ZbLong secondLong = new ZbLong();
    final ZbLong thirdLong = new ZbLong();

    final ZbCompositeKey<ZbString, ZbLong> compositeKey = new ZbCompositeKey<>();
    final ZbCompositeKey<ZbCompositeKey<ZbString, ZbLong>, ZbLong> nestedCompositeKey =
        new ZbCompositeKey<>();

    firstString.wrapString("foo");
    secondLong.wrapLong(121);
    compositeKey.wrapKeys(firstString, secondLong);

    thirdLong.wrapLong(100_234L);
    nestedCompositeKey.wrapKeys(compositeKey, thirdLong);

    // when
    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    nestedCompositeKey.write(buffer, 0);

    // then
    assertThat(nestedCompositeKey.getLength())
        .isEqualTo(Long.BYTES + 3 + Integer.BYTES + Long.BYTES);

    int offset = 0;
    assertThat(buffer.getInt(offset, ZB_DB_BYTE_ORDER)).isEqualTo(3);
    offset += Integer.BYTES;

    final byte[] bytes = new byte[3];
    buffer.getBytes(offset, bytes);
    assertThat(bytes).isEqualTo("foo".getBytes());
    offset += 3;

    assertThat(buffer.getLong(offset, ZB_DB_BYTE_ORDER)).isEqualTo(121);
    offset += Long.BYTES;

    assertThat(buffer.getLong(offset, ZB_DB_BYTE_ORDER)).isEqualTo(100_234L);
  }

  @Test
  public void shouldWrapNestedCompositeKey() {
    // given

    final ZbString firstString = new ZbString();
    final ZbLong secondLong = new ZbLong();
    final ZbLong thirdLong = new ZbLong();

    final ZbCompositeKey<ZbString, ZbLong> compositeKey = new ZbCompositeKey<>();
    final ZbCompositeKey<ZbCompositeKey<ZbString, ZbLong>, ZbLong> nestedCompositeKey =
        new ZbCompositeKey<>();

    compositeKey.wrapKeys(firstString, secondLong);
    nestedCompositeKey.wrapKeys(compositeKey, thirdLong);

    // when
    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    int offset = 0;
    buffer.putInt(offset, 3, ZB_DB_BYTE_ORDER);
    offset += Integer.BYTES;
    buffer.putBytes(offset, "foo".getBytes());
    offset += 3;
    buffer.putLong(offset, 121, ZB_DB_BYTE_ORDER);
    offset += Long.BYTES;
    buffer.putLong(offset, 100_234L, ZB_DB_BYTE_ORDER);
    offset += Long.BYTES;
    nestedCompositeKey.wrap(buffer, 0, offset);

    // then
    assertThat(nestedCompositeKey.getLength())
        .isEqualTo(Long.BYTES + 3 + Integer.BYTES + Long.BYTES);

    final ZbCompositeKey<ZbString, ZbLong> composite = nestedCompositeKey.getFirst();
    assertThat(composite.getFirst().toString()).isEqualTo("foo");
    assertThat(firstString.toString()).isEqualTo("foo");

    assertThat(composite.getSecond().getValue()).isEqualTo(121);
    assertThat(secondLong.getValue()).isEqualTo(121);

    assertThat(nestedCompositeKey.getSecond().getValue()).isEqualTo(100_234L);
    assertThat(thirdLong.getValue()).isEqualTo(100_234L);
  }
}
