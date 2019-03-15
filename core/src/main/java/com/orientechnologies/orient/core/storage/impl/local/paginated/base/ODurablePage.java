/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.storage.impl.local.paginated.base;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

import java.nio.ByteBuffer;

/**
 * Base page class for all durable data structures, that is data structures state of which can be consistently restored after system
 * crash but results of last operations in small interval before crash may be lost.
 * <p>
 * This page has several booked memory areas with following offsets at the beginning:
 * <ol>
 * <li>from 0 to 7 - Magic number</li>
 * <li>from 8 to 11 - crc32 of all page content, which is calculated by cache system just before save</li>
 * <li>from 12 to 23 - LSN of last operation which was stored for given page</li>
 * </ol>
 * <p>
 * Developer which will extend this class should use all page memory starting from {@link #NEXT_FREE_POSITION} offset.
 * All data structures which use this kind of pages should be derived from
 * {@link com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent} class.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 16.08.13
 */
public class ODurablePage {

  public static final    int MAGIC_NUMBER_OFFSET = 0;
  protected static final int CRC32_OFFSET        = MAGIC_NUMBER_OFFSET + OLongSerializer.LONG_SIZE;

  public static final int WAL_SEGMENT_OFFSET  = CRC32_OFFSET + OIntegerSerializer.INT_SIZE;
  public static final int WAL_POSITION_OFFSET = WAL_SEGMENT_OFFSET + OLongSerializer.LONG_SIZE;
  public static final int MAX_PAGE_SIZE_BYTES = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  public static final int NEXT_FREE_POSITION = WAL_POSITION_OFFSET + OLongSerializer.LONG_SIZE;

  private final OCacheEntry cacheEntry;

  private final OCachePointer pointer;

  public ODurablePage(final OCacheEntry cacheEntry) {
    assert cacheEntry != null;

    this.cacheEntry = cacheEntry;
    this.pointer = cacheEntry.getCachePointer();
  }

  @SuppressWarnings("unused")
  public static OLogSequenceNumber getLogSequenceNumberFromPage(final ByteBuffer buffer) {
    buffer.position(WAL_SEGMENT_OFFSET);
    final long segment = buffer.getLong();
    final long position = buffer.getLong();

    return new OLogSequenceNumber(segment, position);
  }

  /**
   * DO NOT DELETE THIS METHOD IT IS USED IN ENTERPRISE STORAGE
   * <p>
   * Copies content of page into passed in byte array.
   *
   * @param buffer Buffer from which data will be copied
   * @param data   Byte array to which data will be copied
   * @param offset Offset of data inside page
   * @param length Length of data to be copied
   */
  @SuppressWarnings("unused")
  public static void getPageData(final ByteBuffer buffer, final byte[] data, final int offset, final int length) {
    buffer.position(0);
    buffer.get(data, offset, length);
  }

  /**
   * DO NOT DELETE THIS METHOD IT IS USED IN ENTERPRISE STORAGE
   * <p>
   * Get value of LSN from the passed in offset in byte array.
   *
   * @param offset Offset inside of byte array from which LSN value will be read.
   * @param data   Byte array from which LSN value will be read.
   */
  @SuppressWarnings("unused")
  public static OLogSequenceNumber getLogSequenceNumber(final int offset, final byte[] data) {
    final long segment = OLongSerializer.INSTANCE.deserializeNative(data, offset + WAL_SEGMENT_OFFSET);
    final long position = OLongSerializer.INSTANCE.deserializeNative(data, offset + WAL_POSITION_OFFSET);

    return new OLogSequenceNumber(segment, position);
  }

  protected final int getIntValue(final int pageOffset) {
    final ByteBuffer buffer = pointer.getBuffer();
    assert buffer != null;

    return buffer.getInt(pageOffset);
  }

  protected final short getShortValue(final int pageOffset) {
    final ByteBuffer buffer = pointer.getBuffer();
    assert buffer != null;

    return buffer.getShort(pageOffset);
  }

  protected final long getLongValue(final int pageOffset) {
    final ByteBuffer buffer = pointer.getBuffer();
    assert buffer != null;

    return buffer.getLong(pageOffset);
  }

  protected final byte[] getBinaryValue(final int pageOffset, final int valLen) {
    final ByteBuffer buffer = pointer.getBufferDuplicate();
    final byte[] result = new byte[valLen];

    assert buffer != null;

    buffer.position(pageOffset);
    buffer.get(result);

    return result;
  }

  protected int getObjectSizeInDirectMemory(final OBinarySerializer binarySerializer, final int offset) {
    final ByteBuffer buffer = pointer.getBufferDuplicate();

    assert buffer != null;

    buffer.position(offset);
    return binarySerializer.getObjectSizeInByteBuffer(buffer);
  }

  protected <T> T deserializeFromDirectMemory(final OBinarySerializer<T> binarySerializer, final int offset) {
    final ByteBuffer buffer = pointer.getBufferDuplicate();

    assert buffer != null;

    buffer.position(offset);
    return binarySerializer.deserializeFromByteBufferObject(buffer);
  }

  protected byte getByteValue(final int pageOffset) {
    final ByteBuffer buffer = pointer.getBuffer();

    assert buffer != null;
    return buffer.get(pageOffset);
  }

  @SuppressWarnings("SameReturnValue")
  protected int setIntValue(final int pageOffset, final int value) {
    final ByteBuffer buffer = pointer.getBuffer();
    assert buffer != null;

    buffer.putInt(pageOffset, value);
    return OIntegerSerializer.INT_SIZE;
  }

  @SuppressWarnings("SameReturnValue")
  protected int setShortValue(final int pageOffset, final short value) {
    final ByteBuffer buffer = pointer.getBuffer();
    assert buffer != null;

    buffer.putShort(pageOffset, value);
    return OShortSerializer.SHORT_SIZE;
  }

  @SuppressWarnings("SameReturnValue")
  protected int setByteValue(final int pageOffset, final byte value) {
    final ByteBuffer buffer = pointer.getBuffer();
    assert buffer != null;
    buffer.put(pageOffset, value);

    return OByteSerializer.BYTE_SIZE;
  }

  @SuppressWarnings("SameReturnValue")
  protected int setLongValue(final int pageOffset, final long value) {
    final ByteBuffer buffer = pointer.getBuffer();
    assert buffer != null;

    buffer.putLong(pageOffset, value);

    return OLongSerializer.LONG_SIZE;
  }

  protected int setBinaryValue(final int pageOffset, final byte[] value) {
    if (value.length == 0) {
      return 0;
    }

    final ByteBuffer buffer = pointer.getBuffer();

    assert buffer != null;

    buffer.position(pageOffset);
    buffer.put(value);

    return value.length;
  }

  protected void moveData(final int from, final int to, final int len) {
    if (len == 0) {
      return;
    }

    final ByteBuffer buffer = pointer.getBuffer();
    assert buffer != null;

    final ByteBuffer rb = buffer.asReadOnlyBuffer();
    rb.position(from);
    rb.limit(from + len);

    buffer.position(to);
    buffer.put(rb);
  }

  public void setLsn(final OLogSequenceNumber lsn) {
    final ByteBuffer buffer = pointer.getBuffer();

    assert buffer != null;

    buffer.position(WAL_SEGMENT_OFFSET);

    buffer.putLong(lsn.getSegment());
    buffer.putLong(lsn.getPosition());
  }

  @Override
  public String toString() {
    if (cacheEntry != null) {
      return getClass().getSimpleName() + "{" + "fileId=" + cacheEntry.getFileId() + ", pageIndex=" + cacheEntry.getPageIndex()
          + '}';
    } else {
      return super.toString();
    }
  }
}
