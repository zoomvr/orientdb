package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.distributed;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAbstractWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class DistributedTxInfoWALRecord extends OAbstractWALRecord {

  protected byte[] content;

  public DistributedTxInfoWALRecord() {

  }

  public DistributedTxInfoWALRecord(byte[] content) {
    this.content = content;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    final ByteBuffer buffer = ByteBuffer.wrap(content).order(ByteOrder.nativeOrder());
    buffer.position(offset);
    buffer.putInt(this.content.length);
    buffer.put(this.content);
    return buffer.position();
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    buffer.putInt(content.length);
    buffer.put(content);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    final ByteBuffer buffer = ByteBuffer.wrap(content).order(ByteOrder.nativeOrder());
    buffer.position(offset);
    int size = buffer.getInt();
    this.content = new byte[size];
    buffer.get(this.content);
    return buffer.position();
  }

  @Override
  public int serializedSize() {
    return content.length + 4;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public int getId() {
    return WALRecordTypes.DISTRIBUTED_TX_INFO_PO;
  }

  public byte[] getContent() {
    return content;
  }
}
