package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.paginatedcluster;

import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.OComponentOperationRecord;

import java.io.IOException;
import java.nio.ByteBuffer;

public class OPaginatedClusterAllocatePositionCO extends OComponentOperationRecord {
  private int  clusterId;
  private byte recordType;

  public OPaginatedClusterAllocatePositionCO() {
  }

  public OPaginatedClusterAllocatePositionCO(final int clusterId, final byte recordType) {
    this.clusterId = clusterId;
    this.recordType = recordType;
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    //do nothing
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    //do nothing
  }

  @Override
  public void redo(final OAbstractPaginatedStorage storage) throws IOException {
    storage.allocatePositionInternal(clusterId, recordType);
  }

  @Override
  public void undo(final OAbstractPaginatedStorage storage) {
    //do nothing
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_ALLOCATE_RECORD_POSITION_CO;
  }
}
