/*
 * Copyright 2018 OrientDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfigurationImpl;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMapBucket;
import com.orientechnologies.orient.core.storage.cluster.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.cluster.OPaginatedClusterDebug;
import com.orientechnologies.orient.core.storage.cluster.v0.OPaginatedClusterStateV0;
import com.orientechnologies.orient.core.storage.cluster.v0.OPaginatedClusterV0;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OClusterBrowsePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import java.io.File;
import java.io.IOException;

/**
 *
 * @author marko
 */
public class OFastRidBagPaginatedCluster extends OPaginatedCluster{

  private OPaginatedClusterV0.FindFreePageResult findFreePage(int contentSize, OAtomicOperation atomicOperation) throws IOException {
    while (true) {
      int freePageIndex = contentSize / ONE_KB;
      freePageIndex -= PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY.getValueAsInteger();
      if (freePageIndex < 0) {
        freePageIndex = 0;
      }

      long pageIndex;

      final OCacheEntry pinnedStateEntry = loadPageForRead(atomicOperation, fileId, pinnedStateEntryIndex, true);
      if (pinnedStateEntry == null) {
        loadPageForRead(atomicOperation, fileId, pinnedStateEntryIndex, true);
      }
      try {
        OPaginatedClusterStateV0 freePageLists = new OPaginatedClusterStateV0(pinnedStateEntry);
        do {
          pageIndex = freePageLists.getFreeListPage(freePageIndex);
          freePageIndex++;
        } while (pageIndex < 0 && freePageIndex < FREE_LIST_SIZE);

      } finally {
        releasePageFromRead(atomicOperation, pinnedStateEntry);
      }

      if (pageIndex < 0) {
        pageIndex = getFilledUpTo(atomicOperation, fileId);
      } else {
        freePageIndex--;
      }

      if (freePageIndex < FREE_LIST_SIZE) {
        OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, false);

        //free list is broken automatically fix it
        if (cacheEntry == null) {
          updateFreePagesList(freePageIndex, -1, atomicOperation);

          continue;
        } else {
          int realFreePageIndex;
          try {
            OClusterPage localPage = new OClusterPage(cacheEntry, false);
            realFreePageIndex = calculateFreePageIndex(localPage);
          } finally {
            releasePageFromWrite(atomicOperation, cacheEntry);
          }

          if (realFreePageIndex != freePageIndex) {
            OLogManager.instance()
                .warn(this, "Page in file %s with index %d was placed in wrong free list, this error will be fixed automatically",
                    getFullName(), pageIndex);

            updateFreePagesIndex(freePageIndex, pageIndex, atomicOperation);
            continue;
          }
        }
      }

      return new OPaginatedClusterV0.FindFreePageResult(pageIndex, freePageIndex);
    }
  }
  
  private AddEntryResult addEntry(final int recordVersion, byte[] entryContent, OAtomicOperation atomicOperation)
      throws IOException {
    final FindFreePageResult findFreePageResult = findFreePage(entryContent.length, atomicOperation);

    int freePageIndex = findFreePageResult.freePageIndex;
    long pageIndex = findFreePageResult.pageIndex;

    boolean newRecord = freePageIndex >= FREE_LIST_SIZE;

    OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, false);
    if (cacheEntry == null) {
      cacheEntry = addPage(atomicOperation, fileId, false);
    }

    int recordSizesDiff;
    int position;
    final int finalVersion;

    try {
      final OClusterPage localPage = new OClusterPage(cacheEntry, newRecord);
      assert newRecord || freePageIndex == calculateFreePageIndex(localPage);

      int initialFreeSpace = localPage.getFreeSpace();

      position = localPage.appendRecord(recordVersion, entryContent);

      if (position < 0) {
        localPage.dumpToLog();
        throw new IllegalStateException(
            "Page " + cacheEntry.getPageIndex() + " does not have enough free space to add record content, freePageIndex="
                + freePageIndex + ", entryContent.length=" + entryContent.length);
      }

      finalVersion = localPage.getRecordVersion(position);

      int freeSpace = localPage.getFreeSpace();
      recordSizesDiff = initialFreeSpace - freeSpace;
    } finally {
      releasePageFromWrite(atomicOperation, cacheEntry);
    }

    updateFreePagesIndex(freePageIndex, pageIndex, atomicOperation);

    return new OPaginatedClusterV0.AddEntryResult(pageIndex, position, finalVersion, recordSizesDiff);
  }
  
  public OFastRidBagPaginatedCluster(OAbstractPaginatedStorage storage, String name, String extension, String lockName) {
    super(storage, name, extension, lockName);
  }

  @Override
  public void replaceFile(File file) throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void replaceClusterMapFile(File file) throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public RECORD_STATUS getRecordStatus(long clusterPosition) throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public OPaginatedClusterDebug readDebug(long clusterPosition) throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void registerInStorageConfig(OStorageConfigurationImpl root) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public long getFileId() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void configure(OStorage iStorage, int iId, String iClusterName, Object... iParameters) throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void configure(OStorage iStorage, OStorageClusterConfiguration iConfig) throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void create(int iStartSize) throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void open() throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void close(boolean flush) throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void delete() throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Object set(ATTRIBUTES iAttribute, Object iValue) throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public String encryption() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public long getTombstonesCount() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void truncate() throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public OPhysicalPosition allocatePosition(byte recordType) throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  private static int getEntryContentLength(int grownContentSize) {
    int entryContentLength =
        grownContentSize + 2 * OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE;

    return entryContentLength;
  }
  
  @Override
  public OPhysicalPosition createRecord(byte[] content, int recordVersion, byte recordType, OPhysicalPosition allocatedPosition) throws IOException {    

    boolean rollback = false;
    OAtomicOperation atomicOperation = startAtomicOperation(true);
    try {
      acquireExclusiveLock();
      try {
        int entryContentLength = getEntryContentLength(content.length);

        if (entryContentLength < OClusterPage.MAX_RECORD_SIZE) {
          byte[] entryContent = new byte[entryContentLength];

          int entryPosition = 0;
          entryContent[entryPosition] = recordType;
          entryPosition++;

          OIntegerSerializer.INSTANCE.serializeNative(content.length, entryContent, entryPosition);
          entryPosition += OIntegerSerializer.INT_SIZE;

          System.arraycopy(content, 0, entryContent, entryPosition, content.length);
          entryPosition += content.length;

          entryContent[entryPosition] = 1;
          entryPosition++;

          OLongSerializer.INSTANCE.serializeNative(-1L, entryContent, entryPosition);

          final AddEntryResult addEntryResult = addEntry(recordVersion, entryContent, atomicOperation);

          updateClusterState(1, addEntryResult.recordsSizeDiff, atomicOperation);

          final long clusterPosition;
          if (allocatedPosition != null) {
            clusterPositionMap.update(allocatedPosition.clusterPosition,
                new OClusterPositionMapBucket.PositionEntry(addEntryResult.pageIndex, addEntryResult.pagePosition),
                atomicOperation);
            clusterPosition = allocatedPosition.clusterPosition;
          } else {
            clusterPosition = clusterPositionMap.add(addEntryResult.pageIndex, addEntryResult.pagePosition, atomicOperation);
          }

          addAtomicOperationMetadata(new ORecordId(id, clusterPosition), atomicOperation);

          return createPhysicalPosition(recordType, clusterPosition, addEntryResult.recordVersion);
        } else {
          int entrySize = content.length + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE;

          int fullEntryPosition = 0;
          byte[] fullEntry = new byte[entrySize];

          fullEntry[fullEntryPosition] = recordType;
          fullEntryPosition++;

          OIntegerSerializer.INSTANCE.serializeNative(content.length, fullEntry, fullEntryPosition);
          fullEntryPosition += OIntegerSerializer.INT_SIZE;

          System.arraycopy(content, 0, fullEntry, fullEntryPosition, content.length);

          long prevPageRecordPointer = -1;
          long firstPageIndex = -1;
          int firstPagePosition = -1;

          int version = 0;

          int from = 0;
          int to = from + (OClusterPage.MAX_RECORD_SIZE - OByteSerializer.BYTE_SIZE - OLongSerializer.LONG_SIZE);

          int recordsSizeDiff = 0;

          do {
            byte[] entryContent = new byte[to - from + OByteSerializer.BYTE_SIZE + OLongSerializer.LONG_SIZE];
            System.arraycopy(fullEntry, from, entryContent, 0, to - from);

            if (from > 0) {
              entryContent[entryContent.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE] = 0;
            } else {
              entryContent[entryContent.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE] = 1;
            }

            OLongSerializer.INSTANCE.serializeNative(-1L, entryContent, entryContent.length - OLongSerializer.LONG_SIZE);

            final OPaginatedClusterV0.AddEntryResult addEntryResult = addEntry(recordVersion, entryContent, atomicOperation);
            recordsSizeDiff += addEntryResult.recordsSizeDiff;

            if (firstPageIndex == -1) {
              firstPageIndex = addEntryResult.pageIndex;
              firstPagePosition = addEntryResult.pagePosition;
              version = addEntryResult.recordVersion;
            }

            long addedPagePointer = createPagePointer(addEntryResult.pageIndex, addEntryResult.pagePosition);
            if (prevPageRecordPointer >= 0) {
              long prevPageIndex = getPageIndex(prevPageRecordPointer);
              int prevPageRecordPosition = getRecordPosition(prevPageRecordPointer);

              final OCacheEntry prevPageCacheEntry = loadPageForWrite(atomicOperation, fileId, prevPageIndex, false);
              try {
                final OClusterPage prevPage = new OClusterPage(prevPageCacheEntry, false);
                prevPage.setRecordLongValue(prevPageRecordPosition, -OLongSerializer.LONG_SIZE, addedPagePointer);
              } finally {
                releasePageFromWrite(atomicOperation, prevPageCacheEntry);
              }
            }

            prevPageRecordPointer = addedPagePointer;
            from = to;
            to = to + (OClusterPage.MAX_RECORD_SIZE - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE);
            if (to > fullEntry.length) {
              to = fullEntry.length;
            }

          } while (from < to);

          updateClusterState(1, recordsSizeDiff, atomicOperation);
          final long clusterPosition;
          if (allocatedPosition != null) {
            clusterPositionMap.update(allocatedPosition.clusterPosition,
                new OClusterPositionMapBucket.PositionEntry(firstPageIndex, firstPagePosition), atomicOperation);
            clusterPosition = allocatedPosition.clusterPosition;
          } else {
            clusterPosition = clusterPositionMap.add(firstPageIndex, firstPagePosition, atomicOperation);
          }

          addAtomicOperationMetadata(new ORecordId(id, clusterPosition), atomicOperation);
          return createPhysicalPosition(recordType, clusterPosition, version);

        }
      } finally {
        releaseExclusiveLock();
      }
    } catch (Exception e) {
      rollback = true;
      throw e;
    } finally {
      endAtomicOperation(rollback);
    }
  }

  @Override
  public boolean deleteRecord(long clusterPosition) throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void updateRecord(long clusterPosition, byte[] content, int recordVersion, byte recordType) throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void recycleRecord(long clusterPosition, byte[] content, int recordVersion, byte recordType) throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public ORawBuffer readRecord(long clusterPosition, boolean prefetchRecords) throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public ORawBuffer readRecordIfVersionIsNotLatest(long clusterPosition, int recordVersion) throws IOException, ORecordNotFoundException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public OPhysicalPosition getPhysicalPosition(OPhysicalPosition iPPosition) throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public boolean isDeleted(OPhysicalPosition iPPosition) throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public long getEntries() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public long getFirstPosition() throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public long getLastPosition() throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public long getNextPosition() throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public String getFileName() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public int getId() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void synch() throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public long getRecordsSize() throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public float recordGrowFactor() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public String compression() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public float recordOverflowGrowFactor() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public boolean isSystemCluster() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public OPhysicalPosition[] higherPositions(OPhysicalPosition position) throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public OPhysicalPosition[] ceilingPositions(OPhysicalPosition position) throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public OPhysicalPosition[] lowerPositions(OPhysicalPosition position) throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public OPhysicalPosition[] floorPositions(OPhysicalPosition position) throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public boolean hideRecord(long position) throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public ORecordConflictStrategy getRecordConflictStrategy() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void acquireAtomicExclusiveLock() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public OClusterBrowsePage nextPage(long lastPosition) throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public int getBinaryVersion() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }  
  
  public OIdentifiable addItem(OIdentifiable ridbagRid, OIdentifiable itemRid){
    
  }

  public OIdentifiable preAllocateRidBagNode(int i) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }
  

  
}
