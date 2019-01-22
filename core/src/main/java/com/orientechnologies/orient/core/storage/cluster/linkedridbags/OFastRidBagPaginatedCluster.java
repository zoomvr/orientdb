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
package com.orientechnologies.orient.core.storage.cluster.linkedridbags;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.compression.OCompression;
import com.orientechnologies.orient.core.compression.OCompressionFactory;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DISK_CACHE_PAGE_SIZE;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfigurationImpl;
import com.orientechnologies.orient.core.config.OStoragePaginatedClusterConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.linked.OLinkedListRidBag;
import static com.orientechnologies.orient.core.db.record.ridbag.linked.OLinkedListRidBag.RECORD_TYPE_ARRAY_NODE;
import static com.orientechnologies.orient.core.db.record.ridbag.linked.OLinkedListRidBag.RECORD_TYPE_LINKED_NODE;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.encryption.OEncryptionFactory;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OPaginatedClusterException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.cluster.OClusterPageDebug;
import com.orientechnologies.orient.core.storage.cluster.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.cluster.OPaginatedClusterDebug;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OClusterBrowseEntry;
import com.orientechnologies.orient.core.storage.impl.local.OClusterBrowsePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordOperationMetadata;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author marko
 */
public class OFastRidBagPaginatedCluster extends OPaginatedCluster{    
  
  private static final int     STATE_ENTRY_INDEX = 0;
  public static final int     BINARY_VERSION    = -1;
  private final        boolean addRidMetadata    = OGlobalConfiguration.STORAGE_TRACK_CHANGED_RECORDS_IN_WAL.getValueAsBoolean();

  private static final int DISK_PAGE_SIZE           = DISK_CACHE_PAGE_SIZE.getValueAsInteger();
  private static final int LOWEST_FREELIST_BOUNDARY = PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY.getValueAsInteger();
  private final static int FREE_LIST_SIZE           = DISK_PAGE_SIZE - LOWEST_FREELIST_BOUNDARY;
  private static final int PAGE_INDEX_OFFSET        = 16;
  private static final int RECORD_POSITION_MASK     = 0xFFFF;
  private static final int ONE_KB                   = 1024;

  private volatile OCompression                          compression;
  private volatile OEncryption                           encryption;
  private final    boolean                               systemCluster;
  private          OFastRidbagPaginatedClusterPositionMap                 clusterPositionMap;
  private          OAbstractPaginatedStorage             storageLocal;
  private volatile int                                   id;
  private          long                                  fileId;
  private          OStoragePaginatedClusterConfiguration config;
  private          ORecordConflictStrategy               recordConflictStrategy;

  private static final class AddEntryResult {
    private final long pageIndex;
    private final int  pagePosition;

    private final int recordVersion;
    private final int recordsSizeDiff;

    AddEntryResult(long pageIndex, int pagePosition, int recordVersion, int recordsSizeDiff) {
      this.pageIndex = pageIndex;
      this.pagePosition = pagePosition;
      this.recordVersion = recordVersion;
      this.recordsSizeDiff = recordsSizeDiff;
    }
  }

  private static final class FindFreePageResult {
    private final long    pageIndex;
    private final int     freePageIndex;
    private final boolean allocateNewPage;

    private FindFreePageResult(long pageIndex, int freePageIndex, boolean allocateNewPage) {
      this.pageIndex = pageIndex;
      this.freePageIndex = freePageIndex;
      this.allocateNewPage = allocateNewPage;
    }
  }  

  public OFastRidBagPaginatedCluster(final String name, final OAbstractPaginatedStorage storage) {
    super(storage, name, ".rbc", name + ".rbc");

    systemCluster = OMetadataInternal.SYSTEM_CLUSTER.contains(name);
  }
  
  @Override
  public void configure(final OStorage storage, final int id, final String clusterName, final Object... parameters)
      throws IOException {
    acquireExclusiveLock();
    try {
      final OContextConfiguration ctxCfg = storage.getConfiguration().getContextConfiguration();
      final String cfgCompression = ctxCfg.getValueAsString(OGlobalConfiguration.STORAGE_COMPRESSION_METHOD);
      final String cfgEncryption = ctxCfg.getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD);
      final String cfgEncryptionKey = ctxCfg.getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY);

      config = new OStoragePaginatedClusterConfiguration(storage.getConfiguration(), id, clusterName, null, true,
          OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR, OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR,
          cfgCompression, cfgEncryption, cfgEncryptionKey, null, OStorageClusterConfiguration.STATUS.ONLINE, BINARY_VERSION);
      config.name = clusterName;

      init((OAbstractPaginatedStorage) storage, config);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public int getBinaryVersion() {
    return BINARY_VERSION;
  }

  @Override
  public void configure(final OStorage storage, final OStorageClusterConfiguration config) throws IOException {
    acquireExclusiveLock();
    try {
      init((OAbstractPaginatedStorage) storage, config);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void create(final int startSize) throws IOException {
    boolean rollback = false;
    final OAtomicOperation atomicOperation = startAtomicOperation(false);
    try {
      acquireExclusiveLock();
      try {
        fileId = addFile(atomicOperation, getFullName());

        initCusterState(atomicOperation);

        clusterPositionMap.create(atomicOperation);
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
  public void registerInStorageConfig(OStorageConfigurationImpl root) {
    root.addCluster(config);
    root.update();
  }

  @Override
  public void open() throws IOException {
    acquireExclusiveLock();
    try {
      final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
      fileId = openFile(atomicOperation, getFullName());
      clusterPositionMap.open(atomicOperation);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void replaceFile(File file) throws IOException {
    acquireExclusiveLock();
    try {
      final String tempFileName = file.getName() + "$temp";
      try {
        final long tempFileId = writeCache.addFile(tempFileName);
        writeCache.replaceFileContentWith(tempFileId, file.toPath());

        readCache.deleteFile(fileId, writeCache);
        writeCache.renameFile(tempFileId, getFullName());
        fileId = tempFileId;
      } finally {
        // If, for some reason, the temp file is still exists, wipe it out.

        final long tempFileId = writeCache.fileIdByName(tempFileName);
        if (tempFileId >= 0) {
          writeCache.deleteFile(tempFileId);
        }
      }
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void replaceClusterMapFile(File file) throws IOException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close() {
    close(true);
  }

  @Override
  public void close(final boolean flush) {
    acquireExclusiveLock();
    try {
      if (flush) {
        synch();
      }

      readCache.closeFile(fileId, flush, writeCache);
      clusterPositionMap.close(flush);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void delete() throws IOException {
    boolean rollback = false;
    final OAtomicOperation atomicOperation = startAtomicOperation(false);

    try {
      acquireExclusiveLock();
      try {
        deleteFile(atomicOperation, fileId);

        clusterPositionMap.delete(atomicOperation);
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
  public Object set(final ATTRIBUTES attribute, final Object value) throws IOException {
    if (attribute == null) {
      throw new IllegalArgumentException("attribute is null");
    }

    final String stringValue = value != null ? value.toString() : null;

    acquireExclusiveLock();
    try {

      switch (attribute) {
      case NAME:
        setNameInternal(stringValue);
        break;
      case RECORD_GROW_FACTOR:
        setRecordGrowFactorInternal(stringValue);
        break;
      case RECORD_OVERFLOW_GROW_FACTOR:
        setRecordOverflowGrowFactorInternal(stringValue);
        break;
      case CONFLICTSTRATEGY:
        setRecordConflictStrategy(stringValue);
        break;
      case STATUS: {
        if (stringValue == null) {
          throw new IllegalStateException("Value of attribute is null");
        }

        return storageLocal.setClusterStatus(id, OStorageClusterConfiguration.STATUS
            .valueOf(stringValue.toUpperCase(storageLocal.getConfiguration().getLocaleInstance())));
      }
      case ENCRYPTION:
        if (getEntries() > 0) {
          throw new IllegalArgumentException(
              "Cannot change encryption setting on cluster '" + getName() + "' because it is not empty");
        }
        setEncryptionInternal(stringValue,
            ODatabaseRecordThreadLocal.instance().get().getStorage().getConfiguration().getContextConfiguration()
                .getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY));
        break;
      default:
        throw new IllegalArgumentException("Runtime change of attribute '" + attribute + " is not supported");
      }

    } finally {
      releaseExclusiveLock();
    }

    return null;
  }

  @Override
  public boolean isSystemCluster() {
    return systemCluster;
  }

  @Override
  public float recordGrowFactor() {
    acquireSharedLock();
    try {
      return config.recordGrowFactor;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public float recordOverflowGrowFactor() {
    acquireSharedLock();
    try {
      return config.recordOverflowGrowFactor;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public String compression() {
    acquireSharedLock();
    try {
      return config.compression;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public String encryption() {
    acquireSharedLock();
    try {
      return config.encryption;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OPhysicalPosition allocatePosition(byte recordType) throws IOException{
    return allocatePosition(recordType, true);
  }
  
  private OPhysicalPosition allocatePosition(byte recordType, boolean lock) throws IOException {
    boolean rollback = false;
    OAtomicOperation atomicOperation;
    if (lock){
      atomicOperation = startAtomicOperation(true);
    }
    else{
      atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    }
    try{
      if (lock){
        acquireExclusiveLock();
      }
      try{
        final OPhysicalPosition pos = createPhysicalPosition(recordType, clusterPositionMap.allocate(atomicOperation), -1);
        addAtomicOperationMetadata(new ORecordId(id, pos.clusterPosition), atomicOperation);        
        return pos;        
      }
      finally{
        if (lock){
          releaseExclusiveLock();
        }
      }
    }
    catch (Exception exc){
      rollback = true;
      throw exc;
    }
    finally{
      if (lock){
        endAtomicOperation(rollback);
      }
    }
  }

  @Override
  public OPhysicalPosition createRecord(byte[] content, final int recordVersion, final byte recordType,
      OPhysicalPosition allocatedPosition) throws IOException {       
    throw new UnsupportedOperationException("This is specialized cluster. This method is not implemented");
  }
  
  /**
   * calculates linked node entry size
   * @return 
   */
  public static int getRidEntrySize(){
    return OByteSerializer.BYTE_SIZE + OLinkSerializer.RID_SIZE + OIntegerSerializer.INT_SIZE;
  }
  
  public int revertBytes(int value){
    int res = 0;
    for (int i = 0; i < 4; i++){
      int vl = value & 0xFF;
      res |= (int) vl;
      if (i < 3){
        res <<= 8;
      }
      value >>>= 8;
    }
    return res;
  }
  
  /**
   * add rid to existing ridbag node. Caller should take care of free space. 
   * @param rid
   * @param pageIndex
   * @param pagePosition
   * @throws IOException 
   */
  private void addRidToLinkedNode(final ORID rid, final OClusterPage localPage, 
          final long pageIndex, final int pagePosition) throws IOException{
    addRidToLinkedNodeUpdateAtOnce(rid, localPage, pageIndex, pagePosition);
  }
  
  private int prevReadSize = 0;
  
  private void addRidToLinkedNodePartialUpdate(final ORID rid, final long pageIndex, final int pagePosition) throws IOException {

    final byte[] content = new byte[getRidEntrySize()];
    int pos = OByteSerializer.BYTE_SIZE;
    OLinkSerializer.INSTANCE.serialize(rid, content, pos);
    pos += OLinkSerializer.RID_SIZE;
    OIntegerSerializer.INSTANCE.serialize(-1, content, pos);
    
    final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
   
    //this is first linked node entry so it is extended, skip rid info and refernce to next
    pos = getRidEntrySize();
    int currentSize = getRecordContentAsInteger(pageIndex, pagePosition, pos);
    currentSize = revertBytes(currentSize);
    prevReadSize = currentSize;
    pos += OIntegerSerializer.INT_SIZE;
    int previousRidPosition = getRecordContentAsInteger(pageIndex, pagePosition, pos);
    previousRidPosition = revertBytes(previousRidPosition);
    final AddEntryResult addEntryResult = addEntry(1, content, pageIndex, atomicOperation);
    //update reference to next rid in previuosly added rid entry
    //position is record type (byte) + rid size
    if (pagePosition != previousRidPosition){
      replaceRecordContent(pageIndex, previousRidPosition, revertBytes(addEntryResult.pagePosition), OByteSerializer.BYTE_SIZE + OLinkSerializer.RID_SIZE);
    }
    else{
      replaceRecordContent(pageIndex, pagePosition, revertBytes(addEntryResult.pagePosition), OByteSerializer.BYTE_SIZE + OLinkSerializer.RID_SIZE);
    }
    //now increment size, and update info about last added rid. That is done in first entry of node
    ++currentSize;                

    pos = getRidEntrySize();
    replaceRecordContent(pageIndex, pagePosition, revertBytes(currentSize), pos);
    //update info about current entry in this node
    pos += OIntegerSerializer.INT_SIZE;
    replaceRecordContent(pageIndex, pagePosition, revertBytes(addEntryResult.pagePosition), pos);

    updateClusterState(1, addEntryResult.recordsSizeDiff, atomicOperation);  
  }
    
  private void addRidToLinkedNodeUpdateAtOnce(final ORID rid, final OClusterPage localPage, 
          final long pageIndex, final int pagePosition) throws IOException {

    final byte[] content = new byte[getRidEntrySize()];
    int pos = OByteSerializer.BYTE_SIZE;
    OLinkSerializer.INSTANCE.serialize(rid, content, pos);
    pos += OLinkSerializer.RID_SIZE;
    OIntegerSerializer.INSTANCE.serialize(-1, content, pos);
    
    OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();          
    final byte[] firstEntryContent = getRidEntry(localPage, pagePosition);

    //this is first linked node entry so it is extended, skip rid info and refernce to next               
    pos = getRidEntrySize();
    int currentSize = OIntegerSerializer.INSTANCE.deserialize(firstEntryContent, pos);        
    pos += OIntegerSerializer.INT_SIZE;
    int previousRidPosition = OIntegerSerializer.INSTANCE.deserialize(firstEntryContent, pos);
    final AddEntryResult addEntryResult = addEntry(1, content, localPage, pageIndex, atomicOperation);

    //update reference to next rid in previuosly added rid entry
    //position is record type (byte) + rid size
    if (pagePosition != previousRidPosition){          
      replaceRecordContent(localPage, previousRidPosition, revertBytes(addEntryResult.pagePosition), OByteSerializer.BYTE_SIZE + OLinkSerializer.RID_SIZE);
    }
    else{        
      OIntegerSerializer.INSTANCE.serialize(addEntryResult.pagePosition, firstEntryContent, OByteSerializer.BYTE_SIZE + OLinkSerializer.RID_SIZE);
    }
    //now increment size, and update info about last added rid. That is done in first entry of node
    ++currentSize;                

    pos = getRidEntrySize();        
    OIntegerSerializer.INSTANCE.serialize(currentSize, firstEntryContent, pos);
    //update info about current entry in this node
    pos += OIntegerSerializer.INT_SIZE;        
    OIntegerSerializer.INSTANCE.serialize(addEntryResult.pagePosition, firstEntryContent, pos);
    replaceContent(localPage, pagePosition, firstEntryContent);

    updateClusterState(1, addEntryResult.recordsSizeDiff, atomicOperation);          
  }
  
  /**
   * 
   * @param rid
   * @param currentNodePageIndex
   * @param currentRidPagePosition, node position in page
   * @throws IOException 
   */
  private void addRidToArrayNode(final ORID rid, final OClusterPage localPage, 
          int currentRidPagePosition) throws IOException {                   
      //skip record type info
      int pos = OByteSerializer.BYTE_SIZE;
      //get last valid current index
      int currentIndex = getRecordContentAsInteger(localPage, currentRidPagePosition, pos);
      currentIndex = revertBytes(currentIndex);
      ++currentIndex;
      pos += OIntegerSerializer.INT_SIZE;
      //let caller take care of capacity, so following lines will be commented
      //serialize current index after record type info
      replaceRecordContent(localPage, currentRidPagePosition, revertBytes(currentIndex), OByteSerializer.BYTE_SIZE);
      pos = OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE * 2 + currentIndex * OLinkSerializer.RID_SIZE;
      //set new rid info
      byte[] ridSerialized = new byte[OLinkSerializer.RID_SIZE];
      OLinkSerializer.INSTANCE.serialize(rid, ridSerialized, 0);
      replaceRecordContent(localPage, currentRidPagePosition, ridSerialized, pos);       
  }
  
  /**
   * rids raw rid entry from page , specified by page position
   * @param pageIndex
   * @param ridPagePosition
   * @return
   * @throws IOException 
   */
  private byte[] getRidEntry(long pageIndex, int ridPagePosition) throws IOException{
    OCacheEntry cacheEntry = null;
    final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    try{      
      cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
      OClusterPage localPage = new OClusterPage(cacheEntry, false);
      int size = localPage.getRecordSize(ridPagePosition);
      byte[] content = localPage.getRecordBinaryValue(ridPagePosition, 0, size);
      return content;
    }
    finally{
      if (cacheEntry != null){
        releasePageFromRead(atomicOperation, cacheEntry);
      }      
    }
  }
  
  private byte[] getRidEntry(OClusterPage localPage, int ridPagePosition) throws IOException{
    int size = localPage.getRecordSize(ridPagePosition);
    byte[] content = localPage.getRecordBinaryValue(ridPagePosition, 0, size);
    return content;
  }
  
  private int getRecordContentAsInteger(long pageIndex, int ridPagePosition, int offset) throws IOException{
    OCacheEntry cacheEntry = null;
    final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    try{      
      cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
      OClusterPage localPage = new OClusterPage(cacheEntry, false);      
      return localPage.getRecordIntegerValue(ridPagePosition, offset);
    }
    finally{
      if (cacheEntry != null){
        releasePageFromRead(atomicOperation, cacheEntry);
      }      
    }
  }
  
  private int getRecordContentAsInteger(final OClusterPage localPage, int ridPagePosition, int offset) throws IOException{   
    return localPage.getRecordIntegerValue(ridPagePosition, offset);
  }
  
  private int getRecordContentAsIntegerFromEntryPointer(OClusterPage page, int entryPosition, int offset) throws IOException{    
    return page.getRecordIntegerValueFromEntryPosition(entryPosition, offset);    
  }
  
  /**
   * replaces content of rid entry specified with ridPagePosition, with new content
   * @param pageIndex
   * @param ridPagePosition
   * @param newContent
   * @throws IOException 
   */
  private void replaceContent(long pageIndex, int ridPagePosition, byte[] newContent) throws IOException{
    OCacheEntry cacheEntry = null;
    final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    try{      
      cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, false);
      OClusterPage localPage = new OClusterPage(cacheEntry, false);
      localPage.replaceRecord(ridPagePosition, newContent, 1);
    }
    finally{
      if (cacheEntry != null){
        releasePageFromWrite(atomicOperation, cacheEntry);
      }      
    }
  }
  
  private void replaceContent(final OClusterPage localPage, int ridPagePosition, byte[] newContent) throws IOException{
    localPage.replaceRecord(ridPagePosition, newContent, 1);
  }
  
  private void replaceRecordContent(final long pageIndex, final int ridPagePosition, final int value, final int offset) throws IOException{
    OCacheEntry cacheEntry = null;
    final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    try{      
      cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, false);
      OClusterPage localPage = new OClusterPage(cacheEntry, false);
      localPage.setRecordIntValue(ridPagePosition, offset, value);
    }
    finally{
      if (cacheEntry != null){
        releasePageFromWrite(atomicOperation, cacheEntry);
      }      
    }
  }
  
  private void replaceRecordContent(final OClusterPage localPage, final int ridPagePosition, final int value, final int offset) throws IOException{
    localPage.setRecordIntValue(ridPagePosition, offset, value);
  }
  
  private void replaceRecordContent(final long pageIndex, final int ridPagePosition, final byte[] value, final int offset) throws IOException{
    OCacheEntry cacheEntry = null;
    final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    try{      
      cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, false);
      OClusterPage localPage = new OClusterPage(cacheEntry, false);
      localPage.setRecordBinaryValue(ridPagePosition, offset, value);
    }
    finally{
      if (cacheEntry != null){
        releasePageFromWrite(atomicOperation, cacheEntry);
      }      
    }
  }
  
  private void replaceRecordContent(final OClusterPage localPage, final int ridPagePosition, 
          final byte[] value, final int offset) throws IOException{
    localPage.setRecordBinaryValue(ridPagePosition, offset, value);
  }
  
  private HelperClasses.Tuple<Long, Long> updatePrevNextNodeinfo(long nodeClusterPosition, 
          Long previousNodeClusterPosition, Long nextnodeClusterPosition) throws IOException{
    OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    
    OFastRidbagClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(nodeClusterPosition, 1, atomicOperation);
    if (previousNodeClusterPosition == null){
      previousNodeClusterPosition = positionEntry.getPreviousNodePosition();
    }
    if (nextnodeClusterPosition == null){
      nextnodeClusterPosition = positionEntry.getNextNodePosition();
    }
    positionEntry = new OFastRidbagClusterPositionMapBucket.PositionEntry(positionEntry.getPageIndex(), 
            positionEntry.getRecordPosition(), previousNodeClusterPosition, nextnodeClusterPosition);
    clusterPositionMap.update(nodeClusterPosition, positionEntry, atomicOperation);
    return new HelperClasses.Tuple<>(previousNodeClusterPosition, nextnodeClusterPosition);
  }    
  
  
  /**
   * should be used to create new link ridbag node
   * @param rid
   * @param allocatedPosition
   * @param previousNodePosition, node should make double linked list, this is cluster position of previous node
   * @param nextNodePosition, nodes should make double linked list , this is cluster position of next node
   * @return
   * @throws IOException 
   */
  private HelperClasses.Tuple<Long, Integer> addRid(final ORID rid, final OPhysicalPosition allocatedPosition,
          final Long previousNodePosition, final Long nextNodePosition) throws IOException {

    //only in first node entry, besides rid entry, want to have info about currentSize, info about last added entry
    byte[] content = new byte[OIntegerSerializer.INT_SIZE * 2 + getRidEntrySize()];
    int pos = 0;
    OByteSerializer.INSTANCE.serialize(OLinkedListRidBag.RECORD_TYPE_LINKED_NODE, content, pos);
    pos += OByteSerializer.BYTE_SIZE;
    OLinkSerializer.INSTANCE.serialize(rid, content, pos);
    pos += OLinkSerializer.RID_SIZE;
    //this is reference to next node (here no next one this is first and last so far
    OIntegerSerializer.INSTANCE.serialize(-1, content, pos);
    pos += OIntegerSerializer.INT_SIZE;
    //serialize current size
    OIntegerSerializer.INSTANCE.serialize(1, content, pos);
    pos += OIntegerSerializer.INT_SIZE;
    
    OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    
    //TODO in add entry maybe we want to find page with at least twice free space??
    final AddEntryResult addEntryResult = addEntry(1, content, atomicOperation);

    updateClusterState(1, addEntryResult.recordsSizeDiff, atomicOperation);

    final long clusterPosition;
    if (allocatedPosition != null) {
      clusterPositionMap.update(allocatedPosition.clusterPosition,
              new OFastRidbagClusterPositionMapBucket.PositionEntry(addEntryResult.pageIndex, addEntryResult.pagePosition, previousNodePosition, nextNodePosition),
              atomicOperation);
      clusterPosition = allocatedPosition.clusterPosition;
    } else {
      clusterPosition = clusterPositionMap.add(addEntryResult.pageIndex, addEntryResult.pagePosition,
              atomicOperation, previousNodePosition, nextNodePosition);
    }

    //write info about last added entry (actually this one)
    OIntegerSerializer.INSTANCE.serialize(addEntryResult.pagePosition, content, pos);
    replaceContent(addEntryResult.pageIndex, addEntryResult.pagePosition, content);                
    return new HelperClasses.Tuple<Long, Integer>(addEntryResult.pageIndex, addEntryResult.pagePosition);      
  }
  
  private HelperClasses.Tuple<Long, Integer> addRids(final ORID[] rids, final OPhysicalPosition allocatedPosition,
          final Long previousNodePosition, final Long nextNodePosition, int lastRealRidIndex) throws IOException {

    byte[] content = new byte[OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE * 2 + OLinkSerializer.RID_SIZE * rids.length];
    int pos = 0;
    OByteSerializer.INSTANCE.serialize(OLinkedListRidBag.RECORD_TYPE_ARRAY_NODE, content, pos);
    pos += OByteSerializer.BYTE_SIZE;
    OIntegerSerializer.INSTANCE.serialize(lastRealRidIndex, content, pos);
    pos += OIntegerSerializer.INT_SIZE;
    OIntegerSerializer.INSTANCE.serialize(rids.length, content, pos);
    pos += OIntegerSerializer.INT_SIZE;
    for (int i = 0; i < rids.length; i++){
      OLinkSerializer.INSTANCE.serialize(rids[i], content, pos);
      pos += OLinkSerializer.RID_SIZE;
    }
    
    OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();    
    final AddEntryResult addEntryResult = addEntry(1, content, atomicOperation);

    updateClusterState(1, addEntryResult.recordsSizeDiff, atomicOperation);

    final long clusterPosition;
    if (allocatedPosition != null) {
      clusterPositionMap.update(allocatedPosition.clusterPosition,
              new OFastRidbagClusterPositionMapBucket.PositionEntry(addEntryResult.pageIndex, addEntryResult.pagePosition, previousNodePosition, nextNodePosition),
              atomicOperation);
      clusterPosition = allocatedPosition.clusterPosition;
    } else {
      clusterPosition = clusterPositionMap.add(addEntryResult.pageIndex, addEntryResult.pagePosition,
              atomicOperation, previousNodePosition, nextNodePosition);
    }

    return new HelperClasses.Tuple<>(addEntryResult.pageIndex, addEntryResult.pagePosition);      
  }

  private void addAtomicOperationMetadata(ORID rid, OAtomicOperation atomicOperation) {
    if (!addRidMetadata) {
      return;
    }

    if (atomicOperation == null) {
      return;
    }

    ORecordOperationMetadata recordOperationMetadata = (ORecordOperationMetadata) atomicOperation
        .getMetadata(ORecordOperationMetadata.RID_METADATA_KEY);

    if (recordOperationMetadata == null) {
      recordOperationMetadata = new ORecordOperationMetadata();
      atomicOperation.addMetadata(recordOperationMetadata);
    }

    recordOperationMetadata.addRid(rid);
  }

  private static int getEntryContentLength(int grownContentSize) {
    int entryContentLength =
        grownContentSize + 2 * OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE;

    return entryContentLength;
  }

  @Override
  @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS")
  public ORawBuffer readRecord(final long clusterPosition, boolean prefetchRecords) throws IOException {
    int pagesToPrefetch = 1;

    if (prefetchRecords) {
      pagesToPrefetch = OGlobalConfiguration.QUERY_SCAN_PREFETCH_PAGES.getValueAsInteger();
    }

    return readRecord(clusterPosition, pagesToPrefetch);
  }

  private ORawBuffer readRecord(final long clusterPosition, final int pageCount) throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();

        final OFastRidbagClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap
            .get(clusterPosition, pageCount, atomicOperation);
        if (positionEntry == null) {
          return null;
        }

        return internalReadRecord(clusterPosition, positionEntry.getPageIndex(), positionEntry.getRecordPosition(), pageCount,
            atomicOperation);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  private ORawBuffer internalReadRecord(long clusterPosition, long pageIndex, int recordPosition, int pageCount,
      final OAtomicOperation atomicOperation) throws IOException {
    int recordVersion;
    final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false, pageCount);
    try {
      final OClusterPage localPage = new OClusterPage(cacheEntry, false);
      recordVersion = localPage.getRecordVersion(recordPosition);
    } finally {
      releasePageFromRead(atomicOperation, cacheEntry);
    }

    final byte[] fullContent = readFullEntry(clusterPosition, pageIndex, recordPosition, atomicOperation, pageCount);
    if (fullContent == null) {
      return null;
    }

    int fullContentPosition = 0;

    final byte recordType = fullContent[fullContentPosition];
    fullContentPosition++;

    final int readContentSize = OIntegerSerializer.INSTANCE.deserializeNative(fullContent, fullContentPosition);
    fullContentPosition += OIntegerSerializer.INT_SIZE;

    byte[] recordContent = Arrays.copyOfRange(fullContent, fullContentPosition, fullContentPosition + readContentSize);

    recordContent = encryption.decrypt(recordContent);
    recordContent = compression.uncompress(recordContent);

    return new ORawBuffer(recordContent, recordVersion, recordType);
  }

  @Override
  public ORawBuffer readRecordIfVersionIsNotLatest(long clusterPosition, final int recordVersion)
      throws IOException, ORecordNotFoundException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
        OFastRidbagClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(clusterPosition, 1, atomicOperation);

        if (positionEntry == null) {
          throw new ORecordNotFoundException(new ORecordId(id, clusterPosition),
              "Record for cluster with id " + id + " and position " + clusterPosition + " is absent.");
        }

        int recordPosition = positionEntry.getRecordPosition();
        long pageIndex = positionEntry.getPageIndex();

        int loadedRecordVersion;
        OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
        try {
          final OClusterPage localPage = new OClusterPage(cacheEntry, false);
          if (localPage.isDeleted(recordPosition)) {
            throw new ORecordNotFoundException(new ORecordId(id, clusterPosition),
                "Record for cluster with id " + id + " and position " + clusterPosition + " is absent.");
          }

          loadedRecordVersion = localPage.getRecordVersion(recordPosition);
        } finally {
          releasePageFromRead(atomicOperation, cacheEntry);
        }

        if (loadedRecordVersion > recordVersion) {
          return readRecord(clusterPosition, false);
        }

        return null;
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public boolean deleteRecord(long clusterPosition) throws IOException {
    HelperClasses.Tuple<Long, Integer> pageIndexPagePosition = getPageIndexAndPagePositionOfRecord(clusterPosition, true);
    return deleteRecord(clusterPosition, pageIndexPagePosition.getFirstVal(),
            pageIndexPagePosition.getSecondVal(), true);
  }
  
  private boolean deleteRecord(final long clusterPosition, final OClusterPage localPage, final long pageIndex,
          final int pagePosition) throws IOException {  
    final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    final int initialFreePageIndex = calculateFreePageIndex(localPage);

    if (localPage.isDeleted(pagePosition)) {
      throw new OPaginatedClusterException("Content of record " + new ORecordId(id, clusterPosition) + " was broken",
          this);
    }    

    final int initialFreeSpace = localPage.getFreeSpace();
    localPage.deleteRecord(pagePosition);

    final int removedContentSize = localPage.getFreeSpace() - initialFreeSpace;

    updateFreePagesIndex(initialFreePageIndex, pageIndex, atomicOperation);
    updateClusterState(-1, -removedContentSize, atomicOperation);
    clusterPositionMap.remove(clusterPosition, atomicOperation);
    addAtomicOperationMetadata(new ORecordId(id, clusterPosition), atomicOperation);
    return true;
  }
  
  private boolean deleteRecord(final long clusterPosition, final long pageIndex, final int pagePosition, final boolean lock) throws IOException {
    boolean rollback = false;    
    final OAtomicOperation atomicOperation;
    if (lock){
      atomicOperation = startAtomicOperation(true);
    }
    else{
      atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    }
    try {
      if (lock){
        acquireExclusiveLock();
      }
      try {
        int removedContentSize = 0;
        
        boolean cacheEntryReleased = false;
        OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, false);
        int initialFreePageIndex;
        try {
          OClusterPage localPage = new OClusterPage(cacheEntry, false);
          initialFreePageIndex = calculateFreePageIndex(localPage);

          if (localPage.isDeleted(pagePosition)) {
            if (removedContentSize == 0) {
              cacheEntryReleased = true;
              releasePageFromWrite(atomicOperation, cacheEntry);
              return false;
            } else {
              throw new OPaginatedClusterException("Content of record " + new ORecordId(id, clusterPosition) + " was broken",
                  this);
            }
          } else if (removedContentSize == 0) {
            releasePageFromWrite(atomicOperation, cacheEntry);

            cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, false);

            localPage = new OClusterPage(cacheEntry, false);
          }          

          int initialFreeSpace = localPage.getFreeSpace();
          localPage.deleteRecord(pagePosition);

          removedContentSize += localPage.getFreeSpace() - initialFreeSpace;                        
        } finally {
          if (!cacheEntryReleased) {
            releasePageFromWrite(atomicOperation, cacheEntry);              
          }
        }

        updateFreePagesIndex(initialFreePageIndex, pageIndex, atomicOperation);

        updateClusterState(-1, -removedContentSize, atomicOperation);

        clusterPositionMap.remove(clusterPosition, atomicOperation);
        addAtomicOperationMetadata(new ORecordId(id, clusterPosition), atomicOperation);

        return true;
      } finally {
        if (lock){
          releaseExclusiveLock();
        }
      }
    } catch (Exception e) {      
      rollback = true;
      throw e;
    } finally {
      if (lock){
        endAtomicOperation(rollback);
      }
    }
  }

  @Override
  public boolean hideRecord(long position) throws IOException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateRecord(final long clusterPosition, byte[] content, final int recordVersion, final byte recordType)
      throws IOException {
    throw new UnsupportedOperationException("not implemented");
  }

  /**
   * Recycles a deleted record.
   * @throws java.io.IOException
   */
  @Override
  public void recycleRecord(final long clusterPosition, byte[] content, final int recordVersion, final byte recordType)
      throws IOException {
    
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getTombstonesCount() {
    return 0;
  }

  @Override
  public void truncate() throws IOException {
    throw new UnsupportedOperationException("not implemented");
  }

  public OPhysicalPosition getPhysicalPositionInternal(OPhysicalPosition position) throws IOException{
    final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    long clusterPosition = position.clusterPosition;
    OFastRidbagClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(clusterPosition, 1, atomicOperation);

    if (positionEntry == null) {
      return null;
    }

    long pageIndex = positionEntry.getPageIndex();
    int recordPosition = positionEntry.getRecordPosition();

    OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
    try {
      final OClusterPage localPage = new OClusterPage(cacheEntry, false);
      if (localPage.isDeleted(recordPosition)) {
        return null;
      }

      final OPhysicalPosition physicalPosition = new OPhysicalPosition();
      physicalPosition.recordSize = -1;

      physicalPosition.recordType = localPage.getRecordByteValue(recordPosition, 0);
      physicalPosition.recordVersion = localPage.getRecordVersion(recordPosition);
      physicalPosition.clusterPosition = position.clusterPosition;

      return physicalPosition;
    } finally {
      releasePageFromRead(atomicOperation, cacheEntry);
    }
  }
  
  @Override
  public OPhysicalPosition getPhysicalPosition(OPhysicalPosition position) throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {        
        return getPhysicalPositionInternal(position);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public boolean isDeleted(OPhysicalPosition position) throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
        long clusterPosition = position.clusterPosition;
        final OFastRidbagClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(clusterPosition, 1, atomicOperation);

        if (positionEntry == null) {
          return false;
        }

        long pageIndex = positionEntry.getPageIndex();
        int recordPosition = positionEntry.getRecordPosition();

        OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
        try {
          final OClusterPage localPage = new OClusterPage(cacheEntry, false);
          if (localPage.isDeleted(recordPosition)) {
            return true;
          }
          return false;
        } finally {
          releasePageFromRead(atomicOperation, cacheEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public long getEntries() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
        final OCacheEntry pinnedStateEntry = loadPageForRead(atomicOperation, fileId, STATE_ENTRY_INDEX, true);
        try {
          return new OFastRidbagPaginatedClusterState(pinnedStateEntry).getSize();
        } finally {
          releasePageFromRead(atomicOperation, pinnedStateEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (IOException ioe) {
      throw OException
          .wrapException(new OPaginatedClusterException("Error during retrieval of size of '" + getName() + "' cluster", this),
              ioe);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public long getFirstPosition() throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
        return clusterPositionMap.getFirstPosition(atomicOperation);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public long getLastPosition() throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
        return clusterPositionMap.getLastPosition(atomicOperation);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public long getNextPosition() throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
        return clusterPositionMap.getNextPosition(atomicOperation);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public String getFileName() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        return writeCache.fileNameById(fileId);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public int getId() {
    return id;
  }

  /**
   * Returns the fileId used in disk cache.
   */
  public long getFileId() {
    return fileId;
  }

  @Override
  public void synch() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        writeCache.flush(fileId);
        clusterPositionMap.flush();
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public long getRecordsSize() throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();

        final OCacheEntry pinnedStateEntry = loadPageForRead(atomicOperation, fileId, STATE_ENTRY_INDEX, true);
        try {
          return new OFastRidbagPaginatedClusterState(pinnedStateEntry).getRecordsSize();
        } finally {
          releasePageFromRead(atomicOperation, pinnedStateEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public OPhysicalPosition[] higherPositions(OPhysicalPosition position) throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
        final long[] clusterPositions = clusterPositionMap.higherPositions(position.clusterPosition, atomicOperation);
        return convertToPhysicalPositions(clusterPositions);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public OPhysicalPosition[] ceilingPositions(OPhysicalPosition position) throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
        final long[] clusterPositions = clusterPositionMap.ceilingPositions(position.clusterPosition, atomicOperation);
        return convertToPhysicalPositions(clusterPositions);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public OPhysicalPosition[] lowerPositions(OPhysicalPosition position) throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
        final long[] clusterPositions = clusterPositionMap.lowerPositions(position.clusterPosition, atomicOperation);
        return convertToPhysicalPositions(clusterPositions);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public OPhysicalPosition[] floorPositions(OPhysicalPosition position) throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
        final long[] clusterPositions = clusterPositionMap.floorPositions(position.clusterPosition, atomicOperation);
        return convertToPhysicalPositions(clusterPositions);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public ORecordConflictStrategy getRecordConflictStrategy() {
    return recordConflictStrategy;
  }

  private void setRecordConflictStrategy(final String stringValue) {
    recordConflictStrategy = Orient.instance().getRecordConflictStrategy().getStrategy(stringValue);
    config.conflictStrategy = stringValue;
    ((OStorageConfigurationImpl) storageLocal.getConfiguration()).update();
  }

  private void updateClusterState(long sizeDiff, long recordsSizeDiff, OAtomicOperation atomicOperation) throws IOException {
    final OCacheEntry pinnedStateEntry = loadPageForWrite(atomicOperation, fileId, STATE_ENTRY_INDEX, false);
    try {
      OFastRidbagPaginatedClusterState paginatedClusterState = new OFastRidbagPaginatedClusterState(pinnedStateEntry);
      paginatedClusterState.setSize((int) (paginatedClusterState.getSize() + sizeDiff));
      paginatedClusterState.setRecordsSize((int) (paginatedClusterState.getRecordsSize() + recordsSizeDiff));
    } finally {
      releasePageFromWrite(atomicOperation, pinnedStateEntry);
    }
  }

  private void init(final OAbstractPaginatedStorage storage, final OStorageClusterConfiguration config) throws IOException {
    OFileUtils.checkValidName(config.getName());

    this.config = (OStoragePaginatedClusterConfiguration) config;
    this.compression = OCompressionFactory.INSTANCE.getCompression(this.config.compression, null);
    this.encryption = OEncryptionFactory.INSTANCE.getEncryption(this.config.encryption, this.config.encryptionKey);

    if (((OStoragePaginatedClusterConfiguration) config).conflictStrategy != null) {
      this.recordConflictStrategy = Orient.instance().getRecordConflictStrategy()
          .getStrategy(((OStoragePaginatedClusterConfiguration) config).conflictStrategy);
    }

    storageLocal = storage;

    this.id = config.getId();

    clusterPositionMap = new OFastRidbagPaginatedClusterPositionMap(storage, getName(), getFullName());
  }

  private void setEncryptionInternal(final String iMethod, final String iKey) {
    try {
      encryption = OEncryptionFactory.INSTANCE.getEncryption(iMethod, iKey);
      config.encryption = iMethod;
      ((OStorageConfigurationImpl) storageLocal.getConfiguration()).update();
    } catch (IllegalArgumentException e) {
      throw OException
          .wrapException(new OPaginatedClusterException("Invalid value for " + ATTRIBUTES.ENCRYPTION + " attribute", this), e);
    }
  }

  private void setRecordOverflowGrowFactorInternal(final String stringValue) {
    try {
      float growFactor = Float.parseFloat(stringValue);
      if (growFactor < 1) {
        throw new OPaginatedClusterException(ATTRIBUTES.RECORD_OVERFLOW_GROW_FACTOR + " cannot be less than 1", this);
      }

      config.recordOverflowGrowFactor = growFactor;
      ((OStorageConfigurationImpl) storageLocal.getConfiguration()).update();
    } catch (NumberFormatException nfe) {
      throw OException.wrapException(new OPaginatedClusterException(
          "Invalid value for cluster attribute " + ATTRIBUTES.RECORD_OVERFLOW_GROW_FACTOR + " was passed [" + stringValue + "]",
          this), nfe);
    }
  }

  private void setRecordGrowFactorInternal(String stringValue) {
    try {
      float growFactor = Float.parseFloat(stringValue);
      if (growFactor < 1) {
        throw new OPaginatedClusterException(ATTRIBUTES.RECORD_GROW_FACTOR + " cannot be less than 1", this);
      }

      config.recordGrowFactor = growFactor;
      ((OStorageConfigurationImpl) storageLocal.getConfiguration()).update();
    } catch (NumberFormatException nfe) {
      throw OException.wrapException(new OPaginatedClusterException(
          "Invalid value for cluster attribute " + ATTRIBUTES.RECORD_GROW_FACTOR + " was passed [" + stringValue + "]", this), nfe);
    }
  }

  private void setNameInternal(final String newName) throws IOException {

    writeCache.renameFile(fileId, newName + getExtension());
    clusterPositionMap.rename(newName);

    config.name = newName;
    storageLocal.renameCluster(getName(), newName);
    setName(newName);

    ((OStorageConfigurationImpl) storageLocal.getConfiguration()).update();
  }

  private static OPhysicalPosition createPhysicalPosition(final byte recordType, final long clusterPosition, final int version) {
    final OPhysicalPosition physicalPosition = new OPhysicalPosition();
    physicalPosition.recordType = recordType;
    physicalPosition.recordSize = -1;
    physicalPosition.clusterPosition = clusterPosition;
    physicalPosition.recordVersion = version;
    return physicalPosition;
  }

  @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS")
  private byte[] readFullEntry(final long clusterPosition, long pageIndex, int recordPosition,
      final OAtomicOperation atomicOperation, final int pageCount) throws IOException {
    final List<byte[]> recordChunks = new ArrayList<>();
    int contentSize = 0;

    long nextPagePointer;
    boolean firstEntry = true;
    do {
      OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false, pageCount);
      try {
        final OClusterPage localPage = new OClusterPage(cacheEntry, false);

        if (localPage.isDeleted(recordPosition)) {
          if (recordChunks.isEmpty()) {
            return null;
          } else {
            throw new OPaginatedClusterException("Content of record " + new ORecordId(id, clusterPosition) + " was broken", this);
          }
        }

        byte[] content = localPage.getRecordBinaryValue(recordPosition, 0, localPage.getRecordSize(recordPosition));

        if (firstEntry && content[content.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE] == 0) {
          return null;
        }

        recordChunks.add(content);
        nextPagePointer = OLongSerializer.INSTANCE.deserializeNative(content, content.length - OLongSerializer.LONG_SIZE);
        contentSize += content.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE;

        firstEntry = false;
      } finally {
        releasePageFromRead(atomicOperation, cacheEntry);
      }

      pageIndex = getPageIndex(nextPagePointer);
      recordPosition = getRecordPosition(nextPagePointer);
    } while (nextPagePointer >= 0);

    byte[] fullContent;
    if (recordChunks.size() == 1) {
      fullContent = recordChunks.get(0);
    } else {
      fullContent = new byte[contentSize + OLongSerializer.LONG_SIZE + OByteSerializer.BYTE_SIZE];
      int fullContentPosition = 0;
      for (byte[] recordChuck : recordChunks) {
        System.arraycopy(recordChuck, 0, fullContent, fullContentPosition,
            recordChuck.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE);
        fullContentPosition += recordChuck.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE;
      }
    }

    return fullContent;
  }

  private static long createPagePointer(long pageIndex, int pagePosition) {
    return pageIndex << PAGE_INDEX_OFFSET | pagePosition;
  }

  private static int getRecordPosition(long nextPagePointer) {
    return (int) (nextPagePointer & RECORD_POSITION_MASK);
  }

  private static long getPageIndex(long nextPagePointer) {
    return nextPagePointer >>> PAGE_INDEX_OFFSET;
  }

  private AddEntryResult addEntry(final int recordVersion, byte[] entryContent, 
          OClusterPage localPage, long pageIndex, OAtomicOperation atomicOperation) throws IOException{
    
    int recordSizesDiff;
    int position;
    final int finalVersion;
    Integer freePageIndex = null;

    freePageIndex = calculateFreePageIndex(localPage);

    int initialFreeSpace = localPage.getFreeSpace();

    position = localPage.appendRecord(recordVersion, entryContent);

    if (position < 0) {        
      localPage.dumpToLog();        
      throw new IllegalStateException(
          "Page " + pageIndex + " does not have enough free space to add record content, freePageIndex="
              + freePageIndex + ", entryContent.length=" + entryContent.length);        
    }

    finalVersion = localPage.getRecordVersion(position);

    int freeSpace = localPage.getFreeSpace();
    recordSizesDiff = initialFreeSpace - freeSpace;
    
    updateFreePagesIndex(freePageIndex, pageIndex, atomicOperation);

    return new AddEntryResult(pageIndex, position, finalVersion, recordSizesDiff);
  }
  
  /**
   * adds entry to specified page, caller should take care of free space
   * @param recordVersion
   * @param entryContent
   * @param pageIndex
   * @param atomicOperation
   * @return 
   */
  private AddEntryResult addEntry(final int recordVersion, byte[] entryContent, 
          long pageIndex, OAtomicOperation atomicOperation) throws IOException{
    OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, false);

    try {
      final OClusterPage localPage = new OClusterPage(cacheEntry, false);
      return addEntry(recordVersion, entryContent, localPage, pageIndex, atomicOperation);
    } finally {
      releasePageFromWrite(atomicOperation, cacheEntry);
    }
  }
  
  /**
   * finds free page and store entry
   * @param recordVersion
   * @param entryContent
   * @param atomicOperation
   * @return
   * @throws IOException 
   */
  private AddEntryResult addEntry(final int recordVersion, byte[] entryContent, OAtomicOperation atomicOperation)
      throws IOException {
    final FindFreePageResult findFreePageResult = findFreePage(entryContent.length, atomicOperation);

    final int freePageIndex = findFreePageResult.freePageIndex;
    final long pageIndex = findFreePageResult.pageIndex;

    final boolean newPage = findFreePageResult.allocateNewPage;

    final OCacheEntry cacheEntry;
    if (newPage) {
      final OCacheEntry stateCacheEntry = loadPageForWrite(atomicOperation, fileId, STATE_ENTRY_INDEX, false);
      try {
        final OFastRidbagPaginatedClusterState clusterState = new OFastRidbagPaginatedClusterState(stateCacheEntry);
        final int fileSize = clusterState.getFileSize();
        final long filledUpTo = getFilledUpTo(atomicOperation, fileId);

        if (fileSize == filledUpTo - 1) {
          cacheEntry = addPage(atomicOperation, fileId, false);
        } else {
          assert fileSize < filledUpTo - 1;

          cacheEntry = loadPageForWrite(atomicOperation, fileId, fileSize + 1, false);
        }

        clusterState.setFileSize(fileSize + 1);
      } finally {
        releasePageFromWrite(atomicOperation, stateCacheEntry);
      }
    } else {
      cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, false);
    }

    int recordSizesDiff;
    int position;
    final int finalVersion;

    try {
      final OClusterPage localPage = new OClusterPage(cacheEntry, newPage);
      assert newPage || freePageIndex == calculateFreePageIndex(localPage);

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

    return new AddEntryResult(pageIndex, position, finalVersion, recordSizesDiff);
  }

  private FindFreePageResult findFreePage(int contentSize, OAtomicOperation atomicOperation) throws IOException {
    int freePageIndex = contentSize / ONE_KB;
    freePageIndex -= PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY.getValueAsInteger();
    if (freePageIndex < 0) {
      freePageIndex = 0;
    }

    long pageIndex;

    final OCacheEntry pinnedStateEntry = loadPageForRead(atomicOperation, fileId, STATE_ENTRY_INDEX, true);
    try {
      OFastRidbagPaginatedClusterState freePageLists = new OFastRidbagPaginatedClusterState(pinnedStateEntry);
      do {
        pageIndex = freePageLists.getFreeListPage(freePageIndex);
        freePageIndex++;
      } while (pageIndex < 0 && freePageIndex < FREE_LIST_SIZE);

    } finally {
      releasePageFromRead(atomicOperation, pinnedStateEntry);
    }

    final boolean allocateNewPage;

    if (pageIndex < 0) {
      final int fileSize;

      final OCacheEntry stateCacheEntry = loadPageForRead(atomicOperation, fileId, STATE_ENTRY_INDEX, false);
      try {
        final OFastRidbagPaginatedClusterState clusterState = new OFastRidbagPaginatedClusterState(stateCacheEntry);
        fileSize = clusterState.getFileSize();
      } finally {
        releasePageFromRead(atomicOperation, stateCacheEntry);
      }

      allocateNewPage = true;
      pageIndex = fileSize + 1;
    } else {
      allocateNewPage = false;
      freePageIndex--;
    }

    return new FindFreePageResult(pageIndex, freePageIndex, allocateNewPage);
  }

  private void updateFreePagesIndex(int prevFreePageIndex, long pageIndex, OAtomicOperation atomicOperation) throws IOException {
    final OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, false);

    try {
      final OClusterPage localPage = new OClusterPage(cacheEntry, false);
      int newFreePageIndex = calculateFreePageIndex(localPage);

      if (prevFreePageIndex == newFreePageIndex) {
        return;
      }

      long nextPageIndex = localPage.getNextPage();
      long prevPageIndex = localPage.getPrevPage();

      if (prevPageIndex >= 0) {
        final OCacheEntry prevPageCacheEntry = loadPageForWrite(atomicOperation, fileId, prevPageIndex, false);
        try {
          final OClusterPage prevPage = new OClusterPage(prevPageCacheEntry, false);
          assert calculateFreePageIndex(prevPage) == prevFreePageIndex;
          prevPage.setNextPage(nextPageIndex);
        } finally {
          releasePageFromWrite(atomicOperation, prevPageCacheEntry);
        }
      }

      if (nextPageIndex >= 0) {
        final OCacheEntry nextPageCacheEntry = loadPageForWrite(atomicOperation, fileId, nextPageIndex, false);
        try {
          final OClusterPage nextPage = new OClusterPage(nextPageCacheEntry, false);
          if (calculateFreePageIndex(nextPage) != prevFreePageIndex) {
            calculateFreePageIndex(nextPage);
          }

          assert calculateFreePageIndex(nextPage) == prevFreePageIndex;
          nextPage.setPrevPage(prevPageIndex);

        } finally {
          releasePageFromWrite(atomicOperation, nextPageCacheEntry);
        }
      }

      localPage.setNextPage(-1);
      localPage.setPrevPage(-1);

      if (prevFreePageIndex < 0 && newFreePageIndex < 0) {
        return;
      }

      if (prevFreePageIndex >= 0 && prevFreePageIndex < FREE_LIST_SIZE) {
        if (prevPageIndex < 0) {
          updateFreePagesList(prevFreePageIndex, nextPageIndex, atomicOperation);
        }
      }

      if (newFreePageIndex >= 0) {
        long oldFreePage;
        OCacheEntry pinnedStateEntry = loadPageForRead(atomicOperation, fileId, STATE_ENTRY_INDEX, true);
        try {
          OFastRidbagPaginatedClusterState clusterFreeList = new OFastRidbagPaginatedClusterState(pinnedStateEntry);
          oldFreePage = clusterFreeList.getFreeListPage(newFreePageIndex);
        } finally {
          releasePageFromRead(atomicOperation, pinnedStateEntry);
        }

        if (oldFreePage >= 0) {
          final OCacheEntry oldFreePageCacheEntry = loadPageForWrite(atomicOperation, fileId, oldFreePage, false);
          try {
            final OClusterPage oldFreeLocalPage = new OClusterPage(oldFreePageCacheEntry, false);
            assert calculateFreePageIndex(oldFreeLocalPage) == newFreePageIndex;

            oldFreeLocalPage.setPrevPage(pageIndex);
          } finally {
            releasePageFromWrite(atomicOperation, oldFreePageCacheEntry);
          }

          localPage.setNextPage(oldFreePage);
          localPage.setPrevPage(-1);
        }

        updateFreePagesList(newFreePageIndex, pageIndex, atomicOperation);
      }
    } finally {
      releasePageFromWrite(atomicOperation, cacheEntry);
    }
  }

  private void updateFreePagesList(int freeListIndex, long pageIndex, OAtomicOperation atomicOperation) throws IOException {
    final OCacheEntry pinnedStateEntry = loadPageForWrite(atomicOperation, fileId, STATE_ENTRY_INDEX, true);
    try {
      OFastRidbagPaginatedClusterState paginatedClusterState = new OFastRidbagPaginatedClusterState(pinnedStateEntry);
      paginatedClusterState.setFreeListPage(freeListIndex, (int) pageIndex);
    } finally {
      releasePageFromWrite(atomicOperation, pinnedStateEntry);
    }
  }

  private static int calculateFreePageIndex(OClusterPage localPage) {
    int newFreePageIndex;
    if (localPage.isEmpty()) {
      newFreePageIndex = FREE_LIST_SIZE - 1;
    } else {
      newFreePageIndex = (localPage.getMaxRecordSize() - (ONE_KB - 1)) / ONE_KB;

      newFreePageIndex -= LOWEST_FREELIST_BOUNDARY;
    }
    return newFreePageIndex;
  }

  private void initCusterState(OAtomicOperation atomicOperation) throws IOException {
    final OCacheEntry stateEntry;
    if (getFilledUpTo(atomicOperation, fileId) == 0) {
      stateEntry = addPage(atomicOperation, fileId, true);
    } else {
      stateEntry = loadPageForWrite(atomicOperation, fileId, STATE_ENTRY_INDEX, false);
    }

    assert stateEntry.getPageIndex() == 0;
    try {
      OFastRidbagPaginatedClusterState paginatedClusterState = new OFastRidbagPaginatedClusterState(stateEntry);
      paginatedClusterState.setSize(0);
      paginatedClusterState.setRecordsSize(0);
      paginatedClusterState.setFileSize(0);

      for (int i = 0; i < FREE_LIST_SIZE; i++) {
        paginatedClusterState.setFreeListPage(i, -1);
      }
    } finally {
      releasePageFromWrite(atomicOperation, stateEntry);
    }

  }

  private static OPhysicalPosition[] convertToPhysicalPositions(long[] clusterPositions) {
    OPhysicalPosition[] positions = new OPhysicalPosition[clusterPositions.length];
    for (int i = 0; i < positions.length; i++) {
      OPhysicalPosition physicalPosition = new OPhysicalPosition();
      physicalPosition.clusterPosition = clusterPositions[i];
      positions[i] = physicalPosition;
    }
    return positions;
  }

  @Override
  public OPaginatedClusterDebug readDebug(long clusterPosition) throws IOException {
    OPaginatedClusterDebug debug = new OPaginatedClusterDebug();
    debug.clusterPosition = clusterPosition;
    debug.fileId = fileId;
    final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();

    OFastRidbagClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(clusterPosition, 1, atomicOperation);
    if (positionEntry == null) {
      debug.empty = true;
      return debug;
    }

    long pageIndex = positionEntry.getPageIndex();
    int recordPosition = positionEntry.getRecordPosition();

    debug.pages = new ArrayList<>();
    int contentSize = 0;

    long nextPagePointer;
    boolean firstEntry = true;
    do {
      OClusterPageDebug debugPage = new OClusterPageDebug();
      debugPage.pageIndex = pageIndex;
      OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
      try {
        final OClusterPage localPage = new OClusterPage(cacheEntry, false);

        if (localPage.isDeleted(recordPosition)) {
          if (debug.pages.isEmpty()) {
            debug.empty = true;
            return debug;
          } else {
            throw new OPaginatedClusterException("Content of record " + new ORecordId(id, clusterPosition) + " was broken", this);
          }
        }
        debugPage.inPagePosition = recordPosition;
        debugPage.inPageSize = localPage.getRecordSize(recordPosition);
        byte[] content = localPage.getRecordBinaryValue(recordPosition, 0, debugPage.inPageSize);
        debugPage.content = content;
        if (firstEntry && content[content.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE] == 0) {
          debug.empty = true;
          return debug;
        }

        debug.pages.add(debugPage);
        nextPagePointer = OLongSerializer.INSTANCE.deserializeNative(content, content.length - OLongSerializer.LONG_SIZE);
        contentSize += content.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE;

        firstEntry = false;
      } finally {
        releasePageFromRead(atomicOperation, cacheEntry);
      }

      pageIndex = getPageIndex(nextPagePointer);
      recordPosition = getRecordPosition(nextPagePointer);
    } while (nextPagePointer >= 0);
    debug.contentSize = contentSize;
    return debug;
  }

  @Override
  public RECORD_STATUS getRecordStatus(final long clusterPosition) throws IOException {
    final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    acquireSharedLock();
    try {
      final byte status = clusterPositionMap.getStatus(clusterPosition, atomicOperation);

      switch (status) {
      case OFastRidbagClusterPositionMapBucket.NOT_EXISTENT:
        return RECORD_STATUS.NOT_EXISTENT;
      case OFastRidbagClusterPositionMapBucket.ALLOCATED:
        return RECORD_STATUS.ALLOCATED;
      case OFastRidbagClusterPositionMapBucket.FILLED:
        return RECORD_STATUS.PRESENT;
      case OFastRidbagClusterPositionMapBucket.REMOVED:
        return RECORD_STATUS.REMOVED;
      }

      // UNREACHABLE
      return null;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void acquireAtomicExclusiveLock() {
    atomicOperationsManager.acquireExclusiveLockTillOperationComplete(this);
  }

  @Override
  public String toString() {
    return "plocal cluster: " + getName();
  }

  @Override
  public OClusterBrowsePage nextPage(long lastPosition) throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();

        OFastRidbagPaginatedClusterPositionMap.OClusterPositionEntry[] nextPositions = clusterPositionMap
            .higherPositionsEntries(lastPosition, atomicOperation);
        if (nextPositions.length > 0) {
          long newLastPosition = nextPositions[nextPositions.length - 1].getPosition();
          List<OClusterBrowseEntry> nexv = new ArrayList<>();
          for (OFastRidbagPaginatedClusterPositionMap.OClusterPositionEntry pos : nextPositions) {
            final ORawBuffer buff = internalReadRecord(pos.getPosition(), pos.getPage(), pos.getOffset(), 1, atomicOperation);
            nexv.add(new OClusterBrowseEntry(pos.getPosition(), buff));
          }
          return new OClusterBrowsePage(nexv, newLastPosition);
        } else {
          return null;
        }
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }
  
  public HelperClasses.Tuple<HelperClasses.Tuple<Byte, Long>, Integer> getPageIndexAndPagePositionAndTypeOfRecord(final long recordPos, 
          boolean lock) throws IOException{
    final OAtomicOperation atomicOperation;
    if (lock){
      atomicOperationsManager.acquireReadLock(this);
      atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    }
    else{
      atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    }
    try{
      if (lock){
        acquireSharedLock();
      }
      try{
        OPhysicalPosition ridsPPos = new OPhysicalPosition(recordPos);
        //here find records physical position
        OPhysicalPosition recordPhysicalPosition = getPhysicalPositionInternal(ridsPPos);
        //this means that node still not saved
        if (recordPhysicalPosition == null){          
          return null;
        }
        OFastRidbagClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(recordPhysicalPosition.clusterPosition, 1, atomicOperation);
        HelperClasses.Tuple<Byte, Long> pageIndexType = new HelperClasses.Tuple<>(recordPhysicalPosition.recordType, positionEntry.getPageIndex());
        return new HelperClasses.Tuple<>(pageIndexType, positionEntry.getRecordPosition());
      }
      finally{
        if (lock){
          releaseSharedLock();
        }
      }
    }
    finally{
      if (lock){
        atomicOperationsManager.releaseReadLock(this);
      }
    }
  }
  
  /**
   * 
   * @param recordPos node cluster position
   * @param lock
   * @return
   * @throws IOException 
   */
  private HelperClasses.Tuple<Long, Integer> getPageIndexAndPagePositionOfRecord(final long recordPos, boolean lock) throws IOException{
    final OAtomicOperation atomicOperation;
    if (lock){
      atomicOperationsManager.acquireReadLock(this);
      atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    }
    else{
      atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    }
    try{
      if (lock){
        acquireSharedLock();
      }
      try{
        OFastRidbagClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(recordPos, 1, atomicOperation);
        return new HelperClasses.Tuple<>(positionEntry.getPageIndex(), positionEntry.getRecordPosition());
      }
      finally{
        if (lock){
          releaseSharedLock();
        }
      }        
    }
    finally{
      if (lock){
        atomicOperationsManager.releaseReadLock(this);
      }
    }
  }

  private boolean checkIfNewContentFitsInPage(final int length, OClusterPage localPage) throws IOException {
    return localPage.canContentFitIntoPage(length);
  }
  
  /**
   * checks if all array node slots are set 
   * @param nodeClusterPosition
   * @return 
   * @throws java.io.IOException 
   */
  private boolean isArrayNodeFull(final OClusterPage localPage, int pagePosition) throws IOException{    
    byte[] content = getRidEntry(localPage, pagePosition);
    //skip record type
    int pos = OByteSerializer.BYTE_SIZE;
    int currentIndex = OIntegerSerializer.INSTANCE.deserialize(content, pos);
    pos += OIntegerSerializer.INT_SIZE;
    int capacity = OIntegerSerializer.INSTANCE.deserialize(content, pos);
    return currentIndex >= capacity - 1;
  }
  
  public ORID[] getAllRidsFromNode(final long pageIndex, final int pagePosition, final byte type, boolean lock) throws IOException{
    if (lock){
      atomicOperationsManager.acquireReadLock(this);
    }
    try{
      if (lock){
        acquireSharedLock();
      }
      try{    
        if (type == OLinkedListRidBag.RECORD_TYPE_LINKED_NODE){
          return getAllRidsFromLinkedNode(pageIndex, pagePosition);
        }
        else if (type == OLinkedListRidBag.RECORD_TYPE_ARRAY_NODE){
          return getAllRidsFromArrayNode(pageIndex, pagePosition);
        }
        else{
          throw new ODatabaseException("Invalid ridbag node type: " + type);
        }
      }
      finally{
        if (lock){
          releaseSharedLock();
        }
      }
    }
    finally{
      if (lock){
        atomicOperationsManager.releaseReadLock(this);
      }
    }
  }
  
  private ORID[] getAllRidsFromNode(final OClusterPage localPage, final int pagePosition, final byte type, boolean lock) throws IOException{ 
    if (type == OLinkedListRidBag.RECORD_TYPE_LINKED_NODE){
      return getAllRidsFromLinkedNode(localPage, pagePosition);
    }
    else if (type == OLinkedListRidBag.RECORD_TYPE_ARRAY_NODE){
      return getAllRidsFromArrayNode(localPage, pagePosition);
    }
    else{
      throw new ODatabaseException("Invalid ridbag node type: " + type);
    }
  }
  
  private ORID[] getAllRidsFromLinkedNode(final OClusterPage localPage, int ridPosition) throws IOException{
    byte[] content = getRidEntry(localPage, ridPosition);
    final int size = OIntegerSerializer.INSTANCE.deserialize(content, getRidEntrySize());
    final ORID[] ret = new ORID[size];
    boolean hasMore = true;
    int counter = 0;
    while (hasMore){
      //skip record type info
      int pos = OByteSerializer.BYTE_SIZE;
      ORID rid = OLinkSerializer.INSTANCE.deserialize(content, pos);
      ret[counter++] = rid;
      pos += OLinkSerializer.RID_SIZE;
      ridPosition = OIntegerSerializer.INSTANCE.deserialize(content, pos);
      if (ridPosition == -1){
        hasMore = false;
      }
      else{
        content = getRidEntry(localPage, ridPosition);
      }
    }
    return ret;
  }
  
  private ORID[] getAllRidsFromLinkedNode(final long pageIndex, int ridPosition) throws IOException{
    byte[] content = getRidEntry(pageIndex, ridPosition);
    final int size = OIntegerSerializer.INSTANCE.deserialize(content, getRidEntrySize());
    final ORID[] ret = new ORID[size];
    boolean hasMore = true;
    int counter = 0;
    while (hasMore){
      //skip record type info
      int pos = OByteSerializer.BYTE_SIZE;
      ORID rid = OLinkSerializer.INSTANCE.deserialize(content, pos);
      ret[counter++] = rid;
      pos += OLinkSerializer.RID_SIZE;
      ridPosition = OIntegerSerializer.INSTANCE.deserialize(content, pos);
      if (ridPosition == -1){
        hasMore = false;
      }
      else{
        content = getRidEntry(pageIndex, ridPosition);
      }
    }
    return ret;
  }
  
  private ORID[] getAllRidsFromArrayNode(final long pageIndex, final int ridPosition) throws IOException{
    final byte[] content = getRidEntry(pageIndex, ridPosition);
    //skip record type info
    int pos = OByteSerializer.BYTE_SIZE;
    final int lastValidIndex = OIntegerSerializer.INSTANCE.deserialize(content, pos);
    final ORID[] ret = new ORID[lastValidIndex + 1];
    pos = OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE * 2;
    for (int i = 0; i <= lastValidIndex; i++){
      ret[i] = OLinkSerializer.INSTANCE.deserialize(content, pos);
      pos += OLinkSerializer.RID_SIZE;
    }
    return ret;
  }
  
  private ORID[] getAllRidsFromArrayNode(final OClusterPage localPage, final int ridPosition) throws IOException{
    final byte[] content = getRidEntry(localPage, ridPosition);
    //skip record type info
    int pos = OByteSerializer.BYTE_SIZE;
    final int lastValidIndex = OIntegerSerializer.INSTANCE.deserialize(content, pos);
    final ORID[] ret = new ORID[lastValidIndex + 1];
    pos = OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE * 2;
    for (int i = 0; i <= lastValidIndex; i++){
      ret[i] = OLinkSerializer.INSTANCE.deserialize(content, pos);
      pos += OLinkSerializer.RID_SIZE;
    }
    return ret;
  }
  
  public int getNodeSize(long pageIndex, int pagePosition, byte type, boolean lock) throws IOException{
    if (lock){
      atomicOperationsManager.acquireReadLock(this);
    }
    try{
      if (lock){
        acquireSharedLock();
      }
      try{
        byte[] entry = getRidEntry(pageIndex, pagePosition);
        if (type == OLinkedListRidBag.RECORD_TYPE_LINKED_NODE){
          int pos = getRidEntrySize();
          int size = OIntegerSerializer.INSTANCE.deserialize(entry, pos);
          return size;
        }
        else if (type == OLinkedListRidBag.RECORD_TYPE_ARRAY_NODE){
          //read last valid index info
          int size = OIntegerSerializer.INSTANCE.deserialize(entry, OByteSerializer.BYTE_SIZE);
          return size + 1;
        }
        throw new ODatabaseException("Invalid ridbag node type: " + type);
      }
      finally{
        if (lock){
          releaseSharedLock();
        }
      }
    }
    finally{
      if (lock){
        atomicOperationsManager.releaseReadLock(this);
      }
    }
  }
  
  private int getNodeSize(final OClusterPage localPage, final int pagePosition, final byte type) throws IOException{
    byte[] entry = getRidEntry(localPage, pagePosition);
    if (type == OLinkedListRidBag.RECORD_TYPE_LINKED_NODE){
      int pos = getRidEntrySize();
      int size = OIntegerSerializer.INSTANCE.deserialize(entry, pos);
      return size;
    }
    else if (type == OLinkedListRidBag.RECORD_TYPE_ARRAY_NODE){
      //read last valid index info
      int size = OIntegerSerializer.INSTANCE.deserialize(entry, OByteSerializer.BYTE_SIZE);
      return size + 1;
    }
    throw new ODatabaseException("Invalid ridbag node type: " + type);
  }
  
  public Long getNextNode(long currentNodeClusterPosition, boolean lock) throws IOException{    
    OAtomicOperation atomicOperation;
    if (lock){
      atomicOperationsManager.acquireReadLock(this);
      atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    }
    else{
      atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    }
    try{
      if (lock){
        acquireSharedLock();
      }
      try{
        OFastRidbagClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(currentNodeClusterPosition, 1, atomicOperation);
        Long nextPos = positionEntry.getNextNodePosition();    
        if (nextPos == -1){
          nextPos = null;
        }
        return nextPos;
      }
      finally{
        if (lock){
          releaseSharedLock();
        }
      }
    }
    finally{
      if (lock){
        atomicOperationsManager.releaseReadLock(this);
      }      
    }
  }
  
  @Override
  public OAtomicOperation startAtomicOperation(boolean trackNonTxOperations) throws IOException {
    return super.startAtomicOperation(trackNonTxOperations);
  }
  
  @Override
  public void endAtomicOperation(boolean rollback) throws IOException {
    super.endAtomicOperation(rollback);
  }
  
  /**
   * 
   * @param currentNodeClusterPosition
   * @return previous node cluster position
   * @throws IOException 
   */
  private long removeNode(final long currentNodeClusterPosition, final long pageIndex, 
          final int pagePosition, byte type, long prevPos, long nextPos) throws IOException{
    OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    //find previous and next node of current node, and connect them
    if (prevPos != -1){
      OFastRidbagClusterPositionMapBucket.PositionEntry previousNodePositionEntry = clusterPositionMap.get(prevPos, 1, atomicOperation);
      previousNodePositionEntry = new OFastRidbagClusterPositionMapBucket.PositionEntry(previousNodePositionEntry.getPageIndex(), 
              previousNodePositionEntry.getRecordPosition(), previousNodePositionEntry.getPreviousNodePosition(), nextPos);
      clusterPositionMap.update(prevPos, previousNodePositionEntry, atomicOperation);
    }
    if (nextPos != -1){
      OFastRidbagClusterPositionMapBucket.PositionEntry nextNodePositionEntry = clusterPositionMap.get(nextPos, 1, atomicOperation);
      nextNodePositionEntry = new OFastRidbagClusterPositionMapBucket.PositionEntry(nextNodePositionEntry.getPageIndex(), 
              nextNodePositionEntry.getRecordPosition(), prevPos, nextNodePositionEntry.getNextNodePosition());
      clusterPositionMap.update(nextPos, nextNodePositionEntry, atomicOperation);
    }
    //delete node data
    if (type == OLinkedListRidBag.RECORD_TYPE_ARRAY_NODE){
      deleteArrayNodeData(currentNodeClusterPosition, pageIndex, pagePosition);
    }
    else if (type == OLinkedListRidBag.RECORD_TYPE_LINKED_NODE){
      deleteLinkNodeData(currentNodeClusterPosition, pageIndex, pagePosition);
    }
    
    return prevPos;
  }
  
  private long removeNode(final long currentNodeClusterPosition, final OClusterPage localPage, 
          final long pageIndex, final int pagePosition, byte type, long prevPos, long nextPos) throws IOException{
    OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    if (prevPos != -1){
      OFastRidbagClusterPositionMapBucket.PositionEntry previousNodePositionEntry = clusterPositionMap.get(prevPos, 1, atomicOperation);
      previousNodePositionEntry = new OFastRidbagClusterPositionMapBucket.PositionEntry(previousNodePositionEntry.getPageIndex(), 
              previousNodePositionEntry.getRecordPosition(), previousNodePositionEntry.getPreviousNodePosition(), nextPos);
      clusterPositionMap.update(prevPos, previousNodePositionEntry, atomicOperation);
    }
    if (nextPos != -1){
      OFastRidbagClusterPositionMapBucket.PositionEntry nextNodePositionEntry = clusterPositionMap.get(nextPos, 1, atomicOperation);
      nextNodePositionEntry = new OFastRidbagClusterPositionMapBucket.PositionEntry(nextNodePositionEntry.getPageIndex(), 
              nextNodePositionEntry.getRecordPosition(), prevPos, nextNodePositionEntry.getNextNodePosition());
      clusterPositionMap.update(nextPos, nextNodePositionEntry, atomicOperation);
    }
    //delete node data
    if (type == OLinkedListRidBag.RECORD_TYPE_ARRAY_NODE){
      deleteArrayNodeData(currentNodeClusterPosition, localPage, pageIndex, pagePosition);
    }
    else if (type == OLinkedListRidBag.RECORD_TYPE_LINKED_NODE){
      deleteLinkNodeData(currentNodeClusterPosition, localPage, pageIndex, pagePosition);
    }
    
    return prevPos;
  }
  
  private void deleteArrayNodeData(final long nodeClusterPosition, final OClusterPage localPage, 
          final long pageIndex, final int pagePosition) throws IOException{    
    deleteRecord(nodeClusterPosition, localPage, pageIndex, pagePosition);    
  }
  
  private void deleteArrayNodeData(final long nodeClusterPosition, final long pageIndex, final int pagePosition) throws IOException{    
    deleteRecord(nodeClusterPosition, pageIndex, pagePosition, false);    
  }
  
  private void deleteLinkNodeData(final long nodeClusterPosition, final long pageIndex, final int pagePosition) throws IOException{
    deleteLinkNodeDataPartialRead(nodeClusterPosition, pageIndex, pagePosition);
  }
  
  private void deleteLinkNodeData(final long nodeClusterPosition, final OClusterPage localPage, 
          final long pageIndex, final int pagePosition) throws IOException{
    deleteLinkNodeDataPartialRead(nodeClusterPosition, localPage, pageIndex, pagePosition);
  }
  
  /**
   * delete node and re-link it's previous with it's next node
   * @param nodeClusterPosition
   * @throws IOException 
   */
   private void deleteLinkNodeDataPartialRead(final long nodeClusterPosition, final long pageIndex, final int pagePosition) throws IOException {
    OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();          

    //delete all following rid entries
    int pos = OLinkSerializer.RID_SIZE + OByteSerializer.BYTE_SIZE;
    int nextEntryPos = getRecordContentAsInteger(pageIndex, pagePosition, pos);
    nextEntryPos = revertBytes(nextEntryPos);
    OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, false);
    boolean firstInNode = true;
    try {
      final OClusterPage localPage = new OClusterPage(cacheEntry, false);
      final int initialFreePageIndex = calculateFreePageIndex(localPage);
      int freeSizeBefore = localPage.getFreeSpace();
      while (nextEntryPos != -1) {
        int entryPointer = localPage.getRecordEntryPosition(nextEntryPos);
        int tmpNextEntryPos = getRecordContentAsIntegerFromEntryPointer(localPage, entryPointer, pos);
        tmpNextEntryPos = revertBytes(tmpNextEntryPos);
        if (!firstInNode) {
          localPage.deleteRecord(nextEntryPos);
        } else {
          firstInNode = false;
        }
        nextEntryPos = tmpNextEntryPos;
      }
      long recordsSizeDiff = localPage.getFreeSpace() - freeSizeBefore;
      updateFreePagesIndex(initialFreePageIndex, pageIndex, atomicOperation);
      updateClusterState(-1, -recordsSizeDiff, atomicOperation);
      
      //delete first rid entry
      deleteRecord(nodeClusterPosition, localPage, pageIndex, pagePosition); 
    } finally {
      releasePageFromWrite(atomicOperation, cacheEntry);
    }   
  }
   
  private void deleteLinkNodeDataPartialRead(final long nodeClusterPosition, final OClusterPage localPage,
          final long pageIndex, final int pagePosition) throws IOException {
    OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();          

    //delete all following rid entries
    int pos = OLinkSerializer.RID_SIZE + OByteSerializer.BYTE_SIZE;
    int nextEntryPos = getRecordContentAsInteger(localPage, pagePosition, pos);
    nextEntryPos = revertBytes(nextEntryPos);
    boolean firstInNode = true;
    
    final int initialFreePageIndex = calculateFreePageIndex(localPage);
    int freeSizeBefore = localPage.getFreeSpace();
    while (nextEntryPos != -1) {
      int entryPointer = localPage.getRecordEntryPosition(nextEntryPos);
      int tmpNextEntryPos = getRecordContentAsIntegerFromEntryPointer(localPage, entryPointer, pos);
      tmpNextEntryPos = revertBytes(tmpNextEntryPos);
      if (!firstInNode) {
        localPage.deleteRecord(nextEntryPos);
      } else {
        firstInNode = false;
      }
      nextEntryPos = tmpNextEntryPos;
    }
    long recordsSizeDiff = localPage.getFreeSpace() - freeSizeBefore;
    updateFreePagesIndex(initialFreePageIndex, pageIndex, atomicOperation);
    updateClusterState(-1, -recordsSizeDiff, atomicOperation);

    //delete first rid entry
    deleteRecord(nodeClusterPosition, localPage, pageIndex, pagePosition); 
  }
   
  private void deleteLinkNodeDataFullDataRead(final long nodeClusterPosition, final long pageIndex, final int pagePosition) throws IOException{
    OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();    
    byte[] content = getRidEntry(pageIndex, pagePosition);            

    //delete all following rid entries
    int pos = OLinkSerializer.RID_SIZE + OByteSerializer.BYTE_SIZE;
    int nextEntryPos = OIntegerSerializer.INSTANCE.deserialize(content, pos);      
    final OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, false);
    boolean firstInNode = true;     
    try{
      final OClusterPage localPage = new OClusterPage(cacheEntry, false);
      final int initialFreePageIndex = calculateFreePageIndex(localPage);  
      final int freeSizeBefore = localPage.getFreeSpace();
      while (nextEntryPos != -1){                   
        content = getRidEntry(localPage, nextEntryPos);
        if (!firstInNode){
          localPage.deleteRecord(nextEntryPos);
        }
        else{
          firstInNode = false;
        }                           
        nextEntryPos = OIntegerSerializer.INSTANCE.deserialize(content, pos);          
      }
      long recordsSizeDiff = localPage.getFreeSpace() - freeSizeBefore;
      updateFreePagesIndex(initialFreePageIndex, pageIndex, atomicOperation);
      updateClusterState(-1, -recordsSizeDiff, atomicOperation);
      
      //delete first rid entry
      deleteRecord(nodeClusterPosition, localPage, pageIndex, pagePosition); 
    }
    finally{
      releasePageFromWrite(atomicOperation, cacheEntry);
    }               
  }    
  
  public static class MegaMergeOutput{
    public long currentRidbagNodePageIndex;
    public int currentRidbagNodePagePosition;
    public byte currentRidbagNodeType;
    public long currentRidbagNodeClusterPosition;
    public long firstRidBagNodeClusterPos;
  }
  
  class NodeToRemove{

    NodeToRemove(long nodeClusterPosition, long nodePageIndex, int nodePagePosition, 
            byte nodeType) {
      this.nodeClusterPosition = nodeClusterPosition;
      this.nodePageIndex = nodePageIndex;
      this.nodePagePosition = nodePagePosition;
      this.nodeType = nodeType;
    }
    
    long nodeClusterPosition;
    long nodePageIndex;
    int nodePagePosition;
    byte nodeType;
  }
  
  public MegaMergeOutput nodesMegaMerge(long currentRidbagNodePageIndex, int currentRidbagNodePagePosition, byte currentRidbagNodeType, 
          long currentRidbagNodeClusterPosition, long firstRidBagNodeClusterPos, final int MAX_RIDBAG_NODE_SIZE) throws IOException {    
    final ORID[] mergedRids = new ORID[MAX_RIDBAG_NODE_SIZE];
    int currentOffset = 0;
    Long currentIteratingNode = firstRidBagNodeClusterPos;
    boolean removedFirstNode = false;
    long lastNonRemovedNode = -1;
    HelperClasses.Tuple<Long, Integer> pageIndexPagePosition = null;
    long megaNodeClusterPosition = -1;
    //previous node of current iterating node
    long prevNodeClusterPosition = -1;
    
    boolean foundOne = false;
    OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    atomicOperationsManager.acquireReadLock(this);
    List<NodeToRemove> nodesToRemove = new ArrayList<>();
    try{
      acquireSharedLock();
      try{
        while (currentIteratingNode != null) {
          boolean fetchedNextNode = false;
          HelperClasses.Tuple<HelperClasses.Tuple<Byte, Long>, Integer> typePageIndexPagePosition = 
                  getPageIndexAndPagePositionAndTypeOfRecord(currentIteratingNode, false);
          final byte type = typePageIndexPagePosition.getFirstVal().getFirstVal();
          final long pageIndex = typePageIndexPagePosition.getFirstVal().getSecondVal();
          final int pagePosition = typePageIndexPagePosition.getSecondVal();
          OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
          OClusterPage localPage = new OClusterPage(cacheEntry, false);
          try{
            if (!isMaxSizeNodeFullNode(localPage, pagePosition, type, MAX_RIDBAG_NODE_SIZE, false)) {
              foundOne = true;
              final ORID[] nodeRids = getAllRidsFromNode(localPage, pagePosition, type, false);
              System.arraycopy(nodeRids, 0, mergedRids, currentOffset, nodeRids.length);
              long tmpCurrNodeClusterPos = currentIteratingNode;
              currentIteratingNode = getNextNode(currentIteratingNode, false);
              nodesToRemove.add(new NodeToRemove(tmpCurrNodeClusterPos, pageIndex, pagePosition, type));
              currentOffset += nodeRids.length;
              if (tmpCurrNodeClusterPos == firstRidBagNodeClusterPos) {
                removedFirstNode = true;
              }
              fetchedNextNode = true;
            } else {
              lastNonRemovedNode = currentIteratingNode;
            }
            if (!fetchedNextNode) {
              prevNodeClusterPosition = currentIteratingNode;
              currentIteratingNode = getNextNode(currentIteratingNode, false);
            }
          }
          finally{
            releasePageFromRead(atomicOperation, cacheEntry);
          }
        }
      }
      finally{
        releaseSharedLock();
      }
    }
    finally{
      atomicOperationsManager.releaseReadLock(this);
    }

    //need this flag because array node of size 600 can be allocated from another place than node mega merge
    if (foundOne){
      boolean rollback = false;
      atomicOperation = startAtomicOperation(true);
      try {
        acquireExclusiveLock();
        try {
          //remove marked nodes, track pages to avoid excessive load page for write
          OClusterPage localPage = null;
          OCacheEntry cacheEntry = null;
          long prevPageIndex = -1;
          for (NodeToRemove nodeToRemove : nodesToRemove){
            if (prevPageIndex != nodeToRemove.nodePageIndex){
              if (localPage != null){
                releasePageFromWrite(atomicOperation, cacheEntry);
              }
              cacheEntry = loadPageForWrite(atomicOperation, fileId, nodeToRemove.nodePageIndex, false);
              localPage = new OClusterPage(cacheEntry, false);
            }
            removeNode(nodeToRemove.nodeClusterPosition, localPage, nodeToRemove.nodePageIndex, 
                    nodeToRemove.nodePagePosition, nodeToRemove.nodeType,
                    prevNodeClusterPosition, currentIteratingNode != null ? currentIteratingNode : -1l);
          }
          if (localPage != null){
            releasePageFromWrite(atomicOperation, cacheEntry);
          }
          
          OPhysicalPosition megaNodeAllocatedPosition = allocatePosition(RECORD_TYPE_ARRAY_NODE, false);
          pageIndexPagePosition = addRids(mergedRids, megaNodeAllocatedPosition, lastNonRemovedNode, -1l, MAX_RIDBAG_NODE_SIZE - 1);          
          megaNodeClusterPosition = megaNodeAllocatedPosition.clusterPosition;
          //set next node of previous node
          if (lastNonRemovedNode != -1) {
            updatePrevNextNodeinfo(lastNonRemovedNode, null, megaNodeClusterPosition);
          }
        } finally {
          releaseExclusiveLock();
        }
      } catch (Exception exc) {
        rollback = true;
        throw exc;
      } finally {
        endAtomicOperation(rollback);
      }

      currentRidbagNodePageIndex = pageIndexPagePosition.getFirstVal();
      currentRidbagNodePagePosition = pageIndexPagePosition.getSecondVal();
      currentRidbagNodeType = RECORD_TYPE_ARRAY_NODE;
      currentRidbagNodeClusterPosition = megaNodeClusterPosition;
      if (removedFirstNode) {
        firstRidBagNodeClusterPos = megaNodeClusterPosition;
      }
    }
    
    MegaMergeOutput ret = new MegaMergeOutput();
    ret.currentRidbagNodePageIndex = currentRidbagNodePageIndex;
    ret.currentRidbagNodePagePosition = currentRidbagNodePagePosition;
    ret.currentRidbagNodeType = currentRidbagNodeType;
    ret.currentRidbagNodeClusterPosition = currentRidbagNodeClusterPosition;
    ret.firstRidBagNodeClusterPos = firstRidBagNodeClusterPos;
    return ret;
  }
  
  private boolean isMaxSizeNodeFullNode(final long pageIndex, final int pagePosition, 
          final byte type, final int MAX_RIDBAG_NODE_SIZE, boolean lock) throws IOException{
    if (type == RECORD_TYPE_ARRAY_NODE){
      return getNodeSize(pageIndex, pagePosition, type, lock) == MAX_RIDBAG_NODE_SIZE;
    }
    return false;
  }
  
  private boolean isMaxSizeNodeFullNode(final OClusterPage localPage, final int pagePosition, 
          final byte type, final int MAX_RIDBAG_NODE_SIZE, boolean lock) throws IOException{
    if (type == RECORD_TYPE_ARRAY_NODE){
      return getNodeSize(localPage, pagePosition, type) == MAX_RIDBAG_NODE_SIZE;
    }
    return false;
  }
  
  public MegaMergeOutput addRidHighLevel(final OIdentifiable value, long pageIndex, int pagePosition, byte currentNodeType,
          final int ADDITIONAL_ALLOCATION_SIZE, final int MAX_RIDBAG_NODE_SIZE,
          long currentRidbagNodeClusterPos, long firstRidBagNodeClusterPos) throws IOException {
    final boolean isCurrentNodeFull;
    OPhysicalPosition newNodePhysicalPosition= null;
    HelperClasses.Tuple<Long, Integer> pageIndexPagePosition = null;
    boolean rollback = false;
    OAtomicOperation atomicOperation = startAtomicOperation(true);
    try {
      acquireExclusiveLock();
      try {
        OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, false);
        OClusterPage localPage = new OClusterPage(cacheEntry, false);
        try{
          isCurrentNodeFull = isCurrentNodeFullNode(localPage, pageIndex, pagePosition, currentNodeType);
          if (isCurrentNodeFull) {
            //By this algorithm allways link node is followed with array node and vice versa
            if (currentNodeType == RECORD_TYPE_LINKED_NODE) {
              ORID[] currentNodeRids = getAllRidsFromLinkedNode(localPage, pagePosition);
              newNodePhysicalPosition = allocatePosition(RECORD_TYPE_ARRAY_NODE, false);
              int newNodePreallocatedSize = calculateArrayRidNodeAllocationSize(currentNodeRids.length,
                      ADDITIONAL_ALLOCATION_SIZE, MAX_RIDBAG_NODE_SIZE);
              ORID[] newNodePreallocatedRids = new ORID[newNodePreallocatedSize];
              System.arraycopy(currentNodeRids, 0, newNodePreallocatedRids, 0, currentNodeRids.length);
              newNodePreallocatedRids[currentNodeRids.length] = value.getIdentity();
              fillRestOfArrayWithDummyRids(newNodePreallocatedRids, currentNodeRids.length);
              pageIndexPagePosition = addRids(newNodePreallocatedRids, newNodePhysicalPosition, currentRidbagNodeClusterPos,
                      -1l, currentNodeRids.length);
              HelperClasses.Tuple<Long, Long> prevNextposition = updatePrevNextNodeinfo(currentRidbagNodeClusterPos, 
                      null, newNodePhysicalPosition.clusterPosition);
              removeNode(currentRidbagNodeClusterPos, localPage, pageIndex, pagePosition, 
                      currentNodeType, prevNextposition.getFirstVal(), prevNextposition.getSecondVal());
              currentNodeType = RECORD_TYPE_ARRAY_NODE;
            } else if (currentNodeType == RECORD_TYPE_ARRAY_NODE) {
              newNodePhysicalPosition = allocatePosition(RECORD_TYPE_LINKED_NODE, false);
              pageIndexPagePosition = addRid(value.getIdentity(), newNodePhysicalPosition, currentRidbagNodeClusterPos, -1l);
              updatePrevNextNodeinfo(currentRidbagNodeClusterPos, null, newNodePhysicalPosition.clusterPosition);
              currentNodeType = RECORD_TYPE_LINKED_NODE;
            } else {
              throw new ODatabaseException("Invalid record type in cluster position: " + currentRidbagNodeClusterPos);
            }
          } else {
            switch (currentNodeType) {
              case RECORD_TYPE_LINKED_NODE:
                addRidToLinkedNode(value.getIdentity(), localPage, pageIndex, pagePosition);
                break;
              case RECORD_TYPE_ARRAY_NODE:
                addRidToArrayNode(value.getIdentity(), localPage, pagePosition);
                break;
              default:
                throw new ODatabaseException("Invalid record type in cluster position: " + currentRidbagNodeClusterPos);
            }
          }
        }
        finally{
          releasePageFromWrite(atomicOperation, cacheEntry);
        }
      } finally {
        releaseExclusiveLock();
      }
    } catch (Exception exc) {
      rollback = true;
      throw exc;
    } finally {
      endAtomicOperation(rollback);
    }
    
    if (isCurrentNodeFull){
      //this means that entry currentNodeType was LINKED
      if (currentNodeType == RECORD_TYPE_ARRAY_NODE){
        if (currentRidbagNodeClusterPos == firstRidBagNodeClusterPos) {
          firstRidBagNodeClusterPos = newNodePhysicalPosition.clusterPosition;
        }
      }
      pageIndex = pageIndexPagePosition.getFirstVal();
      pagePosition = pageIndexPagePosition.getSecondVal();
      currentRidbagNodeClusterPos = newNodePhysicalPosition.clusterPosition;
    }
    
    MegaMergeOutput ret = new MegaMergeOutput();
    ret.currentRidbagNodePageIndex = pageIndex;
    ret.currentRidbagNodePagePosition = pagePosition;
    ret.currentRidbagNodeType = currentNodeType;
    ret.currentRidbagNodeClusterPosition = currentRidbagNodeClusterPos;
    ret.firstRidBagNodeClusterPos = firstRidBagNodeClusterPos;
    return ret;
  }
  
  private static int calculateArrayRidNodeAllocationSize(final int initialNumberOfRids, final int ADDITIONAL_ALLOCATION_SIZE,
          final int MAX_RIDBAG_NODE_SIZE){
    int size = Math.min(initialNumberOfRids + ADDITIONAL_ALLOCATION_SIZE, initialNumberOfRids * 2);
    size = Math.min(size, MAX_RIDBAG_NODE_SIZE);
    return size;
  }
  
  private static void fillRestOfArrayWithDummyRids(final ORID[] array, final int lastValidIndex){
    ORID dummy = new ORecordId(-1, -1);
    for (int i = lastValidIndex + 1; i < array.length; i++){
      array[i] = dummy;
    }
  }
  
  private boolean isCurrentNodeFullNode(final OClusterPage localPage, final long pageIndex, 
          final int pagePosition, final byte type) throws IOException{
    if (type == RECORD_TYPE_LINKED_NODE){
      return (!checkIfNewContentFitsInPage(getRidEntrySize(), localPage));
    }
    else if (type == RECORD_TYPE_ARRAY_NODE){      
      return isArrayNodeFull(localPage, pagePosition);
    }
    throw new ODatabaseException("Invalid record type in page (indes/position): " + pageIndex + "," + pagePosition);
  }
  
  public MegaMergeOutput firstNodeAllocation(ORID[] rids, final int ADDITIONAL_ALLOCATION_SIZE,
          final int MAX_RIDBAG_NODE_SIZE) throws IOException {
    final OPhysicalPosition allocatedPos;
    final HelperClasses.Tuple<Long, Integer> pageIndexPagePosition;
    boolean rollback = false;
    startAtomicOperation(true);
    try {
      acquireExclusiveLock();
      try {
        allocatedPos = allocatePosition(RECORD_TYPE_ARRAY_NODE, false);
        final int size = calculateArrayRidNodeAllocationSize(rids.length, ADDITIONAL_ALLOCATION_SIZE,
                MAX_RIDBAG_NODE_SIZE);
        final ORID[] toAllocate = new ORID[size];
        fillRestOfArrayWithDummyRids(toAllocate, -1);
        pageIndexPagePosition = addRids(toAllocate, allocatedPos, -1l, -1l, -1);
      } finally {
        releaseExclusiveLock();
      }
    } catch (Exception e) {
      rollback = true;
      throw e;
    } finally {
      endAtomicOperation(rollback);
    }
    
    MegaMergeOutput ret = new MegaMergeOutput();
    ret.currentRidbagNodePageIndex = pageIndexPagePosition.getFirstVal();
    ret.currentRidbagNodePagePosition = pageIndexPagePosition.getSecondVal();
    ret.currentRidbagNodeType = RECORD_TYPE_ARRAY_NODE;
    ret.firstRidBagNodeClusterPos = allocatedPos.clusterPosition;
    ret.currentRidbagNodeClusterPosition = allocatedPos.clusterPosition;
    return ret;
  }
}
