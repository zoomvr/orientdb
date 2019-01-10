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
import com.orientechnologies.common.log.OLogManager;
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
import java.util.Set;

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
    acquireExclusiveLock();
    try {
      final String tempFileName = file.getName() + "$temp";
      try {
        final long tempFileId = writeCache.addFile(tempFileName);
        writeCache.replaceFileContentWith(tempFileId, file.toPath());

        readCache.deleteFile(clusterPositionMap.getFileId(), writeCache);
        writeCache.renameFile(tempFileId, clusterPositionMap.getFullName());
        clusterPositionMap.replaceFileId(tempFileId);
      } finally {
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
  private void addRidToLinkedNode(final ORID rid, final long pageIndex, final int pagePosition) throws IOException{
    addRidToLinkedNodeUpdateAtOnce(rid, pageIndex, pagePosition);
  }
    
  private void addRidToLinkedNodePartialUpdate(final ORID rid, final long pageIndex, final int pagePosition) throws IOException {

    final byte[] content = new byte[getRidEntrySize()];
    int pos = OByteSerializer.BYTE_SIZE;
    OLinkSerializer.INSTANCE.serialize(rid, content, pos);
    pos += OLinkSerializer.RID_SIZE;
    OIntegerSerializer.INSTANCE.serialize(-1, content, pos);
    
    OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    
    HelperClasses.Tuple<OClusterPage, Integer> firstRecordPageAndEntryPosition = null;
    try {                
      firstRecordPageAndEntryPosition = getRecordPageAndEntryPosition(pageIndex, pagePosition, false, atomicOperation);
      OClusterPage firstRecordPage = firstRecordPageAndEntryPosition.getFirstVal();
      int firstRecordPageEntryPosition = firstRecordPageAndEntryPosition.getSecondVal();

      //this is first linked node entry so it is extended, skip rid info and refernce to next
      pos = getRidEntrySize();
      int currentSize = getRecordContentAsIntegerFromEntryPosition(firstRecordPage, firstRecordPageEntryPosition, pos);
      currentSize = revertBytes(currentSize);
      pos += OIntegerSerializer.INT_SIZE;
      int previousRidPosition = getRecordContentAsIntegerFromEntryPosition(firstRecordPage, firstRecordPageEntryPosition, pos);
      previousRidPosition = revertBytes(previousRidPosition);
      final AddEntryResult addEntryResult = addEntry(1, content, pageIndex, atomicOperation);
      //update reference to next rid in previuosly added rid entry
      //position is record type (byte) + rid size
      if (pagePosition != previousRidPosition){          
        replaceRecordContent(pageIndex, previousRidPosition, revertBytes(addEntryResult.pagePosition), OByteSerializer.BYTE_SIZE + OLinkSerializer.RID_SIZE);
      }
      else{
        replaceRecordContentFromEntryPosition(firstRecordPage, firstRecordPageEntryPosition, revertBytes(addEntryResult.pagePosition), OByteSerializer.BYTE_SIZE + OLinkSerializer.RID_SIZE);
      }
      //now increment size, and update info about last added rid. That is done in first entry of node
      ++currentSize;                

      pos = getRidEntrySize();
      replaceRecordContentFromEntryPosition(firstRecordPage, firstRecordPageEntryPosition, revertBytes(currentSize), pos);
      //update info about current entry in this node
      pos += OIntegerSerializer.INT_SIZE;
      replaceRecordContentFromEntryPosition(firstRecordPage, firstRecordPageEntryPosition, revertBytes(addEntryResult.pagePosition), pos);

      updateClusterState(1, addEntryResult.recordsSizeDiff, atomicOperation);                

    } finally {      
      if (firstRecordPageAndEntryPosition != null){
        releaseRecordPage(firstRecordPageAndEntryPosition.getFirstVal(), false, atomicOperation);
      }
    }    
  }
    
  private void addRidToLinkedNodeUpdateAtOnce(final ORID rid, final long pageIndex, final int pagePosition) throws IOException {

    final byte[] content = new byte[getRidEntrySize()];
    int pos = OByteSerializer.BYTE_SIZE;
    OLinkSerializer.INSTANCE.serialize(rid, content, pos);
    pos += OLinkSerializer.RID_SIZE;
    OIntegerSerializer.INSTANCE.serialize(-1, content, pos);
    
    OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();          
    final byte[] firstEntryContent = getRidEntry(pageIndex, pagePosition);

    //this is first linked node entry so it is extended, skip rid info and refernce to next               
    pos = getRidEntrySize();
    int currentSize = OIntegerSerializer.INSTANCE.deserialize(firstEntryContent, pos);        
    pos += OIntegerSerializer.INT_SIZE;
    int previousRidPosition = OIntegerSerializer.INSTANCE.deserialize(firstEntryContent, pos);
    final AddEntryResult addEntryResult = addEntry(1, content, pageIndex, atomicOperation);

    //update reference to next rid in previuosly added rid entry
    //position is record type (byte) + rid size
    if (pagePosition != previousRidPosition){          
      replaceRecordContent(pageIndex, previousRidPosition, revertBytes(addEntryResult.pagePosition), OByteSerializer.BYTE_SIZE + OLinkSerializer.RID_SIZE);
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
    replaceContent(pageIndex, pagePosition, firstEntryContent);

    updateClusterState(1, addEntryResult.recordsSizeDiff, atomicOperation);          
  }
  
  /**
   * 
   * @param rid
   * @param currentNodePageIndex
   * @param currentRidPagePosition, node position in page
   * @throws IOException 
   */
  private void addRidToArrayNode(final ORID rid, final long currentNodePageIndex, int currentRidPagePosition) throws IOException {                   
//    byte[] ridEntry = getRidEntry(currentNodePageIndex, currentRidPagePosition);
      //skip record type info
      int pos = OByteSerializer.BYTE_SIZE;
      //get last valid current index
      int currentIndex = getRecordContentAsInteger(currentNodePageIndex, currentRidPagePosition, pos);
      currentIndex = revertBytes(currentIndex);
      ++currentIndex;
      pos += OIntegerSerializer.INT_SIZE;
      int capacity = getRecordContentAsInteger(currentNodePageIndex, currentRidPagePosition, pos);
      capacity = revertBytes(capacity);
      if (currentIndex >= capacity){
        throw new ArrayIndexOutOfBoundsException(currentIndex);
      }
      //serialize current index after record type info
//        OIntegerSerializer.INSTANCE.serialize(currentIndex, ridEntry, OByteSerializer.BYTE_SIZE);
      replaceRecordContent(currentNodePageIndex, currentRidPagePosition, revertBytes(currentIndex), OByteSerializer.BYTE_SIZE);
      pos = OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE * 2 + currentIndex * OLinkSerializer.RID_SIZE;
      //set new rid info
      byte[] ridSerialized = new byte[OLinkSerializer.RID_SIZE];
      OLinkSerializer.INSTANCE.serialize(rid, ridSerialized, 0);
//        OLinkSerializer.INSTANCE.serialize(rid, ridEntry, pos);
      replaceRecordContent(currentNodePageIndex, currentRidPagePosition, ridSerialized, pos);
//        replaceContent(currentNodePageIndex, currentRidPagePosition, ridEntry);
        
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
  
  private HelperClasses.Tuple<OClusterPage, Integer> getRecordPageAndEntryPosition(long pageIndex, int ridPagePosition, 
          boolean forRead, OAtomicOperation atomicOperation) throws IOException{
    OCacheEntry cacheEntry;        
    if (forRead){
      cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
    }
    else{
      cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, false);
    }
    OClusterPage localPage = new OClusterPage(cacheEntry, false);      
    return new HelperClasses.Tuple<>(localPage, localPage.getRecordEntryPosition(ridPagePosition));    
  }
  
  private void releaseRecordPage(OClusterPage page, boolean forRead, OAtomicOperation atomicOperation){
    if (forRead){
      releasePageFromRead(atomicOperation, page.getCacheEntry());
    }
    else{
      releasePageFromWrite(atomicOperation, page.getCacheEntry());
    }
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
  
  private int getRecordContentAsIntegerFromEntryPosition(OClusterPage page, int entryPosition, int offset) throws IOException{    
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
  
  private void replaceRecordContentFromEntryPosition(OClusterPage page, final int entryPosition, final int value, final int offset) throws IOException{    
    page.setRecordIntValueFromEntryPosition(entryPosition, offset, value);    
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
  
  private void updatePrevNextNodeinfo(long nodeClusterPosition, 
          Long previousNodeClusterPosition, Long nextnodeClusterPosition) throws IOException{
    OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    
    OFastRidbagClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(nodeClusterPosition, 1, atomicOperation);
    if (positionEntry == null){
      positionEntry = clusterPositionMap.get(nodeClusterPosition, 1, atomicOperation);
    }
    if (previousNodeClusterPosition == null){
      previousNodeClusterPosition = positionEntry.getPreviousNodePosition();
    }
    if (nextnodeClusterPosition == null){
      nextnodeClusterPosition = positionEntry.getNextNodePosition();
    }
    positionEntry = new OFastRidbagClusterPositionMapBucket.PositionEntry(positionEntry.getPageIndex(), 
            positionEntry.getRecordPosition(), previousNodeClusterPosition, nextnodeClusterPosition);
    clusterPositionMap.update(nodeClusterPosition, positionEntry, atomicOperation);     
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
  private Long addRid(final ORID rid, final OPhysicalPosition allocatedPosition,
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
    return clusterPosition;      
  }
  
  private long addRids(final ORID[] rids, final OPhysicalPosition allocatedPosition,
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

    return clusterPosition;      
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
    return deleteRecord(clusterPosition, true);
  }
    
  private boolean deleteRecord(long clusterPosition, boolean lock) throws IOException {
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
        OFastRidbagClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(clusterPosition, 1, atomicOperation);
        if (positionEntry == null) {
          return false;
        }

        long pageIndex = positionEntry.getPageIndex();
        int recordPosition = positionEntry.getRecordPosition();
        
        int removedContentSize = 0;
        
        boolean cacheEntryReleased = false;
        OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, false);
        int initialFreePageIndex;
        try {
          OClusterPage localPage = new OClusterPage(cacheEntry, false);
          initialFreePageIndex = calculateFreePageIndex(localPage);

          if (localPage.isDeleted(recordPosition)) {
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
          localPage.deleteRecord(recordPosition);

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
    boolean rollback = false;
    final OAtomicOperation atomicOperation = startAtomicOperation(true);
    try {
      acquireExclusiveLock();
      try {
        OFastRidbagClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(position, 1, atomicOperation);

        if (positionEntry == null) {
          return false;
        }

        updateClusterState(-1, 0, atomicOperation);
        clusterPositionMap.remove(position, atomicOperation);

        addAtomicOperationMetadata(new ORecordId(id, position), atomicOperation);

        return true;
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
  public void updateRecord(final long clusterPosition, byte[] content, final int recordVersion, final byte recordType)
      throws IOException {
    int pos = 0;    
    long previousNodePosition = OLongSerializer.INSTANCE.deserialize(content, pos);
    pos += OLongSerializer.LONG_SIZE;
    long nextNodePosition = OLongSerializer.INSTANCE.deserialize(content, pos);
    pos += OLongSerializer.LONG_SIZE;
    content = Arrays.copyOfRange(content, pos, content.length);
    
    content = compression.compress(content);
    content = encryption.encrypt(content);

    boolean rollback = false;
    final OAtomicOperation atomicOperation = startAtomicOperation(true);
    try {
      acquireExclusiveLock();
      try {
        final OFastRidbagClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(clusterPosition, 1, atomicOperation);

        if (positionEntry == null) {
          return;
        }

        int nextRecordPosition = positionEntry.getRecordPosition();
        long nextPageIndex = positionEntry.getPageIndex();

        int newRecordPosition = -1;
        long newPageIndex = -1;

        long prevPageIndex = -1;
        int prevRecordPosition = -1;

        long nextEntryPointer = -1;
        int from = 0;
        int to;

        long sizeDiff = 0;
        byte[] updateEntry = null;

        do {
          final int entrySize;
          final int updatedEntryPosition;

          if (updateEntry == null) {
            if (from == 0) {
              entrySize = Math.min(getEntryContentLength(content.length), OClusterPage.MAX_RECORD_SIZE);
              to = entrySize - (2 * OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE);
            } else {
              entrySize = Math
                  .min(content.length - from + OByteSerializer.BYTE_SIZE + OLongSerializer.LONG_SIZE, OClusterPage.MAX_RECORD_SIZE);
              to = from + entrySize - (OByteSerializer.BYTE_SIZE + OLongSerializer.LONG_SIZE);
            }

            updateEntry = new byte[entrySize];
            int entryPosition = 0;

            if (from == 0) {
              updateEntry[entryPosition] = recordType;
              entryPosition++;

              OIntegerSerializer.INSTANCE.serializeNative(content.length, updateEntry, entryPosition);
              entryPosition += OIntegerSerializer.INT_SIZE;
            }

            System.arraycopy(content, from, updateEntry, entryPosition, to - from);
            entryPosition += to - from;

            if (nextPageIndex == positionEntry.getPageIndex()) {
              updateEntry[entryPosition] = 1;
            }

            entryPosition++;

            OLongSerializer.INSTANCE.serializeNative(-1, updateEntry, entryPosition);

            if (to < content.length) {
              assert entrySize == OClusterPage.MAX_RECORD_SIZE;
            }
          } else {
            entrySize = updateEntry.length;

            if (from == 0) {
              to = entrySize - (2 * OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE);
            } else {
              to = from + entrySize - (OByteSerializer.BYTE_SIZE + OLongSerializer.LONG_SIZE);
            }
          }

          int freePageIndex = -1;

          final boolean isNew;
          if (nextPageIndex < 0) {
            FindFreePageResult findFreePageResult = findFreePage(entrySize, atomicOperation);
            nextPageIndex = findFreePageResult.pageIndex;
            freePageIndex = findFreePageResult.freePageIndex;
            isNew = findFreePageResult.allocateNewPage;
          } else {
            isNew = false;
          }

          final OCacheEntry cacheEntry;
          if (isNew) {
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
            cacheEntry = loadPageForWrite(atomicOperation, fileId, nextPageIndex, false);
          }

          try {
            final OClusterPage localPage = new OClusterPage(cacheEntry, isNew);
            final int pageFreeSpace = localPage.getFreeSpace();

            if (freePageIndex < 0) {
              freePageIndex = calculateFreePageIndex(localPage);
            } else {
              assert isNew || freePageIndex == calculateFreePageIndex(localPage);
            }

            if (nextRecordPosition >= 0) {
              if (localPage.isDeleted(nextRecordPosition)) {
                throw new OPaginatedClusterException("Record with rid " + new ORecordId(id, clusterPosition) + " was deleted",
                    this);
              }

              int currentEntrySize = localPage.getRecordSize(nextRecordPosition);
              nextEntryPointer = localPage.getRecordLongValue(nextRecordPosition, currentEntrySize - OLongSerializer.LONG_SIZE);

              if (currentEntrySize == entrySize) {
                localPage.replaceRecord(nextRecordPosition, updateEntry, recordVersion);
                updatedEntryPosition = nextRecordPosition;
              } else {
                localPage.deleteRecord(nextRecordPosition);

                if (localPage.getMaxRecordSize() >= entrySize) {
                  updatedEntryPosition = localPage.appendRecord(recordVersion, updateEntry);

                  if (updatedEntryPosition < 0) {
                    localPage.dumpToLog();
                    throw new IllegalStateException("Page " + cacheEntry.getPageIndex()
                        + " does not have enough free space to add record content, freePageIndex=" + freePageIndex
                        + ", updateEntry.length=" + updateEntry.length + ", content.length=" + content.length);
                  }
                } else {
                  updatedEntryPosition = -1;
                }
              }

              if (nextEntryPointer >= 0) {
                nextRecordPosition = getRecordPosition(nextEntryPointer);
                nextPageIndex = getPageIndex(nextEntryPointer);
              } else {
                nextPageIndex = -1;
                nextRecordPosition = -1;
              }

            } else {
              assert localPage.getFreeSpace() >= entrySize;
              updatedEntryPosition = localPage.appendRecord(recordVersion, updateEntry);

              if (updatedEntryPosition < 0) {
                localPage.dumpToLog();
                throw new IllegalStateException(
                    "Page " + cacheEntry.getPageIndex() + " does not have enough free space to add record content, freePageIndex="
                        + freePageIndex + ", updateEntry.length=" + updateEntry.length + ", content.length=" + content.length);
              }

              nextPageIndex = -1;
              nextRecordPosition = -1;
            }

            sizeDiff += pageFreeSpace - localPage.getFreeSpace();

          } finally {
            releasePageFromWrite(atomicOperation, cacheEntry);
          }

          updateFreePagesIndex(freePageIndex, cacheEntry.getPageIndex(), atomicOperation);

          if (updatedEntryPosition >= 0) {
            if (from == 0) {
              newPageIndex = cacheEntry.getPageIndex();
              newRecordPosition = updatedEntryPosition;
            }

            from = to;

            if (prevPageIndex >= 0) {
              OCacheEntry prevCacheEntry = loadPageForWrite(atomicOperation, fileId, prevPageIndex, false);
              try {
                OClusterPage prevPage = new OClusterPage(prevCacheEntry, false);
                prevPage.setRecordLongValue(prevRecordPosition, -OLongSerializer.LONG_SIZE,
                    createPagePointer(cacheEntry.getPageIndex(), updatedEntryPosition));
              } finally {
                releasePageFromWrite(atomicOperation, prevCacheEntry);
              }
            }

            prevPageIndex = cacheEntry.getPageIndex();
            prevRecordPosition = updatedEntryPosition;

            updateEntry = null;
          }
        } while (to < content.length || updateEntry != null);

        // clear unneeded pages
        while (nextEntryPointer >= 0) {
          nextPageIndex = getPageIndex(nextEntryPointer);
          nextRecordPosition = getRecordPosition(nextEntryPointer);

          final int freePagesIndex;
          final int freeSpace;

          OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, nextPageIndex, false);
          try {
            final OClusterPage localPage = new OClusterPage(cacheEntry, false);
            freeSpace = localPage.getFreeSpace();
            freePagesIndex = calculateFreePageIndex(localPage);

            nextEntryPointer = localPage.getRecordLongValue(nextRecordPosition, -OLongSerializer.LONG_SIZE);
            localPage.deleteRecord(nextRecordPosition);

            sizeDiff += freeSpace - localPage.getFreeSpace();
          } finally {
            releasePageFromWrite(atomicOperation, cacheEntry);
          }

          updateFreePagesIndex(freePagesIndex, nextPageIndex, atomicOperation);
        }

        assert newPageIndex >= 0;
        assert newRecordPosition >= 0;

        if (newPageIndex != positionEntry.getPageIndex() || newRecordPosition != positionEntry.getRecordPosition()) {
          clusterPositionMap.update(clusterPosition, 
                  new OFastRidbagClusterPositionMapBucket.PositionEntry(newPageIndex, newRecordPosition, previousNodePosition, nextNodePosition),
              atomicOperation);
        }

        updateClusterState(0, sizeDiff, atomicOperation);

        addAtomicOperationMetadata(new ORecordId(id, clusterPosition), atomicOperation);
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

  /**
   * Recycles a deleted record.
   */
  @Override
  public void recycleRecord(final long clusterPosition, byte[] content, final int recordVersion, final byte recordType)
      throws IOException {
    
    int pos = 0;    
    long previousNodePosition = OLongSerializer.INSTANCE.deserialize(content, pos);
    pos += OLongSerializer.LONG_SIZE;
    long nextNodePosition = OLongSerializer.INSTANCE.deserialize(content, pos);
    pos += OLongSerializer.LONG_SIZE;
    content = Arrays.copyOfRange(content, pos, content.length);
    
    boolean rollback = false;
    final OAtomicOperation atomicOperation = startAtomicOperation(true);

    try {
      acquireExclusiveLock();
      try {
        final OFastRidbagClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(clusterPosition, 1, atomicOperation);
        if (positionEntry != null) {
          // NOT DELETED
          throw new OPaginatedClusterException("Record with rid " + new ORecordId(id, clusterPosition) + " was not deleted", this);
        }

        content = compression.compress(content);
        content = encryption.encrypt(content);

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

          clusterPositionMap.resurrect(clusterPosition,
              new OFastRidbagClusterPositionMapBucket.PositionEntry(addEntryResult.pageIndex, addEntryResult.pagePosition, previousNodePosition, nextNodePosition), 
              atomicOperation);

          addAtomicOperationMetadata(new ORecordId(id, clusterPosition), atomicOperation);
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

            final AddEntryResult addEntryResult = addEntry(recordVersion, entryContent, atomicOperation);
            recordsSizeDiff += addEntryResult.recordsSizeDiff;

            if (firstPageIndex == -1) {
              firstPageIndex = addEntryResult.pageIndex;
              firstPagePosition = addEntryResult.pagePosition;
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

          clusterPositionMap.update(clusterPosition, 
              new OFastRidbagClusterPositionMapBucket.PositionEntry(firstPageIndex, firstPagePosition, previousNodePosition, nextNodePosition),
              atomicOperation);

          addAtomicOperationMetadata(new ORecordId(id, clusterPosition), atomicOperation);
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
  public long getTombstonesCount() {
    return 0;
  }

  @Override
  public void truncate() throws IOException {
    boolean rollback = false;
    final OAtomicOperation atomicOperation = startAtomicOperation(true);
    try {
      acquireExclusiveLock();
      try {
        clusterPositionMap.truncate(atomicOperation);

        initCusterState(atomicOperation);
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
  public OPhysicalPosition getPhysicalPosition(OPhysicalPosition position) throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
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

          //do not know what is this, in this cluster no negative offsets are used so suppose that this shouldn't be here
//          if (localPage.getRecordByteValue(recordPosition, -OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE) == 0) {
//            return null;
//          }

          final OPhysicalPosition physicalPosition = new OPhysicalPosition();
          physicalPosition.recordSize = -1;

          physicalPosition.recordType = localPage.getRecordByteValue(recordPosition, 0);
          physicalPosition.recordVersion = localPage.getRecordVersion(recordPosition);
          physicalPosition.clusterPosition = position.clusterPosition;

          return physicalPosition;
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
    
    int recordSizesDiff;
    int position;
    final int finalVersion;
    Integer freePageIndex = null;

    try {
      final OClusterPage localPage = new OClusterPage(cacheEntry, false);
      freePageIndex = calculateFreePageIndex(localPage);

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

    if (freePageIndex != null){
      updateFreePagesIndex(freePageIndex, pageIndex, atomicOperation);
    }

    return new AddEntryResult(pageIndex, position, finalVersion, recordSizesDiff);
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
  
  private Long getPageIndexOfRecord(final long recordPos) throws IOException{
    final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
//    OPhysicalPosition ridsPPos = new OPhysicalPosition(recordPos);
    //here find records physical position
//    OPhysicalPosition recordPhysicalPosition = getPhysicalPosition(ridsPPos);
    //this means that node still not saved
//    if (recordPhysicalPosition == null){
//      return null;
//    }
    OFastRidbagClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(recordPos, 1, atomicOperation);
    return positionEntry.getPageIndex();
  }
  
  /**
   * 
   * @param recordPos node cluster position
   * @return
   * @throws IOException 
   */
  public HelperClasses.Tuple<Byte, Long> getPageIndexAndTypeOfRecord(final long recordPos) throws IOException{
    final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    OPhysicalPosition ridsPPos = new OPhysicalPosition(recordPos);
    //here find records physical position
    OPhysicalPosition recordPhysicalPosition = getPhysicalPosition(ridsPPos);
    //this means that node still not saved
    if (recordPhysicalPosition == null){
      return null;
    }
    OFastRidbagClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(recordPhysicalPosition.clusterPosition, 1, atomicOperation);
    return new HelperClasses.Tuple<>(recordPhysicalPosition.recordType, positionEntry.getPageIndex());
  }
  
  public HelperClasses.Tuple<HelperClasses.Tuple<Byte, Long>, Integer> getPageIndexAndPagePositionAndTypeOfRecord(final long recordPos) throws IOException{
    final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    OPhysicalPosition ridsPPos = new OPhysicalPosition(recordPos);
    //here find records physical position
    OPhysicalPosition recordPhysicalPosition = getPhysicalPosition(ridsPPos);
    //this means that node still not saved
    if (recordPhysicalPosition == null){      
      return null;
    }
    OFastRidbagClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(recordPhysicalPosition.clusterPosition, 1, atomicOperation);
    HelperClasses.Tuple<Byte, Long> pageIndexType = new HelperClasses.Tuple<>(recordPhysicalPosition.recordType, positionEntry.getPageIndex());
    return new HelperClasses.Tuple<>(pageIndexType, positionEntry.getRecordPosition());
  }
  
  public byte getTypeOfRecord(final long recordPos) throws IOException{    
    OPhysicalPosition ridsPPos = new OPhysicalPosition(recordPos);
    //here find records physical position
    OPhysicalPosition recordPhysicalPosition = getPhysicalPosition(ridsPPos);
    //this means that node still not saved    
    return recordPhysicalPosition.recordType;
  }
  
  /**
   * 
   * @param recordPos node cluster position
   * @return
   * @throws IOException 
   */
  public HelperClasses.Tuple<Long, Integer> getPageIndexAndPagePositionOfRecord(final long recordPos) throws IOException{
    final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    OFastRidbagClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(recordPos, 1, atomicOperation);
    return new HelperClasses.Tuple<>(positionEntry.getPageIndex(), positionEntry.getRecordPosition());
  }
  
  public int getFreeSpace(final long pageIndex) throws IOException{    
    OCacheEntry cacheEntry = null;
    final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    try{      
      cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
      OClusterPage localPage = new OClusterPage(cacheEntry, false);
      int freeSpace = localPage.getFreeSpace();
      return freeSpace;
    }
    finally{
      if (cacheEntry != null){
        releasePageFromRead(atomicOperation, cacheEntry);
      }      
    }
  }
  
  public boolean checkIfNewContentFitsInPage(final int length, long pageIndex) throws IOException {
    Integer freeSpace = getFreeSpace(pageIndex);        
    //three ints are header for record content in page
    return freeSpace >= length + 3 * OIntegerSerializer.INT_SIZE;
  }
  
  public HelperClasses.Tuple<Long, Long> getNodePreviousNextNodePositons(long position) throws IOException{
    final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    return clusterPositionMap.getPreviousNextNodeInfo(position, atomicOperation);
  }
  
  /**
   * checks if all array node slots are set 
   * @param nodeClusterPosition
   * @return 
   * @throws java.io.IOException 
   */
  public boolean isArrayNodeFull(long pageIndex, int pagePosition) throws IOException{    
    byte[] content = getRidEntry(pageIndex, pagePosition);
    //skip record type
    int pos = OByteSerializer.BYTE_SIZE;
    int currentIndex = OIntegerSerializer.INSTANCE.deserialize(content, pos);
    pos += OIntegerSerializer.INT_SIZE;
    int capacity = OIntegerSerializer.INSTANCE.deserialize(content, pos);
    return currentIndex >= capacity - 1;
  }
  
  public ORID[] getAllRidsFromNode(long nodeClusterPosition) throws IOException{
    byte type = getTypeOfRecord(nodeClusterPosition);    
    if (type == OLinkedListRidBag.RECORD_TYPE_LINKED_NODE){
      return getAllRidsFromLinkedNode(nodeClusterPosition);
    }
    else if (type == OLinkedListRidBag.RECORD_TYPE_ARRAY_NODE){
      return getAllRidsFromArrayNode(nodeClusterPosition);
    }
    else{
      throw new ODatabaseException("Invalid ridbag node type: " + type);
    }
  }
  
  public ORID[] getAllRidsFromLinkedNode(long nodeClusterPosition) throws IOException{
    HelperClasses.Tuple<Long, Integer> pageIndexPagePosition = getPageIndexAndPagePositionOfRecord(nodeClusterPosition);
    final long pageIndex = pageIndexPagePosition.getFirstVal();
    //this is entry position on page
    int ridPosition = pageIndexPagePosition.getSecondVal();
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
  
  public ORID[] getAllRidsFromArrayNode(long nodeClusterPosition) throws IOException{
    final HelperClasses.Tuple<Long, Integer> pageIndexPagePosition = getPageIndexAndPagePositionOfRecord(nodeClusterPosition);
    final long pageIndex = pageIndexPagePosition.getFirstVal();
    //this is entry position on page
    final int ridPosition = pageIndexPagePosition.getSecondVal();
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
  
  public int getNodeSize(long nodeClusterPosition) throws IOException{
    HelperClasses.Tuple<HelperClasses.Tuple<Byte, Long>, Integer> nodeTypePageIndexPagePosition = getPageIndexAndPagePositionAndTypeOfRecord(nodeClusterPosition);    
    HelperClasses.Tuple<Byte, Long> nodeTypePageIndex = nodeTypePageIndexPagePosition.getFirstVal();
    byte type = nodeTypePageIndex.getFirstVal();
    long pageIndex = nodeTypePageIndex.getSecondVal();    
    int pagePosition = nodeTypePageIndexPagePosition.getSecondVal();
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
  
  public Long getNextNode(long currentNodeClusterPosition) throws IOException{
    OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    OFastRidbagClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(currentNodeClusterPosition, 1, atomicOperation);
    Long nextPos = positionEntry.getNextNodePosition();    
    if (nextPos == -1){
      nextPos = null;
    }
    return nextPos;
  }
  
  public Long getPreviousNode(long currentNodeClusterPosition) throws IOException{
    OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    OFastRidbagClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(currentNodeClusterPosition, 1, atomicOperation);
    Long prevPos = positionEntry.getPreviousNodePosition();    
    if (prevPos == -1){
      prevPos = null;
    }
    return prevPos;
  }
  
  /**
   * 
   * @param currentNodeClusterPosition
   * @return previous node cluster position
   * @throws IOException 
   */
  public long removeNode(long currentNodeClusterPosition) throws IOException{
    OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    OFastRidbagClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(currentNodeClusterPosition, 1, atomicOperation);
    //find previous and next node of current node, and connect them
    Long nextPos = positionEntry.getNextNodePosition();
    Long prevPos = positionEntry.getPreviousNodePosition();
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
    HelperClasses.Tuple<Byte, Long> nodeTypeAndPageIndex = getPageIndexAndTypeOfRecord(currentNodeClusterPosition);
    byte type = nodeTypeAndPageIndex.getFirstVal();
    if (type == OLinkedListRidBag.RECORD_TYPE_ARRAY_NODE){
      deleteArrayNodeData(currentNodeClusterPosition);
    }
    else if (type == OLinkedListRidBag.RECORD_TYPE_LINKED_NODE){
      deleteLinkNodeData(currentNodeClusterPosition);
    }
    
    return prevPos;
  }
  
  private void deleteArrayNodeData(long nodeClusterPosition) throws IOException{    
    deleteRecord(nodeClusterPosition, false);    
  }
  
  private void deleteLinkNodeData(long nodeClusterPosition) throws IOException{
    deleteLinkNodeDataFullDataRead(nodeClusterPosition);
  }
  
  /**
   * delete node and re-link it's previous with it's next node
   * @param nodeClusterPosition
   * @throws IOException 
   */
   private void deleteLinkNodeDataPartialRead(long nodeClusterPosition) throws IOException {
    OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();        
    HelperClasses.Tuple<Long, Integer> nodePageIndexAndPagePosition = getPageIndexAndPagePositionOfRecord(nodeClusterPosition);
    long pageIndex = nodePageIndexAndPagePosition.getFirstVal();
    int pagePosition = nodePageIndexAndPagePosition.getSecondVal();          

    //delete all following rid entries
    int pos = OLinkSerializer.RID_SIZE + OByteSerializer.BYTE_SIZE;
    int nextEntryPos = getRecordContentAsInteger(pageIndex, pagePosition, pos);
    nextEntryPos = revertBytes(nextEntryPos);
    OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, false);
    boolean firstInNode = true;
    long recordsSizeDiff = 0;
    try {
      final OClusterPage localPage = new OClusterPage(cacheEntry, false);
      int initialFreePageIndex = calculateFreePageIndex(localPage);
      while (nextEntryPos != -1) {
        int freeSizeBefore = localPage.getFreeSpace();
        int tmpNextEntryPos = getRecordContentAsInteger(pageIndex, nextEntryPos, pos);
        tmpNextEntryPos = revertBytes(tmpNextEntryPos);
        if (!firstInNode) {
          localPage.deleteRecord(nextEntryPos);
        } else {
          firstInNode = false;
        }
        recordsSizeDiff += localPage.getFreeSpace() - freeSizeBefore;
        nextEntryPos = tmpNextEntryPos;

      }
      updateFreePagesIndex(initialFreePageIndex, pageIndex, atomicOperation);
      updateClusterState(-1, -recordsSizeDiff, atomicOperation);
    } finally {
      releasePageFromWrite(atomicOperation, cacheEntry);
    }

    //delete first rid entry
    deleteRecord(nodeClusterPosition, false);    
  }
   
  private void deleteLinkNodeDataFullDataRead(long nodeClusterPosition) throws IOException{
    OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();    
    HelperClasses.Tuple<Long, Integer> nodePageIndexAndPagePosition = getPageIndexAndPagePositionOfRecord(nodeClusterPosition);    
    long pageIndex = nodePageIndexAndPagePosition.getFirstVal();
    int pagePosition = nodePageIndexAndPagePosition.getSecondVal();
    byte[] content = getRidEntry(pageIndex, pagePosition);            

    //delete all following rid entries
    int pos = OLinkSerializer.RID_SIZE + OByteSerializer.BYTE_SIZE;
    int nextEntryPos = OIntegerSerializer.INSTANCE.deserialize(content, pos);      
    OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, false);
    boolean firstInNode = true;
    long recordsSizeDiff = 0;            
    try{
      final OClusterPage localPage = new OClusterPage(cacheEntry, false);
      int initialFreePageIndex = calculateFreePageIndex(localPage);        
      while (nextEntryPos != -1){
        int freeSizeBefore = localPage.getFreeSpace();           
        content = getRidEntry(pageIndex, nextEntryPos);
        if (!firstInNode){
          localPage.deleteRecord(nextEntryPos);
        }
        else{
          firstInNode = false;
        }
        recordsSizeDiff += localPage.getFreeSpace() - freeSizeBefore;                           
        nextEntryPos = OIntegerSerializer.INSTANCE.deserialize(content, pos);          
      }
      updateFreePagesIndex(initialFreePageIndex, pageIndex, atomicOperation);
      updateClusterState(-1, -recordsSizeDiff, atomicOperation);        
    }
    finally{
      releasePageFromWrite(atomicOperation, cacheEntry);
    }            

    //delete first rid entry
    deleteRecord(nodeClusterPosition, false);    
  }    
  
  public static class MegaMergeOutput{
    public long currentRidbagNodeClusterPos;
    public long firstRidBagNodeClusterPos;
    public boolean shouldSaveParentRecord;
  }
  
  public MegaMergeOutput nodesMegaMerge(Long currentRidbagNodeClusterPos, long firstRidBagNodeClusterPos, final int MAX_RIDBAG_NODE_SIZE,
          boolean shouldSaveParentRecord) throws IOException {
    boolean rollback = false;
    startAtomicOperation(true);
    try {
      acquireExclusiveLock();
      try {
        final ORID[] mergedRids = new ORID[MAX_RIDBAG_NODE_SIZE];
        int currentOffset = 0;
        Long currentIteratingNode = firstRidBagNodeClusterPos;
        boolean removedFirstNode = false;
        long lastNonRemovedNode = -1;
        while (currentIteratingNode != null) {
          boolean fetchedNextNode = false;
          if (!isMaxSizeNodeFullNode(currentIteratingNode, MAX_RIDBAG_NODE_SIZE)) {
            final byte type = getTypeOfRecord(currentIteratingNode);
            final ORID[] nodeRids;
            if (type == RECORD_TYPE_LINKED_NODE) {
              nodeRids = getAllRidsFromLinkedNode(currentIteratingNode);
            } else if (type == RECORD_TYPE_ARRAY_NODE) {
              nodeRids = getAllRidsFromArrayNode(currentIteratingNode);
            } else {
              throw new ODatabaseException("Invalid node type: " + type);
            }
            System.arraycopy(nodeRids, 0, mergedRids, currentOffset, nodeRids.length);
            long tmpCurrNodeClusterPos = currentIteratingNode;
            currentIteratingNode = getNextNode(currentIteratingNode);
            removeNode(tmpCurrNodeClusterPos);
            currentOffset += nodeRids.length;
            if (tmpCurrNodeClusterPos == firstRidBagNodeClusterPos) {
              removedFirstNode = true;
            }
            fetchedNextNode = true;
          } else {
            lastNonRemovedNode = currentIteratingNode;
          }
          if (!fetchedNextNode) {
            currentIteratingNode = getNextNode(currentIteratingNode);
          }
        }
        
        OPhysicalPosition megaNodeAllocatedPosition = allocatePosition(RECORD_TYPE_ARRAY_NODE);
        long megaNodeClusterPosition = addRids(mergedRids, megaNodeAllocatedPosition, lastNonRemovedNode, -1l, MAX_RIDBAG_NODE_SIZE - 1);
        //set next node of previous node
        if (lastNonRemovedNode != -1) {
          updatePrevNextNodeinfo(lastNonRemovedNode, null, megaNodeClusterPosition);
        }
        currentRidbagNodeClusterPos = megaNodeClusterPosition;
        if (removedFirstNode) {
          firstRidBagNodeClusterPos = megaNodeClusterPosition;
          shouldSaveParentRecord = true;
        }
        
        MegaMergeOutput ret = new MegaMergeOutput();
        ret.currentRidbagNodeClusterPos = currentRidbagNodeClusterPos;
        ret.firstRidBagNodeClusterPos = firstRidBagNodeClusterPos;
        ret.shouldSaveParentRecord = shouldSaveParentRecord;
        return ret;
      } finally {
        releaseExclusiveLock();
      }
    } catch (Exception exc) {
      rollback = true;
      throw exc;
    } finally {
      endAtomicOperation(rollback);
    }
  }
  
  private boolean isMaxSizeNodeFullNode(final long nodeClusterPosition, final int MAX_RIDBAG_NODE_SIZE) throws IOException{
    byte nodeType = getTypeOfRecord(nodeClusterPosition);
    if (nodeType == RECORD_TYPE_ARRAY_NODE){
      return getNodeSize(nodeClusterPosition) == MAX_RIDBAG_NODE_SIZE;
    }
    return false;
  }
  
  public MegaMergeOutput addRidHighLevel(final OIdentifiable value, long pageIndex, int pagePosition, byte currentNodeType,
          final int ADDITIONAL_ALLOCATION_SIZE, final int MAX_RIDBAG_NODE_SIZE,
          long currentRidbagNodeClusterPos, long firstRidBagNodeClusterPos,
          boolean shouldSaveParentRecord) throws IOException {
    boolean rollback = false;
    startAtomicOperation(true);
    try {
      acquireExclusiveLock();
      try {
        boolean isCurrentNodeFull = isCurrentNodeFullNode(pageIndex, pagePosition, currentNodeType);
        if (isCurrentNodeFull) {
          //By this algorithm allways link node is followed with array node and vice versa
          if (currentNodeType == RECORD_TYPE_LINKED_NODE) {
            ORID[] currentNodeRids = getAllRidsFromLinkedNode(currentRidbagNodeClusterPos);
            OPhysicalPosition newNodePhysicalPosition = allocatePosition(RECORD_TYPE_ARRAY_NODE);
            int newNodePreallocatedSize = calculateArrayRidNodeAllocationSize(currentNodeRids.length,
                    ADDITIONAL_ALLOCATION_SIZE, MAX_RIDBAG_NODE_SIZE);
            ORID[] newNodePreallocatedRids = new ORID[newNodePreallocatedSize];
            System.arraycopy(currentNodeRids, 0, newNodePreallocatedRids, 0, currentNodeRids.length);
            newNodePreallocatedRids[currentNodeRids.length] = value.getIdentity();
            fillRestOfArrayWithDummyRids(newNodePreallocatedRids, currentNodeRids.length);
            long newNodeClusterPosition = addRids(newNodePreallocatedRids, newNodePhysicalPosition, currentRidbagNodeClusterPos,
                    -1l, currentNodeRids.length);
            updatePrevNextNodeinfo(currentRidbagNodeClusterPos, null, newNodeClusterPosition);
            removeNode(currentRidbagNodeClusterPos);
            if (currentRidbagNodeClusterPos == firstRidBagNodeClusterPos) {
              firstRidBagNodeClusterPos = newNodeClusterPosition;
              shouldSaveParentRecord = true;
            }
            currentRidbagNodeClusterPos = newNodeClusterPosition;
          } else if (currentNodeType == RECORD_TYPE_ARRAY_NODE) {
            OPhysicalPosition newNodePhysicalPosition = allocatePosition(RECORD_TYPE_ARRAY_NODE);
            long newNodeClusterPosition = addRid(value.getIdentity(), newNodePhysicalPosition, currentRidbagNodeClusterPos, -1l);
            updatePrevNextNodeinfo(currentRidbagNodeClusterPos, null, newNodeClusterPosition);
            currentRidbagNodeClusterPos = newNodeClusterPosition;
          } else {
            throw new ODatabaseException("Invalid record type in cluster position: " + currentRidbagNodeClusterPos);
          }
        } else {
          switch (currentNodeType) {
            case RECORD_TYPE_LINKED_NODE:
              addRidToLinkedNode(value.getIdentity(), pageIndex, pagePosition);
              break;
            case RECORD_TYPE_ARRAY_NODE:
              addRidToArrayNode(value.getIdentity(), pageIndex, pagePosition);
              break;
            default:
              throw new ODatabaseException("Invalid record type in cluster position: " + currentRidbagNodeClusterPos);
          }
        }

        MegaMergeOutput ret = new MegaMergeOutput();
        ret.currentRidbagNodeClusterPos = currentRidbagNodeClusterPos;
        ret.firstRidBagNodeClusterPos = firstRidBagNodeClusterPos;
        ret.shouldSaveParentRecord = shouldSaveParentRecord;
        return ret;
      } finally {
        releaseExclusiveLock();
      }
    } catch (Exception exc) {
      rollback = true;
      throw exc;
    } finally {
      endAtomicOperation(rollback);
    }
  }
  
  private static int calculateArrayRidNodeAllocationSize(final int initialNumberOfRids, final int ADDITIONAL_ALLOCATION_SIZE,
          final int MAX_RIDBAG_NODE_SIZE){
    int size = Math.min(initialNumberOfRids + ADDITIONAL_ALLOCATION_SIZE, initialNumberOfRids * 2);
    size = Math.min(size, MAX_RIDBAG_NODE_SIZE);
    return size;
  }
  
  private static void fillRestOfArrayWithDummyRids(final ORID[] array, final int lastValidIndex){
    for (int i = lastValidIndex + 1; i < array.length; i++){
      array[i] = new ORecordId(-1, -1);
    }
  }
  
  private boolean isCurrentNodeFullNode(long pageIndex, int pagePosition, byte type) throws IOException{
    if (type == RECORD_TYPE_LINKED_NODE){
      return (!checkIfNewContentFitsInPage(getRidEntrySize(), pageIndex));
    }
    else if (type == RECORD_TYPE_ARRAY_NODE){      
      return isArrayNodeFull(pageIndex, pagePosition);
    }
    throw new ODatabaseException("Invalid record type in page (indes/position): " + pageIndex + "," + pagePosition);
  }
  
  public MegaMergeOutput firstNodeAllocation(ORID[] rids, final int ADDITIONAL_ALLOCATION_SIZE,
          final int MAX_RIDBAG_NODE_SIZE) throws IOException {
    boolean rollback = false;
    startAtomicOperation(true);
    try {
      acquireExclusiveLock();
      try {
        final OPhysicalPosition allocatedPos = allocatePosition(RECORD_TYPE_ARRAY_NODE);
        final int size = calculateArrayRidNodeAllocationSize(rids.length, ADDITIONAL_ALLOCATION_SIZE,
                MAX_RIDBAG_NODE_SIZE);
        final ORID[] toAllocate = new ORID[size];
        fillRestOfArrayWithDummyRids(toAllocate, -1);
        long clusterPosition = addRids(toAllocate, allocatedPos, -1l, -1l, -1);
        long firstRidBagNodeClusterPos;
        long currentRidbagNodeClusterPos;
        firstRidBagNodeClusterPos = currentRidbagNodeClusterPos = clusterPosition;        
        boolean shouldSaveParentRecord = true;
        MegaMergeOutput ret = new MegaMergeOutput();
        ret.currentRidbagNodeClusterPos = currentRidbagNodeClusterPos;
        ret.firstRidBagNodeClusterPos = firstRidBagNodeClusterPos;
        ret.shouldSaveParentRecord = shouldSaveParentRecord;
        return ret;
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
}
