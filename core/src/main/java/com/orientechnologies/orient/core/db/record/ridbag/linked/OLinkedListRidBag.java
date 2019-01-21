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
package com.orientechnologies.orient.core.db.record.ridbag.linked;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeListener;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBagDelegate;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses;
import com.orientechnologies.orient.core.storage.cluster.linkedridbags.OFastRidBagPaginatedCluster;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.Change;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author marko
 */
public class OLinkedListRidBag implements ORidBagDelegate{           
  
  protected static class RidbagMetadata{
    private long firstNode;
    private long size;
    private long storedSize;
    private long currentNodePageIndex;
    private int currentNodePagePosition;
    private byte currentNodeType;
    private long currentNodeClusterPosition;
    
    public RidbagMetadata(long firstNode, long currentNodeClusterPosition, long currentNodePageIndex, int currentNodePagePosition, 
            byte currentNodeType, long size, long storedSize) {
      this.size = size;
      this.storedSize = storedSize;
      this.currentNodePageIndex = currentNodePageIndex;
      this.currentNodePagePosition = currentNodePagePosition;
      this.currentNodeType = currentNodeType;
      this.firstNode = firstNode;
      this.currentNodeClusterPosition = currentNodeClusterPosition;
    }
    
    public long getSize(){
      return size;
    }
    
    public long getStoredSize(){
      return storedSize;
    }
    
    public long getFirstNodeClusterPosition(){
      return firstNode;
    }
    
    public void setSize(long size){
      this.size = size;
    }
    
    public void setStoredSize(long storedSize){
      this.storedSize = storedSize;
    }
    
    public void setFirstNode(long firstNode){
      this.firstNode = firstNode;
    }

    public long getCurrentNodePageIndex() {
      return currentNodePageIndex;
    }

    public void setCurrentNodePageIndex(long currentNodePageIndex) {
      this.currentNodePageIndex = currentNodePageIndex;
    }

    public int getCurrentNodePagePosition() {
      return currentNodePagePosition;
    }

    public void setCurrentNodePagePosition(int currentNodePagePosition) {
      this.currentNodePagePosition = currentNodePagePosition;
    }

    public byte getCurrentNodeType() {
      return currentNodeType;
    }

    public void setCurrentNodeType(byte currentNodeType) {
      this.currentNodeType = currentNodeType;
    }

    public long getCurrentNodeClusterPosition() {
      return currentNodeClusterPosition;
    }

    public void setCurrentNodeClusterPosition(long currentNodeClusterPosition) {
      this.currentNodeClusterPosition = currentNodeClusterPosition;
    }
    
  }
  
  
  public static final byte RECORD_TYPE_LINKED_NODE = 'l';
  public static final byte RECORD_TYPE_ARRAY_NODE = 'a';  
    
  private OFastRidBagPaginatedCluster cluster = null;
      
  protected static final int MAX_RIDBAG_NODE_SIZE = 600;
  private static final int ADDITIONAL_ALLOCATION_SIZE = 20;
  
  private boolean autoConvertToRecord = true;
  private List<OMultiValueChangeListener<OIdentifiable, OIdentifiable>> changeListeners;
  private ORecord owner = null;  
  
//  private boolean shouldSaveParentRecord = false;
  
  private final List<OIdentifiable> pendingRids = new LinkedList<>();
  private final UUID uuid;
    
  private static Map<UUID, RidbagMetadata> mappedRidbagInfo = new ConcurrentHashMap<>();
  
  private static Object[] lockObjects = new Object[64];
  static {
    for (int i = 0; i < lockObjects.length; i++){
      lockObjects[i] = new Object();
    }
  }
  
  protected static Object getLockObject(UUID uuid){
    int hash = uuid.hashCode();
    if (hash == Integer.MIN_VALUE){
      hash++;
    }
    hash = Math.abs(hash);
    return lockObjects[hash % lockObjects.length];
  }
  
  public OLinkedListRidBag(OFastRidBagPaginatedCluster cluster, UUID uuid){
    this.cluster = cluster;
    this.uuid = uuid;    
  }  
  
  public OLinkedListRidBag(OFastRidBagPaginatedCluster cluster, ORID[] rids, UUID uuid) throws IOException{    
    this.cluster = cluster;
    long size = rids.length;
    long storedSize = 0;
    this.uuid = uuid;
    synchronized(getLockObject(uuid)){
      OFastRidBagPaginatedCluster.MegaMergeOutput output = cluster.firstNodeAllocation(rids, ADDITIONAL_ALLOCATION_SIZE, MAX_RIDBAG_NODE_SIZE);
      long firstRidBagNodeClusterPos = output.firstRidBagNodeClusterPos;
      long currentRidbagNodePageIndex = output.currentRidbagNodePageIndex;
      int currentRidbagNodePagePosition = output.currentRidbagNodePagePosition;
      byte currentRidbagNodeType = output.currentRidbagNodeType;
      long currentRidbagNodeClusterPosition = output.currentRidbagNodeClusterPosition;
//      shouldSaveParentRecord = output.shouldSaveParentRecord;
      if (mappedRidbagInfo.containsKey(uuid) == false){
        RidbagMetadata info  = new RidbagMetadata(firstRidBagNodeClusterPos, currentRidbagNodeClusterPosition,
                currentRidbagNodePageIndex, currentRidbagNodePagePosition, currentRidbagNodeType, 
                size, storedSize);
        mappedRidbagInfo.put(uuid, info);
      }
    } 
    Collections.addAll(pendingRids, rids);
  }
  
  @Override
  public void addAll(Collection<OIdentifiable> values) {    
    values.forEach(this::add);
  } 
  
  /**
   * process all previously invalid rids, which are valid now
   * @param fireChangedEvent
   * 
   */
  private void processInvalidRidsReferences(){
    //go through collection of invalid rids and check if some become valid
    Iterator<OIdentifiable> addedInvalidRidsIter = pendingRids.iterator();
    
//    RidbagMetadata info = mappedRidbagInfo.get(uuid);
//    long currentRidbagNodePageIndex = info.getCurrentNodePageIndex();
//    int currentRidbagNodePagePosition = info.getCurrentNodePagePosition();
//    byte currentNodeType = info.getCurrentNodeType();
    
//    long storedSize;
//    long firstRidBagNodeClusterPos;      
    
    while (addedInvalidRidsIter.hasNext()){
      RidbagMetadata info = mappedRidbagInfo.get(uuid);
      long currentRidbagNodePageIndex = info.getCurrentNodePageIndex();
      int currentRidbagNodePagePosition = info.getCurrentNodePagePosition();
      byte currentNodeType = info.getCurrentNodeType();
      long storedSize = info.getStoredSize();
      long firstRidBagNodeClusterPos = info.getFirstNodeClusterPosition();
      long currentRidbagNodeClusterPosition = info.getCurrentNodeClusterPosition();
      
      OIdentifiable value = addedInvalidRidsIter.next();
      //process only persistent rids
      if (!value.getIdentity().isPersistent()){
        continue;
      }
      try {
        //check for megaMerge
        if (storedSize > 0 && storedSize % MAX_RIDBAG_NODE_SIZE == 0) {                     
          nodesMegaMerge();
          info = mappedRidbagInfo.get(uuid);
          currentRidbagNodePageIndex = info.getCurrentNodePageIndex();
          currentRidbagNodePagePosition = info.getCurrentNodePagePosition();
          currentNodeType = info.getCurrentNodeType();
          currentRidbagNodeClusterPosition = info.getCurrentNodeClusterPosition();
          storedSize = info.getStoredSize();
          firstRidBagNodeClusterPos = info.getFirstNodeClusterPosition();
        }

        OFastRidBagPaginatedCluster.MegaMergeOutput output = cluster.addRidHighLevel(value, currentRidbagNodePageIndex, 
                currentRidbagNodePagePosition, currentNodeType, ADDITIONAL_ALLOCATION_SIZE, MAX_RIDBAG_NODE_SIZE, 
                currentRidbagNodeClusterPosition, firstRidBagNodeClusterPos);
        firstRidBagNodeClusterPos = output.firstRidBagNodeClusterPos;
        currentRidbagNodePageIndex = output.currentRidbagNodePageIndex;
        currentRidbagNodePagePosition = output.currentRidbagNodePagePosition;
        currentNodeType = output.currentRidbagNodeType;
        currentRidbagNodeClusterPosition = output.currentRidbagNodeClusterPosition;
      } catch (IOException exc) {
        OLogManager.instance().errorStorage(this, exc.getMessage(), exc);
        throw new ODatabaseException(exc.getMessage());
      }

      ++storedSize;
      addedInvalidRidsIter.remove();
      info.setCurrentNodePageIndex(currentRidbagNodePageIndex);
      info.setCurrentNodePagePosition(currentRidbagNodePagePosition);
      info.setCurrentNodeType(currentNodeType);
      info.setCurrentNodeClusterPosition(currentRidbagNodeClusterPosition);
      info.setFirstNode(firstRidBagNodeClusterPos);
      info.setStoredSize(storedSize);
    }
  }    
  
  @Override
  public void add(OIdentifiable valToAdd) {
    if (valToAdd == null) {
      throw new IllegalArgumentException("Impossible to add a null identifiable in a ridbag");
    }
    
    if (this.owner != null) {
      ORecordInternal.track(this.owner, valToAdd);
    }
    
    synchronized(getLockObject(uuid)){
      RidbagMetadata info = mappedRidbagInfo.get(uuid);
      long size = info.getSize();

      ++size;            
    
      info.setSize(size);
    }
    
    pendingRids.add(valToAdd);
    fireCollectionChangedEvent(
                  new OMultiValueChangeEvent<>(OMultiValueChangeEvent.OChangeType.ADD, valToAdd, valToAdd, null, false));
    //TODO add it to index
  }  
  
  /**
   * merges all tailing node in node of maximum length. Caller should take care of counting tail rids
   * @throws IOException 
   */
  private void nodesMegaMerge() throws IOException{
    RidbagMetadata info = mappedRidbagInfo.get(uuid);
    long currentRidbagNodePageIndex = info.getCurrentNodePageIndex();
    int currentRidbagNodePagePosition = info.getCurrentNodePagePosition();
    byte currentRidbagNodeType = info.getCurrentNodeType();
    long currentRidbagNodeClusterPos = info.getCurrentNodeClusterPosition();
    long firstRidBagNodeClusterPos = info.getFirstNodeClusterPosition();
    
    OFastRidBagPaginatedCluster.MegaMergeOutput output = cluster.nodesMegaMerge(currentRidbagNodePageIndex, 
            currentRidbagNodePagePosition, currentRidbagNodeType, currentRidbagNodeClusterPos, firstRidBagNodeClusterPos, 
            MAX_RIDBAG_NODE_SIZE);
    
    currentRidbagNodeClusterPos = output.currentRidbagNodeClusterPosition;
    currentRidbagNodePageIndex = output.currentRidbagNodePageIndex;
    currentRidbagNodePagePosition = output.currentRidbagNodePagePosition;
    currentRidbagNodeType = output.currentRidbagNodeType;
    firstRidBagNodeClusterPos = output.firstRidBagNodeClusterPos;    
    
    info.setCurrentNodeClusterPosition(currentRidbagNodeClusterPos);
    info.setCurrentNodePageIndex(currentRidbagNodePageIndex);
    info.setCurrentNodePagePosition(currentRidbagNodePagePosition);
    info.setCurrentNodeType(currentRidbagNodeType);
    info.setFirstNode(firstRidBagNodeClusterPos);
  }
  
  @Override
  public void remove(OIdentifiable value) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isEmpty() {
    RidbagMetadata info = mappedRidbagInfo.get(uuid);
    long size = info.getSize();
    
    return size == 0;
  }

  @Override
  public int getSerializedSize() {
    return OLongSerializer.LONG_SIZE;
  }

  @Override
  public int getSerializedSize(byte[] stream, int offset) {
    return OLongSerializer.LONG_SIZE;
  }    
  
  @Override
  public int serialize(byte[] stream, int offset, UUID ownerUuid) {
    synchronized(getLockObject(uuid)){
      processInvalidRidsReferences();
      
      RidbagMetadata info = mappedRidbagInfo.get(uuid);      
      long firstRidBagNodeClusterPos = info.getFirstNodeClusterPosition();
      
      OLongSerializer.INSTANCE.serialize(firstRidBagNodeClusterPos, stream, offset);
    }
   
    return offset + OLongSerializer.LONG_SIZE;
  }

  @Override
  public int deserialize(byte[] stream, int offset) {
    if (mappedRidbagInfo.containsKey(uuid)){
      //potentionaly we can have followed situation here
      //if T1 done serialization to stream, but still didn't persist it onto disk
      //and at that moment T2 tries to read firstNodeInfo from disk
      //than actual first node info can differs from on disk (deserialized) first node info
    }
    else{
      synchronized(getLockObject(uuid)){            
        //find last node            
        long currentRidbagNodeClusterPos;
        long size;
        long storedSize;
        long firstRidBagNodeClusterPos = OLongSerializer.INSTANCE.deserialize(stream, offset);
        currentRidbagNodeClusterPos = firstRidBagNodeClusterPos;
        boolean exit = false;
        try{
          while (exit == false) {
            Long nextNode = cluster.getNextNode(currentRidbagNodeClusterPos, true);
            if (nextNode != null){
              currentRidbagNodeClusterPos = nextNode;              
            }
            else{
              exit = true;
            }
          }
          storedSize = size = getSize(firstRidBagNodeClusterPos);
        }
        catch (IOException exc){
          OLogManager.instance().errorStorage(this, exc.getMessage(), exc);
          throw new ODatabaseException(exc.getMessage());
        }
        try{
        HelperClasses.Tuple<HelperClasses.Tuple<Byte, Long>, Integer> typePageIndexPagePosition = cluster.getPageIndexAndPagePositionAndTypeOfRecord(currentRidbagNodeClusterPos, true);
        RidbagMetadata info = new RidbagMetadata(firstRidBagNodeClusterPos, currentRidbagNodeClusterPos, 
                typePageIndexPagePosition.getFirstVal().getFirstVal(), typePageIndexPagePosition.getSecondVal(), 
                typePageIndexPagePosition.getFirstVal().getFirstVal(),
                size, storedSize);
        mappedRidbagInfo.put(uuid, info);
        }
        catch (IOException exc){
          OLogManager.instance().errorStorage(this, exc.getMessage(), exc);
          throw new ODatabaseException(exc.getMessage());
        }
      }
    }
    
    return offset + OLongSerializer.LONG_SIZE;
  }
  
  @Override
  public void requestDelete() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean contains(OIdentifiable value) {    
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setOwner(ORecord owner) {    

    this.owner = owner;
        
  }

  @Override
  public ORecord getOwner() {
    return owner;
  }

  @Override
  public List<OMultiValueChangeListener<OIdentifiable, OIdentifiable>> getChangeListeners() {
    if (changeListeners == null){
      return Collections.emptyList();
    }
    return Collections.unmodifiableList(changeListeners);
  }

  @Override
  public NavigableMap<OIdentifiable, Change> getChanges() {
    return null;
  }

  @Override
  public void setSize(int size) {
    //do nothing , this size is invalid one
  }

  @Override
  public Iterator<OIdentifiable> iterator() {    
    return new OLinkedListRidBagIterator(this);    
  }

  @Override
  public Iterator<OIdentifiable> rawIterator() {
    return new OLinkedListRidBagIterator(this);
  }

  @Override
  public void convertLinks2Records() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean convertRecords2Links() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isAutoConvertToRecord() {
    return autoConvertToRecord;
  }

  @Override
  public void setAutoConvertToRecord(boolean convertToRecord) {
    autoConvertToRecord = convertToRecord;
  }

  @Override
  public boolean detach() {
    return convertRecords2Links();
  }

  @Override
  public int size() {
    RidbagMetadata info = mappedRidbagInfo.get(uuid);
    long size = info.getSize();    
    return Long.valueOf(size).intValue();
  }

  @Override
  public void addChangeListener(OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener) {
    if (changeListeners == null){
      changeListeners = new LinkedList<>();
    }
    changeListeners.add(changeListener);
  }

  @Override
  public void removeRecordChangeListener(OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener) {
    if (changeListeners != null){
      changeListeners.remove(changeListener);
    }
  }

  @Override
  public Object returnOriginalState(List<OMultiValueChangeEvent<OIdentifiable, OIdentifiable>> changeEvents) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void fireCollectionChangedEvent(OMultiValueChangeEvent<OIdentifiable, OIdentifiable> event) {
    if (changeListeners != null) {
      for (final OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener : changeListeners) {
        if (changeListener != null){
          changeListener.onAfterRecordChanged(event);
        }
      }
    }
  }

  @Override
  public Class<?> getGenericClass() {
    return OIdentifiable.class;
  }

  @Override
  public void replace(OMultiValueChangeEvent<Object, Object> event, Object newValue) {
    //do nothing
  }      

  private long getSize(long firstRidBagNodeClusterPos) throws IOException{
    long size = 0;
    Long iteratingNode = firstRidBagNodeClusterPos;
    while (iteratingNode != null){
      HelperClasses.Tuple<HelperClasses.Tuple<Byte, Long>, Integer> typePageIndexPagePosition = cluster.getPageIndexAndPagePositionAndTypeOfRecord(iteratingNode, autoConvertToRecord);
      byte type = typePageIndexPagePosition.getFirstVal().getFirstVal();
      long pageIndex = typePageIndexPagePosition.getFirstVal().getSecondVal();
      int pagePosition = typePageIndexPagePosition.getSecondVal();
      size += cluster.getNodeSize(pageIndex, pagePosition, type, true);
      iteratingNode = cluster.getNextNode(iteratingNode, true);
    }
    return size;
  }
  
  protected Long getFirstNodeClusterPos(){
    RidbagMetadata info = mappedRidbagInfo.get(uuid);
    return info.getFirstNodeClusterPosition();
  }
  
  protected OFastRidBagPaginatedCluster getCluster(){
    return cluster;
  }
  
  protected List<OIdentifiable> getPendingRids(){
    return pendingRids;
  }
  
  protected UUID getUUID(){
    return uuid;
  }
  
  /**
   * caller should take care of synchronization
   * @return 
   */
  protected RidbagMetadata getCurrentMetadataState(){
    return mappedRidbagInfo.get(uuid);
  }
 
}
