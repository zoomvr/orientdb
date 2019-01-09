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
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.cluster.linkedridbags.OFastRidBagPaginatedCluster;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.Change;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;

/**
 *
 * @author marko
 */
public class OLinkedListRidBag implements ORidBagDelegate{           
  
  public static final byte RECORD_TYPE_LINKED_NODE = 'l';
  public static final byte RECORD_TYPE_ARRAY_NODE = 'a';
  
  private Long firstRidBagNodeClusterPos;
  private Long currentRidbagNodeClusterPos;
  
  private Map<OIdentifiable, Long> indexedRidsNodes;
  private OFastRidBagPaginatedCluster cluster = null;
      
  protected static final int MAX_RIDBAG_NODE_SIZE = 600;
  private static final int ADDITIONAL_ALLOCATION_SIZE = 20;
  
  private boolean autoConvertToRecord = true;
  private List<OMultiValueChangeListener<OIdentifiable, OIdentifiable>> changeListeners;
  private ORecord owner = null;  

  //cached size of ridbag
  private long size = 0;
  private long storedSize = 0;
  
  private boolean shouldSaveParentRecord = false;
  
  private final List<OIdentifiable> addedStillInvalidRids = new LinkedList<>();
  
  private static int calculateArrayRidNodeAllocationSize(final int initialNumberOfRids){
    int size = Math.min(initialNumberOfRids + ADDITIONAL_ALLOCATION_SIZE, initialNumberOfRids * 2);
    size = Math.min(size, MAX_RIDBAG_NODE_SIZE);
    return size;
  }
  
  public OLinkedListRidBag(OFastRidBagPaginatedCluster cluster){
    this.cluster = cluster;
  }
  
  public OLinkedListRidBag(OFastRidBagPaginatedCluster cluster, ORID firstRid) throws IOException{    
    this.cluster = cluster;
    OPhysicalPosition allocatedPos = cluster.allocatePosition(RECORD_TYPE_LINKED_NODE);
    long clusterPosition = cluster.addRid(firstRid, allocatedPos, -1l, -1l);
    firstRidBagNodeClusterPos = currentRidbagNodeClusterPos = clusterPosition;
    storedSize = size = 1;
    shouldSaveParentRecord = true;
  }
  
  private void fillRestOfArrayWithDummyRids(final ORID[] array, final int lastValidIndex){
    for (int i = lastValidIndex + 1; i < array.length; i++){
      array[i] = new ORecordId(-1, -1);
    }
  }
  
  public OLinkedListRidBag(OFastRidBagPaginatedCluster cluster, ORID[] rids) throws IOException{    
    this.cluster = cluster;
    final OPhysicalPosition allocatedPos = cluster.allocatePosition(RECORD_TYPE_ARRAY_NODE);
    final int size = calculateArrayRidNodeAllocationSize(rids.length);
    final ORID[] toAllocate = new ORID[size];
//    System.arraycopy(rids, 0, toAllocate, 0, rids.length);
    fillRestOfArrayWithDummyRids(toAllocate, -1);
    long clusterPosition = cluster.addRids(toAllocate, allocatedPos, -1l, -1l, -1);
    firstRidBagNodeClusterPos = currentRidbagNodeClusterPos = clusterPosition;
    //storedSize should remain 0
    this.size = rids.length;
    shouldSaveParentRecord = true;
    for (ORID inputRid : rids){
      addedStillInvalidRids.add(inputRid);
    }
  }
  
  @Override
  public void addAll(Collection<OIdentifiable> values) {    
    values.forEach(this::add);
  }

  private HelperClasses.Tuple<Boolean, Byte> isCurrentNodeFullNode() throws IOException{
    HelperClasses.Tuple<Byte, Long> pageIndexAndType = cluster.getPageIndexAndTypeOfRecord(currentRidbagNodeClusterPos);
    Byte type = pageIndexAndType.getFirstVal();
    if (type == RECORD_TYPE_LINKED_NODE){
      return new HelperClasses.Tuple<>(!ifOneMoreFitsToPage(currentRidbagNodeClusterPos), type);
    }
    else if (type == RECORD_TYPE_ARRAY_NODE){
      return new HelperClasses.Tuple<>(cluster.isArrayNodeFull(currentRidbagNodeClusterPos), type);
    }
    throw new ODatabaseException("Invalid record type in cluster position: " + currentRidbagNodeClusterPos);
  }
  
  /**
   * process all previously invalid rids, which are valid now
   * @param fireChangedEvent
   * 
   */
  private OIdentifiable processInvalidRidsReferences(){        
    OIdentifiable ret = null;
    //go through collection of invalid rids and check if some become valid
    Iterator<OIdentifiable> addedInvalidRidsIter = addedStillInvalidRids.iterator();
    while (addedInvalidRidsIter.hasNext()){
      OIdentifiable value = addedInvalidRidsIter.next();
      //process only persistent rids
      if (!value.getIdentity().isPersistent()){
        continue;
      }
      try {
        //check for megaMerge
        if (storedSize > 0 && storedSize % MAX_RIDBAG_NODE_SIZE == 0) {
          try {
            nodesMegaMerge();
          } catch (IOException exc) {
            OLogManager.instance().errorStorage(this, exc.getMessage(), exc, (Object[]) null);
            throw new ODatabaseException(exc.getMessage());
          }
        }

        HelperClasses.Tuple<Boolean, Byte> isCurrentFullAndType = isCurrentNodeFullNode();
        boolean isCurrentNodeFull = isCurrentFullAndType.getFirstVal();
        byte currentNodeType = isCurrentFullAndType.getSecondVal();
        if (isCurrentNodeFull) {
          //By this algorithm allways link node is followed with array node and vice versa
          if (currentNodeType == RECORD_TYPE_LINKED_NODE){
            ORID[] currentNodeRids = cluster.getAllRidsFromLinkedNode(currentRidbagNodeClusterPos);
            OPhysicalPosition newNodePhysicalPosition = cluster.allocatePosition(RECORD_TYPE_ARRAY_NODE);
            int newNodePreallocatedSize = calculateArrayRidNodeAllocationSize(currentNodeRids.length);
            ORID[] newNodePreallocatedRids = new ORID[newNodePreallocatedSize];
            System.arraycopy(currentNodeRids, 0, newNodePreallocatedRids, 0, currentNodeRids.length);
            newNodePreallocatedRids[currentNodeRids.length] = value.getIdentity();
            fillRestOfArrayWithDummyRids(newNodePreallocatedRids, currentNodeRids.length);
            long newNodeClusterPosition = cluster.addRids(currentNodeRids, newNodePhysicalPosition, currentRidbagNodeClusterPos, 
                    -1l, currentNodeRids.length);
            cluster.removeNode(currentRidbagNodeClusterPos);
            if (currentRidbagNodeClusterPos.equals(firstRidBagNodeClusterPos)){
              firstRidBagNodeClusterPos = newNodeClusterPosition;
              shouldSaveParentRecord = true;
            }
            //update previous node prev and next info
            cluster.updatePrevNextNodeinfo(currentRidbagNodeClusterPos, null, newNodeClusterPosition);
            currentRidbagNodeClusterPos = newNodeClusterPosition;          
          }
          else if (currentNodeType  == RECORD_TYPE_ARRAY_NODE){
            OPhysicalPosition newNodePhysicalPosition = cluster.allocatePosition(RECORD_TYPE_ARRAY_NODE);
            long newNodeClusterPosition = cluster.addRid(value.getIdentity(), newNodePhysicalPosition, currentRidbagNodeClusterPos, -1l);
            cluster.updatePrevNextNodeinfo(currentRidbagNodeClusterPos, null, newNodeClusterPosition);
            currentRidbagNodeClusterPos = newNodeClusterPosition;
          }
          else{
            throw new ODatabaseException("Invalid record type in cluster position: " + currentRidbagNodeClusterPos);
          }
        } 
        else {
          switch (currentNodeType){
            case RECORD_TYPE_LINKED_NODE:
              cluster.addRidToLinkedNode(value.getIdentity(), currentRidbagNodeClusterPos);
              break;
            case RECORD_TYPE_ARRAY_NODE:
              HelperClasses.Tuple<Long, Integer> pageIndexAndPagePosition = cluster.getPageIndexAndPagePositionOfRecord(currentRidbagNodeClusterPos);
              cluster.addRidToArrayNode(value.getIdentity(), pageIndexAndPagePosition.getFirstVal(), pageIndexAndPagePosition.getSecondVal());
              break;
            default:
              throw new ODatabaseException("Invalid record type in cluster position: " + currentRidbagNodeClusterPos);
          }
        }
      } catch (IOException exc) {
        OLogManager.instance().errorStorage(this, exc.getMessage(), exc);
        throw new ODatabaseException(exc.getMessage());
      }

      ++storedSize;
      addedInvalidRidsIter.remove();            
      if (ret == null){
        ret = value;
      }
    }    

    return ret;
  }
  
  @Override
  public void add(OIdentifiable valToAdd) {
    if (valToAdd == null) {
      throw new IllegalArgumentException("Impossible to add a null identifiable in a ridbag");
    }
    
    addedStillInvalidRids.add(valToAdd);    
    
  //processInvalidRidsReferences();
    
    ++size;    
    
    if (this.owner != null) {
      ORecordInternal.track(this.owner, valToAdd);
    }
    
    fireCollectionChangedEvent(
                new OMultiValueChangeEvent<>(OMultiValueChangeEvent.OChangeType.ADD, valToAdd, valToAdd, null, false));
    shouldSaveParentRecord = false;
    //TODO add it to index
  }
  
  private boolean isMaxSizeNodeFullNode(long nodeClusterPosition) throws IOException{
    HelperClasses.Tuple<Byte, Long> nodeTypeAndPageIndex = cluster.getPageIndexAndTypeOfRecord(nodeClusterPosition);
    if (nodeTypeAndPageIndex.getFirstVal() == RECORD_TYPE_ARRAY_NODE){
      return cluster.getNodeSize(nodeClusterPosition) == MAX_RIDBAG_NODE_SIZE;
    }
    return false;
  }
  
  /**
   * merges all tailing node in node of maximum length. Caller should take care of counting tail rids
   * @throws IOException 
   */
  private void nodesMegaMerge() throws IOException{
    final ORID[] mergedRids = new ORID[MAX_RIDBAG_NODE_SIZE];
    int currentOffset = 0;
    Long currentIteratingNode = firstRidBagNodeClusterPos;
    boolean removedFirstNode = false;
    long lastNonRemovedNode = -1;
    while (currentIteratingNode != null){
      boolean fetchedNextNode = false;
      if (!isMaxSizeNodeFullNode(currentIteratingNode)){
        final HelperClasses.Tuple<Byte, Long> nodePageIndexAndType = cluster.getPageIndexAndTypeOfRecord(currentIteratingNode);
        final byte type = nodePageIndexAndType.getFirstVal();
        final ORID[] nodeRids;
        if (type == RECORD_TYPE_LINKED_NODE){
          nodeRids = cluster.getAllRidsFromLinkedNode(currentIteratingNode);          
        }
        else if (type == RECORD_TYPE_ARRAY_NODE){
          nodeRids = cluster.getAllRidsFromArrayNode(currentIteratingNode);
        }
        else{
          throw new ODatabaseException("Invalid node type: " + type);
        }
        for (int i = 0; i < nodeRids.length; i++){
          if (nodeRids[i] == null){
            int a = 0;
            ++a;
          }
        }
        System.arraycopy(nodeRids, 0, mergedRids, currentOffset, nodeRids.length);
        long tmpCurrNodeClusterPos = currentIteratingNode;
        currentIteratingNode = cluster.getNextNode(currentIteratingNode);
        cluster.removeNode(tmpCurrNodeClusterPos);
        currentOffset += nodeRids.length;
        if (tmpCurrNodeClusterPos == firstRidBagNodeClusterPos){
          removedFirstNode = true;          
        }
        fetchedNextNode = true;
      }
      else{
        lastNonRemovedNode = currentIteratingNode;
      }
      if (!fetchedNextNode){
        currentIteratingNode = cluster.getNextNode(currentIteratingNode);
      }
    }
    
    OPhysicalPosition megaNodeAllocatedPosition = cluster.allocatePosition(RECORD_TYPE_ARRAY_NODE);
    long megaNodeClusterPosition = cluster.addRids(mergedRids, megaNodeAllocatedPosition, lastNonRemovedNode, -1l, MAX_RIDBAG_NODE_SIZE - 1);
    //set next node of previous node
    if (lastNonRemovedNode != -1){
      cluster.updatePrevNextNodeinfo(lastNonRemovedNode, null, megaNodeClusterPosition);
    }
    currentRidbagNodeClusterPos = megaNodeClusterPosition;
    if (removedFirstNode){
      firstRidBagNodeClusterPos = megaNodeClusterPosition;
      shouldSaveParentRecord = true;
    }
  }
  
  @Override
  public void remove(OIdentifiable value) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isEmpty() {
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
    OIdentifiable firstStored = processInvalidRidsReferences();    
    if (firstStored != null && shouldSaveParentRecord){
      fireCollectionChangedEvent(
                new OMultiValueChangeEvent<>(OMultiValueChangeEvent.OChangeType.ADD, firstStored, firstStored, null, true));
      shouldSaveParentRecord = false;
    }
    
    OLongSerializer.INSTANCE.serialize(firstRidBagNodeClusterPos, stream, offset);
    return offset + OLongSerializer.LONG_SIZE;
  }

  @Override
  public int deserialize(byte[] stream, int offset) {
    currentRidbagNodeClusterPos = firstRidBagNodeClusterPos = OLongSerializer.INSTANCE.deserialize(stream, offset);
    //find last node
    boolean exit = false;
    try{
      while (exit == false) {
        Long nextNode = cluster.getNextNode(currentRidbagNodeClusterPos);
        if (nextNode != null){
          currentRidbagNodeClusterPos = nextNode;
        }
        else{
          exit = true;
        }
      }
      storedSize = size = getSize();
    }
    catch (IOException exc){
      OLogManager.instance().errorStorage(this, exc.getMessage(), exc, (Object[])null);
      throw new ODatabaseException(exc.getMessage());
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
    
  //checks if one more rid can be stored on the same page of link ridbag node
  private boolean ifOneMoreFitsToPage(long nodeClusterPosition) throws IOException{    
    boolean val = cluster.checkIfNewContentFitsInPage(nodeClusterPosition, OFastRidBagPaginatedCluster.getRidEntrySize());    
    return val;
  }

  private long getSize() throws IOException{
    long size = 0;
    Long iteratingNode = firstRidBagNodeClusterPos;
    while (iteratingNode != null){
      size += cluster.getNodeSize(iteratingNode);
      iteratingNode = cluster.getNextNode(iteratingNode);
    }
    return size;
  }
  
  protected Long getFirstNodeClusterPos(){
    return firstRidBagNodeClusterPos;
  }
  
  protected OFastRidBagPaginatedCluster getCluster(){
    return cluster;
  }
 
}
