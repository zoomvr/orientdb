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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author marko
 */
public class OLinkedListRidBag implements ORidBagDelegate{           
  
  public static final byte RECORD_TYPE_LINKED_NODE = 'l';
  public static final byte RECORD_TYPE_ARRAY_NODE = 'a';
  
  private Long firstRidBagNodeClusterPos;
  private Long currentRidbagNodeClusterPos;
    
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
  private final UUID uuid = UUID.randomUUID();
  
  private static Map<UUID, Set<Long>> nodesInRidbags = new ConcurrentHashMap<>();
  private static Map<UUID, Set<Long>> deletedNodesInRidbags = new ConcurrentHashMap<>();
  
  private final boolean deserialized;
  
  public OLinkedListRidBag(OFastRidBagPaginatedCluster cluster){
    this.cluster = cluster;
    Set<Long> nodes = new HashSet<>();
    nodesInRidbags.put(uuid, nodes);
    Set<Long> deleted = new HashSet<>();
    deletedNodesInRidbags.put(uuid, deleted);
    deserialized = true;
  }  
  
  public OLinkedListRidBag(OFastRidBagPaginatedCluster cluster, ORID[] rids) throws IOException{    
    this.cluster = cluster;
    this.size = rids.length;
    OFastRidBagPaginatedCluster.MegaMergeOutput output = cluster.firstNodeAllocation(rids, ADDITIONAL_ALLOCATION_SIZE, MAX_RIDBAG_NODE_SIZE);
    firstRidBagNodeClusterPos = output.firstRidBagNodeClusterPos;
    currentRidbagNodeClusterPos = output.currentRidbagNodeClusterPos;
    shouldSaveParentRecord = output.shouldSaveParentRecord;
    for (ORID inputRid : rids){
      addedStillInvalidRids.add(inputRid);
    }
    Set<Long> nodes = new HashSet<>();
    nodes.add(firstRidBagNodeClusterPos);
    nodesInRidbags.put(uuid, nodes);
    Set<Long> deleted = new HashSet<>();
    deletedNodesInRidbags.put(uuid, deleted);
    deserialized = false;
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
  private OIdentifiable processInvalidRidsReferences(){        
    OIdentifiable ret = null;
    //go through collection of invalid rids and check if some become valid
    Iterator<OIdentifiable> addedInvalidRidsIter = addedStillInvalidRids.iterator();
    
    byte currentNodeType;
    long pageIndex;
    int pagePosition;
    
    try{
      HelperClasses.Tuple<HelperClasses.Tuple<Byte, Long>, Integer> currentNodeTypeAndPageIndexAndPagePosition = 
              cluster.getPageIndexAndPagePositionAndTypeOfRecord(currentRidbagNodeClusterPos, true);
      currentNodeType = currentNodeTypeAndPageIndexAndPagePosition.getFirstVal().getFirstVal();
      pageIndex = currentNodeTypeAndPageIndexAndPagePosition.getFirstVal().getSecondVal();
      pagePosition = currentNodeTypeAndPageIndexAndPagePosition.getSecondVal();    
    }
    catch (IOException exc){
      OLogManager.instance().errorStorage(this, exc.getMessage(), exc);
      throw new ODatabaseException(exc.getMessage());
    }
    long previousCurrentNodeClusterPos = currentRidbagNodeClusterPos;
    
    Set<Long> nodes = nodesInRidbags.get(uuid);
    Set<Long> deletedNodes = deletedNodesInRidbags.get(uuid);
    
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

        if (previousCurrentNodeClusterPos != currentRidbagNodeClusterPos){
          HelperClasses.Tuple<HelperClasses.Tuple<Byte, Long>, Integer> currentNodeTypeAndPageIndexAndPagePosition = 
                  cluster.getPageIndexAndPagePositionAndTypeOfRecord(currentRidbagNodeClusterPos, true);
          currentNodeType = currentNodeTypeAndPageIndexAndPagePosition.getFirstVal().getFirstVal();
          pageIndex = currentNodeTypeAndPageIndexAndPagePosition.getFirstVal().getSecondVal();
          pagePosition = currentNodeTypeAndPageIndexAndPagePosition.getSecondVal();    
          previousCurrentNodeClusterPos = currentRidbagNodeClusterPos;
        }
        
        OFastRidBagPaginatedCluster.MegaMergeOutput output = cluster.addRidHighLevel(value, pageIndex, 
                pagePosition, currentNodeType, ADDITIONAL_ALLOCATION_SIZE, MAX_RIDBAG_NODE_SIZE, 
                currentRidbagNodeClusterPos, firstRidBagNodeClusterPos, shouldSaveParentRecord,
                nodes, deletedNodes);
        firstRidBagNodeClusterPos = output.firstRidBagNodeClusterPos;
        currentRidbagNodeClusterPos = output.currentRidbagNodeClusterPos;
        shouldSaveParentRecord = output.shouldSaveParentRecord;
      } catch (IOException exc) {
        OLogManager.instance().errorStorage(this, exc.getMessage(), exc);
        throw new ODatabaseException(exc.getMessage());
      }

      ++storedSize;
      addedInvalidRidsIter.remove();            
      if (ret == null){
        ret = value;
      }      
      nodes.add(firstRidBagNodeClusterPos);
      nodes.add(currentRidbagNodeClusterPos);
    }    

    return ret;
  }
  
  @Override
  public void add(OIdentifiable valToAdd) {
    if (valToAdd == null) {
      throw new IllegalArgumentException("Impossible to add a null identifiable in a ridbag");
    }
    
    addedStillInvalidRids.add(valToAdd);          
    
    ++size;    
    
    if (this.owner != null) {
      ORecordInternal.track(this.owner, valToAdd);
    }
    
    fireCollectionChangedEvent(
                new OMultiValueChangeEvent<>(OMultiValueChangeEvent.OChangeType.ADD, valToAdd, valToAdd, null, false));
    shouldSaveParentRecord = false;
    //TODO add it to index
  }    
  
  /**
   * merges all tailing node in node of maximum length. Caller should take care of counting tail rids
   * @throws IOException 
   */
  private void nodesMegaMerge() throws IOException{
    Set<Long> nodes = nodesInRidbags.get(uuid);
    Set<Long> deletedNodes = deletedNodesInRidbags.get(uuid);
    OFastRidBagPaginatedCluster.MegaMergeOutput output = cluster.nodesMegaMerge(currentRidbagNodeClusterPos, 
            firstRidBagNodeClusterPos, MAX_RIDBAG_NODE_SIZE, shouldSaveParentRecord, nodes, deletedNodes);
    currentRidbagNodeClusterPos = output.currentRidbagNodeClusterPos;
    firstRidBagNodeClusterPos = output.firstRidBagNodeClusterPos;
    shouldSaveParentRecord = output.shouldSaveParentRecord;
    nodes.add(firstRidBagNodeClusterPos);
    nodes.add(currentRidbagNodeClusterPos);
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
    
    Set<Long> nodes = nodesInRidbags.get(uuid);
    nodes.add(firstRidBagNodeClusterPos);
    
    boolean exit = false;
    try{
      while (exit == false) {
        Long nextNode = cluster.getNextNode(currentRidbagNodeClusterPos, true);
        if (nextNode != null){
          currentRidbagNodeClusterPos = nextNode;
          nodes.add(nextNode);
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

  private long getSize() throws IOException{
    long size = 0;
    Long iteratingNode = firstRidBagNodeClusterPos;
    while (iteratingNode != null){
      size += cluster.getNodeSize(iteratingNode, true);
      iteratingNode = cluster.getNextNode(iteratingNode, true);
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
