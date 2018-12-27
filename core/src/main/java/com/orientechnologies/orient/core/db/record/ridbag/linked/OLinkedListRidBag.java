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
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeListener;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBagDelegate;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OVarIntSerializer;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.cluster.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.cluster.linkedridbags.OFastRidBagPaginatedCluster;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.Change;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;

/**
 *
 * @author marko
 */
public class OLinkedListRidBag implements ORidBagDelegate{           
  
  private OIdentifiable ridbagRid;  
  private Map<OIdentifiable, ORidbagNode> indexedRidsNodes;
  private OFastRidBagPaginatedCluster cluster = null;
  private int size = 0;
  private int tailSize = 0;    
      
  protected static final int MAX_RIDBAG_NODE_SIZE = 600;
  private static final int ADDITIONAL_ALLOCATION_SIZE = 20;    
  
  //currently active node
  private ORidbagNode activeNode = null;  
  private ORidbagNode firstNode = null;
  
  private boolean autoConvertToRecord = true;
  private List<OMultiValueChangeListener<OIdentifiable, OIdentifiable>> changeListeners;
  private ORecord owner = null;    
  
  public OLinkedListRidBag(OFastRidBagPaginatedCluster cluster){    
    this.cluster = cluster;
  }
  
  @Override
  public void addAll(Collection<OIdentifiable> values) {    
    values.forEach(this::add);
  }

  @Override
  public void add(OIdentifiable value) {
    if (value == null) {
      throw new IllegalArgumentException("Impossible to add a null identifiable in a ridbag");
    }

    //check for megaMerge
    if (size > 0 && size % MAX_RIDBAG_NODE_SIZE == 0) {
      try {
        nodesMegaMerge();
      } catch (IOException exc) {
        OLogManager.instance().errorStorage(this, exc.getMessage(), exc, (Object[]) null);
        throw new ODatabaseException(exc.getMessage());
      }
    }

    try {
      if (activeNode.isFullNode()) {
        boolean canFitInPage;
        //check if new rid can fit in page with previous tail Node           
        canFitInPage = ifOneMoreFitsToPage(activeNode);

        if (!canFitInPage) {
          int allocateSize = Math.min(tailSize * 2, tailSize + ADDITIONAL_ALLOCATION_SIZE);
          allocateSize = Math.min(allocateSize, MAX_RIDBAG_NODE_SIZE);

          int extraSlots = 1;
          if (allocateSize == MAX_RIDBAG_NODE_SIZE) {
            extraSlots = 0;
          }
          //created merged node
          allocateSize += extraSlots;
          ORidbagNode ridBagNode;
          ridBagNode = createNodeOfSpecificSize(allocateSize, true, activeNode.getClusterPosition());          
          activeNode = ridBagNode;

          OIdentifiable[] mergedTail = mergeTail(extraSlots);
          if (extraSlots == 1) {
            //here we are dealing with node size less than max node size
            mergedTail[mergedTail.length - 1] = value;            
            activeNode.addAll(mergedTail);
            relaxTail();
            //no increment of tailSize bacuse no way that this node is tail node
          } else {
            //here we deal with node which size is equal to max node size          
            activeNode.addAll(mergedTail);
            relaxTail();
            //add new rid to tail
            ORidbagNode node = createNodeOfSpecificSize(1, true, activeNode.getClusterPosition());            
            activeNode = node;
            ++tailSize;
          }
        } else {
          ORidbagNode node = createNodeOfSpecificSize(1, true, activeNode.getClusterPosition());          
          activeNode = node;
          ++tailSize;
        }
      } else {
        if (!activeNode.isLoaded()) {
          activeNode.load();
        }
        if (activeNode.isTailNode()) {
          ++tailSize;
        }
        activeNode.add(value);
      }
    } catch (IOException exc) {
      OLogManager.instance().errorStorage(this, exc.getMessage(), exc);
      throw new ODatabaseException(exc.getMessage());
    }

    ++size;

    if (this.owner != null) {
      ORecordInternal.track(this.owner, value);
    }

    fireCollectionChangedEvent(
            new OMultiValueChangeEvent<>(OMultiValueChangeEvent.OChangeType.ADD, value, value));
    //TODO add it to index
  }
  
  /**
   * 
   */
  private void nodesMegaMerge() throws IOException{
    OIdentifiable[] mergedRids = new OIdentifiable[MAX_RIDBAG_NODE_SIZE];
    int currentOffset = 0;
    ORidbagNode currentIteratingNode = firstNode;
    while (currentIteratingNode != null){      
      if (!currentIteratingNode.isMaxSizeNodeFullNode()){
        if (!currentIteratingNode.isLoaded()){
          currentIteratingNode.load();
        }
        if (currentIteratingNode.currentIndex() > 0){
          OIdentifiable[] nodeRids = currentIteratingNode.getAllRids();
          System.arraycopy(nodeRids, 0, mergedRids, currentOffset, currentIteratingNode.currentIndex());          
                    
          deleteRidbagNodeData(currentIteratingNode);          
        }
      }
      
      currentIteratingNode = getNextNodeOfNode(currentIteratingNode);
    }
    
    ORidbagNode megaNode;
    try{
      megaNode = createNodeOfSpecificSize(mergedRids.length, true, activeNode.getClusterPosition());      
    }
    catch (IOException exc){
      OLogManager.instance().errorStorage(this, exc.getMessage(), exc, (Object[])null);
      throw new ODatabaseException(exc.getMessage());
    }
    
    megaNode.addAll(mergedRids);    
  }
  
  private OIdentifiable[] mergeTail(int extraSlots) {           
    OIdentifiable[] ret = new OIdentifiable[tailSize + extraSlots];
    ORidbagNode currentIteratingNode = firstNode;
    int currentIndex = 0;
    while (currentIteratingNode != null){
      try{
        if (!currentIteratingNode.isLoaded()){
          currentIteratingNode.load();
        }
        if (currentIteratingNode.isTailNode()){
          ret[currentIndex++] = currentIteratingNode.getAt(0);          
        }
        currentIteratingNode = getNextNodeOfNode(currentIteratingNode);
      }
      catch (IOException exc){
        OLogManager.instance().errorStorage(this, exc.getMessage(), exc, (Object[])null);
        throw new ODatabaseException(exc.getMessage());
      }
    }
    return ret;
  }
  
  private void relaxTail(){    
    ORidbagNode previousNode = null;
    ORidbagNode currentIteratingNode = firstNode;
    while (currentIteratingNode != null){
      try{
        if (!currentIteratingNode.isLoaded()){
          currentIteratingNode.load();
        }
        ORidbagNode nextNode = getNextNodeOfNode(currentIteratingNode);
        if (currentIteratingNode.isTailNode()){
          deleteRidbagNodeData(currentIteratingNode);
          --tailSize;
          if (previousNode == null){
            firstNode = nextNode;
          }
          else{
            if (nextNode == null){
              previousNode.setNextNode(null);
            }
            else{
              previousNode.setNextNode(nextNode.getClusterPosition());
              nextNode.setPreviousNode(previousNode.getClusterPosition());
            }
          }
        }
        previousNode = currentIteratingNode;
        currentIteratingNode = nextNode;        
      }
      catch (IOException exc){
        OLogManager.instance().errorStorage(this, exc.getMessage(), exc, (Object[])null);
        throw new ODatabaseException(exc.getMessage());
      }
    }
  }

  private void linkPreviousNodeWithNextAndDeleteCurrent(ORidbagNode currentNode) throws IOException{
    ORidbagNode nextNode = getNextNodeOfNode(currentNode);
    ORidbagNode previousNode = getPreviousNodeOfNode(currentNode);
    deleteRidbagNodeData(currentNode);
    if (previousNode != null){
      if (nextNode != null){
        previousNode.setNextNode(nextNode.getClusterPosition());
      }
      else{
        previousNode.setNextNode(null);
      }
    }
    if (nextNode != null){
      if (previousNode != null){
        nextNode.setPreviousNode(previousNode.getClusterPosition());
      }
      else{
        nextNode.setPreviousNode(null);
      }
    }
  }
  
  private void linkNodeToNode(ORidbagNode previousNode, ORidbagNode nextNode){
    if (previousNode != null){
      if (nextNode != null){
        previousNode.setNextNode(nextNode.getClusterPosition());
      }
      else{
        previousNode.setNextNode(null);
      }
    }
    if (nextNode != null){
      if (previousNode != null){
        nextNode.setPreviousNode(previousNode.getClusterPosition());
      }
      else{
        nextNode.setPreviousNode(null);
      }
    }
  }
  
  @Override
  public void remove(OIdentifiable value) {
    boolean removed = false;    
    if (indexedRidsNodes == null){
      ORidbagNode node = indexedRidsNodes.get(value);
      if (node != null){
        if (!node.isLoaded()){
          try{
            node.load();
          }
          catch (IOException exc){
            OLogManager.instance().errorStorage(this, exc.getMessage(), exc, (Object[])null);
            throw new ODatabaseException(exc.getMessage());
          }
        }
        boolean isTail = node.isTailNode();
        if (node.remove(value)){
          if (isTail){
            --tailSize;
            try{
              linkPreviousNodeWithNextAndDeleteCurrent(node);
            }
            catch (IOException exc){
              OLogManager.instance().errorStorage(this, exc.getMessage(), exc, (Object[])null);
              throw new ODatabaseException(exc.getMessage());
            }
          }      
          removed = true;
        }
      }
    }
    else{
      //go through all
      ORidbagNode previousNode = null;
      ORidbagNode currentIteratingNode = firstNode;
      while(currentIteratingNode != null){
        try{
          if (!currentIteratingNode.isLoaded()){            
            currentIteratingNode.load();            
          }
          boolean isTail = currentIteratingNode.isTailNode();
          if (currentIteratingNode.remove(value)){
            if (isTail){
              --tailSize;
              ORidbagNode nextNode = getNextNodeOfNode(currentIteratingNode);
              linkNodeToNode(previousNode, nextNode);
              deleteRidbagNodeData(currentIteratingNode);              
            }      
            removed = true;
            break;
          }

          previousNode = currentIteratingNode;
          currentIteratingNode = getNextNodeOfNode(currentIteratingNode);
        }
        catch (IOException exc){
          OLogManager.instance().errorStorage(this, exc.getMessage(), exc, (Object[])null);
          throw new ODatabaseException(exc.getMessage());
        }
      }
    }
    if (removed){
      --size;
      fireCollectionChangedEvent(
          new OMultiValueChangeEvent<>(OMultiValueChangeEvent.OChangeType.REMOVE, value, null,
              value));
    }    
  }

  @Override
  public boolean isEmpty() {
    return size == 0;
  }

  @Override
  public int getSerializedSize() {
    BytesContainer container = new BytesContainer();
    serializeInternal(container);
    
    return container.offset;
  }

  @Override
  public int getSerializedSize(byte[] stream, int offset) {
    BytesContainer container = new BytesContainer();    
    serializeInternal(container);    
    
    return container.offset;
  }  
  
  private void serializeInternal(BytesContainer container){
    //serialize currentSize
    OVarIntSerializer.write(container, size);
    //serialize tail size
    OVarIntSerializer.write(container, tailSize);
    
    //serialize active node
    if (activeNode != null){           
      OVarIntSerializer.write(container, activeNode.getClusterPosition());
    }
    else{
      OVarIntSerializer.write(container, -1l);
    }
    
    //serialize first node
    if (firstNode != null){
      OVarIntSerializer.write(container, firstNode.getClusterPosition());
    }
    else{
      OVarIntSerializer.write(container, -1l);
    }
  }
  
//  private void serializeNodeData(ORidbagNode node) throws IOException{
//    byte[] serialized = node.serialize();    
//    OPhysicalPosition ppos = new OPhysicalPosition(node.getClusterPosition());
//    OPaginatedCluster.RECORD_STATUS status = cluster.getRecordStatus(node.getClusterPosition());
//    if (status == OPaginatedCluster.RECORD_STATUS.ALLOCATED || status == OPaginatedCluster.RECORD_STATUS.REMOVED){
//      cluster.createRecord(serialized, node.getVersion(), ORidbagNode.RECORD_TYPE, ppos);
//    }
//    else if (status == OPaginatedCluster.RECORD_STATUS.PRESENT){
//      cluster.updateRecord(ppos.clusterPosition, serialized, node.getVersion(), ORidbagNode.RECORD_TYPE);
//    }
//  }
  
  @Override
  public int serialize(byte[] stream, int offset, UUID ownerUuid) {
    BytesContainer container = new BytesContainer(stream, offset);    
    serializeInternal(container);    
    return container.offset;
  }

  @Override
  public int deserialize(byte[] stream, int offset) {
    BytesContainer container = new BytesContainer(stream, offset);
    //deserialize size
    size = OVarIntSerializer.readAsInteger(container);
    
    //deserialize tail size   
    tailSize = OVarIntSerializer.readAsInteger(container);
    
    //deserialize reference to active node
    long activeNodeRef = OVarIntSerializer.readAsLong(container);
    if (activeNodeRef == -1){
      activeNode = null;
    }
    else{
      activeNode = new ORidbagNode(activeNodeRef, false, cluster);
      try{
        activeNode.load();
      }
      catch (IOException exc){
        OLogManager.instance().errorStorage(this, exc.getMessage(), exc, (Object[])null);
        throw new ODatabaseException(exc.getMessage());
      }
    }
    
    //deserialize first node ref
    long firstNodeRef = OVarIntSerializer.readAsLong(container);
    if (firstNodeRef == -1){
      firstNode = null;
    }
    else{
      firstNode = new ORidbagNode(firstNodeRef, false, cluster);
      try{
        firstNode.load();
      }
      catch (IOException exc){
        OLogManager.instance().errorStorage(this, exc.getMessage(), exc, (Object[])null);
        throw new ODatabaseException(exc.getMessage());
      }
    }
    
    return container.offset;
  }

  private void deleteRidbagNodeData(ORidbagNode node) throws IOException{
    OPhysicalPosition nodesRidPos = new OPhysicalPosition(node.getClusterPosition());
    OPhysicalPosition pos = cluster.getPhysicalPosition(nodesRidPos);
    cluster.deleteRecord(pos.clusterPosition);
  }
  
  @Override
  public void requestDelete() {
    ORidbagNode currentIteratingNode = firstNode;
    while (currentIteratingNode != null){
      try{
        //have to do load because we need reference to next node
        if (currentIteratingNode.isLoadedMetdata()){            
          currentIteratingNode.loadMetadata();
        }
        deleteRidbagNodeData(currentIteratingNode);

        currentIteratingNode = getNextNodeOfNode(currentIteratingNode);
      }
      catch (IOException exc){
        OLogManager.instance().errorStorage(this, exc.getMessage(), exc, (Object[])null);
        throw new ODatabaseException(exc.getMessage());
      }
    }
  }

  @Override
  public boolean contains(OIdentifiable value) {    
    if (indexedRidsNodes == null){
      ORidbagNode node = indexedRidsNodes.get(value);
      if (node != null){
        if (!node.isLoaded()){
          try{
            node.load();
          }
          catch (IOException exc){
            OLogManager.instance().errorStorage(this, exc.getMessage(), exc, (Object[])null);
            throw new ODatabaseException(exc.getMessage());
          }
        }
        return node.contains(value);
      }
    }
    else{    
      //go through all
      ORidbagNode currentIteratingNode = firstNode;
      while (currentIteratingNode != null){
        try{
          if (currentIteratingNode.isLoaded()){            
            currentIteratingNode.load();            
          }
          if (currentIteratingNode.contains(value)){
            return true;
          }

          currentIteratingNode = getNextNodeOfNode(currentIteratingNode);
        }
        catch (IOException exc){
          OLogManager.instance().errorStorage(this, exc.getMessage(), exc, (Object[])null);
          throw new ODatabaseException(exc.getMessage());
        }
      }
    }
    return false;
  }

  @Override
  public void setOwner(ORecord owner) {
    throw new UnsupportedOperationException("Not implemented");
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
    this.size = size;
  }

  @Override
  public Iterator<OIdentifiable> iterator() {    
    return new OLinkedListRidBagIterator(this, cluster);    
  }

  @Override
  public Iterator<OIdentifiable> rawIterator() {
    return new OLinkedListRidBagIterator(this, cluster);
  }

  @Override
  public void convertLinks2Records() {
    ORidbagNode currentIteratingNode = firstNode;
    try{
      while (currentIteratingNode != null){      
        if (!currentIteratingNode.isLoaded()){
          try{
            currentIteratingNode.load();
          }
          catch (IOException exc){
            OLogManager.instance().errorStorage(this, exc.getMessage(), exc, (Object[])null);
            throw new ODatabaseException(exc.getMessage());
          }
        }
        for (int i = 0; i < currentIteratingNode.currentIndex(); i++){
          OIdentifiable id = currentIteratingNode.getAt(i);
          if (id.getRecord() != null){
            currentIteratingNode.setAt(id.getRecord(), i);
          }
        }

        currentIteratingNode = getNextNodeOfNode(currentIteratingNode);
      }
    }
    catch (IOException exc){
      OLogManager.instance().errorStorage(this, exc.getMessage(), exc, (Object[])null);
      throw new ODatabaseException(exc.getMessage());
    }
  }

  @Override
  public boolean convertRecords2Links() {
    ORidbagNode currentIteratingNode = firstNode;
    while (currentIteratingNode != null){      
      try{
        if (!currentIteratingNode.isLoaded()){        
          currentIteratingNode.load();        
        }
        for (int i = 0; i < currentIteratingNode.currentIndex(); i++){
          OIdentifiable id = currentIteratingNode.getAt(i);
          if (id instanceof ORecord){
            ORecord rec = (ORecord)id;
            currentIteratingNode.setAt(rec.getIdentity(), i);
          }
          else{
            return false;
          }
        }

        currentIteratingNode = getNextNodeOfNode(currentIteratingNode);
      }
      catch (IOException exc){
        OLogManager.instance().errorStorage(this, exc.getMessage(), exc, (Object[])null);
        throw new ODatabaseException(exc.getMessage());
      }
    }        

    return true;
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
    return size;
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
    final OLinkedListRidBag reverted = new OLinkedListRidBag(cluster);    
    ORidbagNode currentIteratingNode = firstNode;
    while (currentIteratingNode != null){      
      try{
        if (!currentIteratingNode.isLoaded()){       
          currentIteratingNode.load();          
        }
        for (int i = 0; i < currentIteratingNode.currentIndex(); i++){
          OIdentifiable id = currentIteratingNode.getAt(i);
          reverted.add(id);
        }

        currentIteratingNode = getNextNodeOfNode(currentIteratingNode);
      }
      catch (IOException exc){
        OLogManager.instance().errorStorage(this, exc.getMessage(), exc, (Object[])null);
        throw new ODatabaseException(exc.getMessage());
      }
    }

    final ListIterator<OMultiValueChangeEvent<OIdentifiable, OIdentifiable>> listIterator = changeEvents
        .listIterator(changeEvents.size());

    while (listIterator.hasPrevious()) {
      final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> event = listIterator.previous();
      switch (event.getChangeType()) {
      case ADD:
        reverted.remove(event.getKey());
        break;
      case REMOVE:
        reverted.add(event.getOldValue());
        break;
      default:
        throw new IllegalArgumentException("Invalid change type : " + event.getChangeType());
      }
    }

    return reverted;
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

  public OPaginatedCluster getCluster() {
    return cluster;
  }

  public void setCluster(OFastRidBagPaginatedCluster cluster) {
    this.cluster = cluster;
  }
  
  private ORidbagNode createNodeOfSpecificSize(int numberOfRids, boolean considerNodeLoaded, Long previousNode) throws IOException{
    OPhysicalPosition newNodePhysicalPosition = cluster.allocatePosition(ORidbagNode.RECORD_TYPE_LINKED_NODE);
    ORidbagNode ret;    
    
    ret = new ORidbagNode(newNodePhysicalPosition.clusterPosition, numberOfRids, considerNodeLoaded, cluster);
    ret.setPreviousNode(previousNode);
    ret.setNextNode(null);
    
    ret.initInCluster();
    
    return ret;
  }
    
  private boolean ifOneMoreFitsToPage(ORidbagNode referenceNode) throws IOException{    
    return cluster.checkIfNewContentFitsInPage(referenceNode.getClusterPosition(), ORidbagNode.getSerializedSize(1));
  }     
  
  ORidbagNode getNextNodeOfNode(ORidbagNode node) throws IOException{
    Long nextNodeRef = node.getNextNode();
    if (nextNodeRef == null || nextNodeRef == -1){
      return null;
    }
    else{
      ORidbagNode nextNode = new ORidbagNode(nextNodeRef, false, cluster);      
      return nextNode;
    }
  }
  
  ORidbagNode getPreviousNodeOfNode(ORidbagNode node) throws IOException{
    Long previousNodeRef = node.getPreviousNode();
    if (previousNodeRef == null || previousNodeRef == -1){
      return null;
    }
    else{
      ORidbagNode nextNode = new ORidbagNode(previousNodeRef, false, cluster);      
      return nextNode;
    }
  }
  
  protected ORidbagNode getFirstNode(){
    return firstNode;
  }
}
