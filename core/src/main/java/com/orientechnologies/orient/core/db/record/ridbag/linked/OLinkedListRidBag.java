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
import com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses;
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
  
  public static final byte RECORD_TYPE_LINKED_NODE = 'l';
  public static final byte RECORD_TYPE_ARRAY_NODE = 'a';
  
  private Long firstRidBagNodeClusterPos;
  private Long currentRidbagNodeClusterPos;
  
  private Map<OIdentifiable, Long> indexedRidsNodes;
  private OFastRidBagPaginatedCluster cluster = null;
  private int tailSize = 0;    
      
  protected static final int MAX_RIDBAG_NODE_SIZE = 600;
  private static final int ADDITIONAL_ALLOCATION_SIZE = 20;
  
  private boolean autoConvertToRecord = true;
  private List<OMultiValueChangeListener<OIdentifiable, OIdentifiable>> changeListeners;
  private final ORecord owner = null;    
  
  public OLinkedListRidBag(OFastRidBagPaginatedCluster cluster){    
    this.cluster = cluster;
  }
  
  @Override
  public void addAll(Collection<OIdentifiable> values) {    
    values.forEach(this::add);
  }

  private HelperClasses.Tuple<Boolean, Byte> isCurrentNodeFullNode() throws IOException{
    HelperClasses.Tuple<Byte, Long> pageIndexAndType = cluster.getPageIndexAndTypeOfRecord(currentRidbagNodeClusterPos);
    Byte type = pageIndexAndType.getFirstVal();
    if (type == RECORD_TYPE_LINKED_NODE){
      return new HelperClasses.Tuple<>(ifOneMoreFitsToPage(currentRidbagNodeClusterPos), type);
    }
    else if (type == RECORD_TYPE_ARRAY_NODE){
      return new HelperClasses.Tuple<>(cluster.isArrayNodeFull(currentRidbagNodeClusterPos), type);
    }
    throw new ODatabaseException("Invalid record type in cluster position: " + currentRidbagNodeClusterPos);
  }
  
  @Override
  public void add(OIdentifiable value) {
    if (value == null) {
      throw new IllegalArgumentException("Impossible to add a null identifiable in a ridbag");
    }

    //check for megaMerge
    long size = getSize();
    if (size > 0 && size % MAX_RIDBAG_NODE_SIZE == 0) {
      try {
        nodesMegaMerge();
      } catch (IOException exc) {
        OLogManager.instance().errorStorage(this, exc.getMessage(), exc, (Object[]) null);
        throw new ODatabaseException(exc.getMessage());
      }
    }

    try {
      HelperClasses.Tuple<Boolean, Byte> isCurrentFullAndType = isCurrentNodeFullNode();
      boolean isCurrentNodeFull = isCurrentFullAndType.getFirstVal();
      byte currentNodeType = isCurrentFullAndType.getSecondVal();
      if (isCurrentNodeFull) {
        if (currentNodeType == RECORD_TYPE_LINKED_NODE){
          
        }
        else if (currentNodeType  == RECORD_TYPE_ARRAY_NODE){
          
        }
      } 
      else {
        switch (currentNodeType){
          case RECORD_TYPE_LINKED_NODE:
            cluster.addRidToLinkedNode(value.getIdentity(), currentRidbagNodeClusterPos);
            break;
          case RECORD_TYPE_ARRAY_NODE:
            HelperClasses.Tuple<Long, Integer> pgaeIndexAndPagePosition = cluster.getPageIndexAndPagePositionOfRecord(currentRidbagNodeClusterPos);
            cluster.addRidToArrayNode(value.getIdentity(), currentRidbagNodeClusterPos, pgaeIndexAndPagePosition.getSecondVal());
            break;
          default:
            throw new ODatabaseException("Invalid record type in cluster position: " + currentRidbagNodeClusterPos);
        }
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
    
  private boolean ifOneMoreFitsToPage(long nodeClusterPosition) throws IOException{    
    return cluster.checkIfNewContentFitsInPage(nodeClusterPosition, OFastRidBagPaginatedCluster.getRidEntrySize());
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
 
}
