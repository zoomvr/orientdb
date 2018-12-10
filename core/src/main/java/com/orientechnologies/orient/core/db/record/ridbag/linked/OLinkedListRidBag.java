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

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeListener;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBagDelegate;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OVarIntSerializer;
import com.orientechnologies.orient.core.storage.cluster.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.Change;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;

/**
 *
 * @author marko
 */
public class OLinkedListRidBag implements ORidBagDelegate{           
  
  private OIdentifiable ridbagRid;  
  private Map<OIdentifiable, ORidbagNode> indexedRidsNodes;
  private OPaginatedCluster cluster = null;
  private int size = 0;
  private final List<ORidbagNode> ridbagNodes = new LinkedList<>();
  //tail is composed of nodes of size 1
  private int tailSize = 0;
  //this is internal ridbag list of free nodes already allocated. Differs from cluster free space
  private final Queue<ORidbagNode> freeNodes = new LinkedList<>();
  
  protected static final int MAX_RIDBAG_NODE_SIZE = 600;
  private static final int ADDITIONAL_ALLOCATION_SIZE = 20;
  public static final byte RECORD_TYPE = 'l';
  private static boolean hardRelaxPolicy = false;
  
  //if some node is made by merging, until its capacity is fullfillled it should be active node
  private ORidbagNode activeNode = null; 
  
  public OLinkedListRidBag(){
    
  }
  
  public OLinkedListRidBag(OPaginatedCluster cluster){    
    this.cluster = cluster;
  }
  
  @Override
  public void addAll(Collection<OIdentifiable> values) {    
    for (OIdentifiable rid : values){
      add(rid);
    }
  }

  @Override
  public void add(OIdentifiable value) {    
    
    //check for megaMerge
    if (size % MAX_RIDBAG_NODE_SIZE == 0){
      nodesMegaMerge(hardRelaxPolicy);
    }
    
    //check if there is active node with free space
    if (activeNode != null){
      if (activeNode.currentIndex() >= activeNode.capacity()){
        activeNode = freeNodes.poll();        
      }      
    }
    
    if (activeNode == null){
      //handle add to tail
      boolean canFitInPage = ifOneMoreFitsToPage();
      if (!canFitInPage){
        int allocateSize = Math.min(tailSize * 2, tailSize + ADDITIONAL_ALLOCATION_SIZE);
        allocateSize = Math.min(allocateSize, MAX_RIDBAG_NODE_SIZE);
        
        int extraSlots = 1;
        if (allocateSize == MAX_RIDBAG_NODE_SIZE){
          extraSlots = 0;
        }
        //created merged node
        allocateSize += extraSlots;
        ORidbagNode ridBagNode = getOrCreateNodeOfSpecificSize(allocateSize);
        
        OIdentifiable[] mergedTail = mergeTail(extraSlots);
        if (extraSlots == 1){
          //here we are dealing with node size less than max node size
          mergedTail[mergedTail.length - 1] = value;
          activeNode = ridBagNode;
          activeNode.addAll(mergedTail);          
          relaxTail(hardRelaxPolicy);
          //no increment of tailSize bacuse no way that this node is tail node
        }
        else{
          //here we deal with node which size is equal to max node size          
          ridBagNode.addAll(mergedTail);          
          relaxTail(hardRelaxPolicy);
          //add new rid
          //first check for some free space in existing, now, empty nodes
          activeNode = getOrCreateNodeOfSpecificSize(1);
          if (activeNode.isTailNode()){
            ++tailSize;
          }
        }                
      }
      else{        
        activeNode = getOrCreateNodeOfSpecificSize(1);
        activeNode.add(value);        
        if (activeNode.isTailNode()){
          ++tailSize;
        }
      }
    }
    else{
      if (!activeNode.isLoaded()){
        activeNode.load();
      }
      activeNode.add(value);
      if (activeNode.isTailNode()){
        tailSize++;
      }
    }
    
    ++size;
    //TODO add it to index
  }
  
  /**
   * 
   */
  private void nodesMegaMerge(boolean hardRelax){
    OIdentifiable[] mergedRids = new OIdentifiable[MAX_RIDBAG_NODE_SIZE];
    int currentOffset = 0;
    Iterator<ORidbagNode> iter = ridbagNodes.iterator();
    while (iter.hasNext()){
      ORidbagNode node = iter.next();
      if (!node.isMaxSizeNodeFullNode()){
        if (!node.isLoaded()){
          node.load();
        }
        if (node.currentIndex() > 0){
          OIdentifiable[] nodeRids = node.getAllRids();
          System.arraycopy(nodeRids, 0, mergedRids, currentOffset, node.currentIndex());
          iter.remove();
          if (node.isTailNode()){
            --tailSize;
          }
          if (hardRelax){
            //release it in cluster
          }
          else{            
            node.reset();
            freeNodes.add(node);
          }
        }
      }
    }
    
    ORidbagNode megaNode = getOrCreateNodeOfSpecificSize(mergedRids.length);
    megaNode.addAll(mergedRids);
    ridbagNodes.add(megaNode);
  }
  
  private OIdentifiable[] mergeTail(int extraSlots) {
    OIdentifiable[] ret = new OIdentifiable[tailSize + extraSlots];
    int i = 0;
    for (ORidbagNode ridbagNode : ridbagNodes){
      if (!ridbagNode.isLoaded()){
        ridbagNode.load();
      }
      if (ridbagNode.isTailNode()){
        ret[i++] = ridbagNode.getAt(0);
      }
    }
    
    return ret;
  }
  
  private void relaxTail(boolean hardRelax){
    Iterator<ORidbagNode> iter = ridbagNodes.iterator();
    while (iter.hasNext()){
      ORidbagNode node = iter.next();
      //no need to check if node is loaded because it is loaded in mergeTail
      if (node.isTailNode()){        
        iter.remove();
        if (hardRelax){
          //TODO mark in cluster that it is removed
        }
        else{
          node.reset();
          freeNodes.add(node);
        }
      }
    }
    tailSize = 0;
  }

  @Override
  public void remove(OIdentifiable value) {
    boolean removed = false;    
    if (indexedRidsNodes == null){
      ORidbagNode node = indexedRidsNodes.get(value);
      if (node != null){
        if (!node.isLoaded()){
          node.load();
        }
        boolean isTail = node.isTailNode();
        if (node.remove(value)){
          if (activeNode == null){
            activeNode = node;
          }
          else{
            freeNodes.add(node);
          }
          if (isTail){
            --tailSize;
          }
          removed = true;
        }
      }
    }
    else{
      //go through all
      for (ORidbagNode ridbagNode : ridbagNodes){
        if (!ridbagNode.isLoaded()){
          ridbagNode.load();
        }
        boolean isTail = ridbagNode.isTailNode();
        if (ridbagNode.remove(value)){
          if (activeNode == null){
            activeNode = ridbagNode;
          }
          else{
            freeNodes.add(ridbagNode);
          }
          if (isTail){
            --tailSize;
          }
          removed = true;
        }
      }
    }
    if (removed){
      --size;
    }    
  }

  @Override
  public boolean isEmpty() {
    return size == 0;
  }

  @Override
  public int getSerializedSize() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
    
    //serialize active node
    if (activeNode != null){
      HelperClasses.writeLinkOptimized(container, activeNode.getRid());
    }
    else{
      OVarIntSerializer.write(container, -1l);
    }
    
    //serailize free nodes queue size
    OVarIntSerializer.write(container, freeNodes.size());
    
    //serialize free nodes queue
    for (ORidbagNode node : freeNodes){      
      HelperClasses.writeLinkOptimized(container, node.getRid());
    }
    
    //serialize size of associated ridbag nodes
    OVarIntSerializer.write(container, ridbagNodes.size());
    
    //serialize nodes associated with this ridbag    
    for (ORidbagNode node : ridbagNodes){      
      HelperClasses.writeLinkOptimized(container, node.getRid());
    }        
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
    
    //deserialize activeNode
    int currentOffset = container.offset;
    int checkValue = OVarIntSerializer.readAsInteger(container);
    OIdentifiable activeNodeRid = null;
    if (checkValue == -1){
      activeNodeRid = null;
    }
    else{
      container.offset = currentOffset;
      activeNodeRid = HelperClasses.readOptimizedLink(container, false);
    }
    
    //deserialize free nodes queue size
    int nodesSize = OVarIntSerializer.readAsInteger(container);
    
    //deserialize free nodes queue rids
    Set<OIdentifiable> freeNodesRids = new HashSet<>();
    for (int i = 0; i < nodesSize; i++){
      OIdentifiable nodeRid = HelperClasses.readOptimizedLink(container, false);
      freeNodesRids.add(nodeRid);      
    }
    
    //deserialize associated nodes size
    nodesSize = OVarIntSerializer.readAsInteger(container);
    for (int i = 0; i < nodesSize; i++){
      OIdentifiable nodeRid = HelperClasses.readOptimizedLink(container, false);
      ORidbagNode node = new ORidbagNode(nodeRid, false);
      //setup active node
      if (nodeRid.equals(activeNodeRid)){
        activeNode = node;
      }
      //if it is free node add it to free nodes queue
      if (freeNodesRids.contains(nodeRid)){
        freeNodes.add(node);
      }
      ridbagNodes.add(node);
    }
    
    return container.offset;
  }

  @Override
  public void requestDelete() {   
  }

  @Override
  public boolean contains(OIdentifiable value) {    
    if (indexedRidsNodes == null){
      ORidbagNode node = indexedRidsNodes.get(value);
      if (node != null){
        if (!node.isLoaded()){
          node.load();
        }
        return node.contains(value);
      }
    }
    else{    
      //go through all
      for (ORidbagNode ridbagNode : ridbagNodes){
        if (ridbagNode.isLoaded()){
          ridbagNode.load();
        }
        if (ridbagNode.contains(value)){
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public void setOwner(ORecord owner) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public ORecord getOwner() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public List<OMultiValueChangeListener<OIdentifiable, OIdentifiable>> getChangeListeners() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public NavigableMap<OIdentifiable, Change> getChanges() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void setSize(int size) {
    this.size = size;
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
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public boolean convertRecords2Links() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public boolean isAutoConvertToRecord() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void setAutoConvertToRecord(boolean convertToRecord) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public boolean detach() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public void addChangeListener(OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void removeRecordChangeListener(OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Object returnOriginalState(List<OMultiValueChangeEvent<OIdentifiable, OIdentifiable>> changeEvents) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void fireCollectionChangedEvent(OMultiValueChangeEvent<OIdentifiable, OIdentifiable> event) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Class<?> getGenericClass() {
    return OIdentifiable.class;
  }

  @Override
  public void replace(OMultiValueChangeEvent<Object, Object> event, Object newValue) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  public OPaginatedCluster getCluster() {
    return cluster;
  }

  public void setCluster(OPaginatedCluster cluster) {
    this.cluster = cluster;
  }
  
  protected ORidbagNode getAtIndex(int index){
    if (index < 0 || index >= ridbagNodes.size()){
      return null;
    }
    return ridbagNodes.get(index);
  }
  
  /**
   * search for free node of specific size, and create new one is none can recycle
   * @param numberOfRids
   * @return 
   */
  private ORidbagNode getOrCreateNodeOfSpecificSize(int numberOfRids){
    Iterator<ORidbagNode> iter = freeNodes.iterator();
    ORidbagNode ret = null;
    while (iter.hasNext() && ret == null){
      ORidbagNode freeNode  = iter.next();
      if (freeNode.isLoaded()){
        freeNode.load();
      }
      if (freeNode.getFreeSpace() >= numberOfRids){
        ret = freeNode;
        iter.remove();
      }
    }
    
    if (ret == null){
      OIdentifiable newNodeRid = allocateNodeInCluster(numberOfRids);
      ret = new ORidbagNode(newNodeRid, numberOfRids);
    }
    
    return ret;
  }
  
  private OIdentifiable allocateNodeInCluster(int numberOfRids){
    
  }
  
  private boolean ifOneMoreFitsToPage(){
    
  }
    
}
