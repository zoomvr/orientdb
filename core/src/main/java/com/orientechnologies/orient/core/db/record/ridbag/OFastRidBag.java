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
package com.orientechnologies.orient.core.db.record.ridbag;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeListener;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OFastRidBagPaginatedCluster;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.Change;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.UUID;

/**
 *
 * @author marko
 */
public class OFastRidBag implements ORidBagDelegate{    
  
  public static class ORidBagNode{
    public ORidBagNode(OIdentifiable rid){
      ridBagNodeRid = rid;
      rids = new OIdentifiable[1];
    }
    
    public ORidBagNode(OIdentifiable rid, int initialCapacity){
      ridBagNodeRid = rid;
      rids = new OIdentifiable[initialCapacity];
    }
    
    private final OIdentifiable ridBagNodeRid;
    private final OIdentifiable[] rids;
    private int currentIndex = 0;
    private boolean loaded = false;
    
    public int capacity(){
      return rids.length;
    }
    
    public int currentIndex(){
      return currentIndex;
    }
    
    public boolean add(OIdentifiable value){
      if (currentIndex < rids.length){
        rids[currentIndex++] = value;
        return true;
      }
      return false;
    }
    
    public boolean addAll(OIdentifiable[] values){
      if (currentIndex + values.length <= rids.length){
        for (int i = 0; i < values.length; i++){
          rids[currentIndex + i] = values[i];
        }
        currentIndex += values.length;
        return true;
      }
      return false;
    }
    
    public OIdentifiable getAt(int index){
      return rids[index];
    }
    
    public boolean isTailNode(){
      return capacity() == 1 && currentIndex == 1;
    }
    
    public boolean remove(OIdentifiable value){      
      for (int i = 0; i < rids.length; i++){
        OIdentifiable val = rids[i];
        if (val.equals(value)){
          //found so remove it
          //first shift all
          for (int j = i + 1; j < rids.length; j++){
            rids[j - 1] = rids[j];
          }
          --currentIndex;
          return true;
        }
      }
      
      return false;
    }
    
    public boolean contains(OIdentifiable value){      
      for (int i = 0; i < rids.length; i++){
        OIdentifiable val = rids[i];
        if (val.equals(value)){
          return true;
        }
      }
      
      return false;
    }
  };   
  
  private OIdentifiable ridbagRid;  
  private Map<OIdentifiable, ORidBagNode> indexedRidsNodes;
  private OFastRidBagPaginatedCluster cluster;
  private int size = 0;
  private List<ORidBagNode> ridbagNodes = new LinkedList<>();
  //tail is composed of nodes of size 1
  private int tailSize = 0;
  //this is internal ridbag list of free nodes already allocated. Differs from cluster free space
  private Queue<ORidBagNode> freeNodes = new LinkedList<>();
  
  private static final int MAX_RIDBAG_NODE_SIZE = 600;
  private static final int ADDITIONAL_ALLOCATION_SIZE = 20;  
  
  //if some node is made by merging, until its capacity is fullfillled it should be active node
  private ORidBagNode activeNode = null;
  
  public OFastRidBag(){    
  }
  
  @Override
  public void addAll(Collection<OIdentifiable> values) {    
    for (OIdentifiable rid : values){
      add(rid);
    }
  }

  @Override
  public void add(OIdentifiable value) {    
    if (activeNode != null){
      if (activeNode.currentIndex() >= activeNode.capacity()){
        activeNode = freeNodes.poll();
      }
      else{
        activeNode.add(value);
        if (activeNode.isTailNode()){
          tailSize++;
        }
      }
    }
    
    if (activeNode == null){
      //handle add to tail
      OIdentifiable ridBagNodeRid = cluster.addItem(ridbagRid, value);
      if (ridBagNodeRid == null){                
        int allocateSize = Math.min(tailSize * 2, tailSize + ADDITIONAL_ALLOCATION_SIZE);
        allocateSize = Math.min(allocateSize, 600);
        
        int extraSlots = 1;
        if (allocateSize == MAX_RIDBAG_NODE_SIZE){
          extraSlots = 0;
        }
        //created merged node
        ridBagNodeRid = cluster.preAllocateRidBagNode(allocateSize + extraSlots);
        
        OIdentifiable[] mergedTail = mergeTail(extraSlots);
        if (extraSlots == 1){
          //here we are dealing with node size less than max node size
          mergedTail[mergedTail.length - 1] = value;
          activeNode = new ORidBagNode(ridBagNodeRid, allocateSize);
          activeNode.addAll(mergedTail);
          ridbagNodes.add(activeNode);
          relaxTail();
        }
        else{
          //here we deal with node which size is equal to max node size
          ORidBagNode mergedMaxNode = new ORidBagNode(ridBagNodeRid, MAX_RIDBAG_NODE_SIZE);
          mergedMaxNode.addAll(mergedTail);
          ridbagNodes.add(mergedMaxNode);
          relaxTail();
          //add new rid
          OIdentifiable newNodeRid = cluster.addItem(ridbagRid, value);
          ORidBagNode newNode = new ORidBagNode(newNodeRid);
          ridbagNodes.add(newNode);
          ++tailSize;
        }                
      }
      else{
        ORidBagNode currentNode = new ORidBagNode(ridBagNodeRid);
        currentNode.add(value);
        ridbagNodes.add(currentNode);
        ++tailSize;
      }
    }    
    
    ++size;
    //TODO add it to index
  }
  
  private OIdentifiable[] mergeTail(int extraSlots) {
    OIdentifiable[] ret = new OIdentifiable[tailSize + extraSlots];
    int i = 0;
    for (ORidBagNode ridbagNode : ridbagNodes){
      if (ridbagNode.isTailNode()){
        ret[i++] = ridbagNode.getAt(0);
      }
    }
    
    return ret;
  }
  
  private void relaxTail(){
    Iterator<ORidBagNode> iter = ridbagNodes.iterator();
    while (iter.hasNext()){
      ORidBagNode node = iter.next();
      if (node.isTailNode()){
        //TODO mark in cluster that it is removed
        iter.remove();
      }
    }
    tailSize = 0;
  }

  @Override
  public void remove(OIdentifiable value) {
    boolean removed = false;    
    if (indexedRidsNodes == null){
      ORidBagNode node = indexedRidsNodes.get(value);
      if (node != null){        
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
//    if (!removed && !found){
      //go through all
      for (ORidBagNode ridbagNode : ridbagNodes){
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
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public int serialize(byte[] stream, int offset, UUID ownerUuid) {
    
  }

  @Override
  public int deserialize(byte[] stream, int offset) {
    
  }

  @Override
  public void requestDelete() {   
  }

  @Override
  public boolean contains(OIdentifiable value) {    
    if (indexedRidsNodes == null){
      ORidBagNode node = indexedRidsNodes.get(value);
      if (node != null){             
        return node.contains(value);
      }
    }
    else{    
      //go through all
      for (ORidBagNode ridbagNode : ridbagNodes){
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
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Iterator<OIdentifiable> rawIterator() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
  
  
}
