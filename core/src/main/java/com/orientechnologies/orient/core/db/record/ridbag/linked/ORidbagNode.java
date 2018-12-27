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

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.cluster.linkedridbags.OFastRidBagPaginatedCluster;
import java.io.IOException;
import java.util.Objects;

/**
 *
 * @author marko
 */
public class ORidbagNode{
  
  protected final long clusterPosition;  
  private int version;
  private boolean loaded = false;    
  private boolean loadedMetadata = false;
  int currentIndex = 0;
  public static byte RECORD_TYPE_LINKED_NODE = 'l';
  public static byte RECORD_TYPE_ARRAY_NODE = 'a';
  boolean stored = false;
  //reference to next node, as in linked list
  Long nextNode;
  //reference to previous node, as in double linked list
  Long previousNode;
  final OFastRidBagPaginatedCluster cluster;
  private OIdentifiable[] rids;                     
      
  
  protected ORidbagNode(long physicalPosition, boolean considerLoaded, OFastRidBagPaginatedCluster cluster) {
    clusterPosition = physicalPosition;
    loaded = considerLoaded;
    loadedMetadata = considerLoaded;
    this.cluster = cluster;    
  }
  
  //will also pre allocate size for rids in cluster
  protected ORidbagNode(long physicalPosition, int initialCapacity, boolean considerLoaded, OFastRidBagPaginatedCluster cluster){
    this(physicalPosition, considerLoaded, cluster);
    rids = new OIdentifiable[initialCapacity];
    for (int i = 0; i < initialCapacity; i++){
      rids[i] = new ORecordId(-1, -1);
    }
  }
  
  protected int currentIndex(){
    return currentIndex;
  }
  
  protected int capacity(){
    return rids.length;
  }
  
  protected OIdentifiable getAt(int index){
    return rids[index];
  }
  
  protected boolean add(OIdentifiable value){
    if (currentIndex() < capacity()){
      rids[currentIndex] = value;
      currentIndex++;
      stored = false;
      return true;
    }
    return false;
  }
  
  protected boolean remove(OIdentifiable value){      
    for (int i = 0; i < rids.length; i++){
      OIdentifiable val = rids[i];
      if (val.equals(value)){
        //found so remove it
        //first shift all
        for (int j = i + 1; j < rids.length; j++){
          rids[j - 1] = rids[j];
        }
        --currentIndex;
        stored = false;
        return true;
      }
    }

    return false;
  }
  
  protected boolean contains(OIdentifiable value){
    for (int i = 0; i < rids.length; i++){
      OIdentifiable val = rids[i];
      if (val.equals(value)){
        return true;
      }
    }

    return false;
  }
  
  protected boolean isTailNode(){    
    return capacity() == 1 && currentIndex == 1;
  }
  
  protected OIdentifiable[] getAllRids(){
    return rids;
  }

  protected boolean addAll(OIdentifiable[] values){
    if (currentIndex + values.length <= capacity()){
      System.arraycopy(values, 0, rids, currentIndex, values.length);              
      currentIndex += values.length;
      stored = false;
      return true;
    }
    return false;
  }  

  protected long getClusterPosition(){
    return clusterPosition;
  }

  protected void load() throws IOException{
    if (!loadedMetadata){
      loadMetadata();
    }
    if (!loaded){
      ORawBuffer buffer = cluster.readRecord(clusterPosition, false);
      byte[] stream = buffer.getBuffer();
      deserialize(stream);
    }
    loaded = true;
  }

  protected boolean isLoaded() {
    return loaded;
  }

  protected boolean isMaxSizeNodeFullNode(){
    return capacity() == OLinkedListRidBag.MAX_RIDBAG_NODE_SIZE && currentIndex == OLinkedListRidBag.MAX_RIDBAG_NODE_SIZE;
  }
  
  protected boolean isFullNode(){
    return capacity() == currentIndex();
  }

  protected void reset(){
    currentIndex = 0;
    stored = false;
  }  

  protected int getFreeSpace(){
    return capacity() - currentIndex;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 97 * hash + Objects.hashCode(this.clusterPosition);
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final ORidbagNode other = (ORidbagNode) obj;
    if (!Objects.equals(this.clusterPosition, other.clusterPosition)) {
      return false;
    }
    return true;
  }    
  
  int getVersion(){
    return version;
  }
  
  protected void deserialize(byte[] content){    
    int pos = 0;
    long nextNodeRef = OLongSerializer.INSTANCE.deserialize(content, pos);
    if (nextNodeRef != -1){
      nextNode = null;
    }
    else{      
      nextNode = nextNodeRef;
    }
    pos += OLongSerializer.LONG_SIZE;
    int size = OIntegerSerializer.INSTANCE.deserialize(content, pos);
    pos += OIntegerSerializer.INT_SIZE;    
    for (int i = 0; i < size; i++){
      ORecordId value = OLinkSerializer.INSTANCE.deserialize(content, pos);
      addInDeserializeInternal(value, i);
      pos += OLinkSerializer.RID_SIZE;
    }
  }
  
  protected boolean getStored(){
    return stored;
  }
  
  protected byte[] serialize(){
    byte[] ret = serializeInternal();
    stored = true;
    return ret;
  }
  
  protected static int getSerializedSize(int numberOfRids){
    return OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE + OLinkSerializer.RID_SIZE * numberOfRids;
  }

  public Long getNextNode() {
    return nextNode;
  }

  public void setNextNode(Long nextNode) {
    this.nextNode = nextNode;
    stored = false;
  }

  public Long getPreviousNode() {
    return previousNode;
  }

  public void setPreviousNode(Long previousNode) {
    this.previousNode = previousNode;
    stored = false;
  }
  
  /**
   * pre-allocates node in cluster
   * @throws IOException 
   */
  protected void initInCluster() throws IOException{
    byte[] bytes = serialize();
    OPhysicalPosition ppos = new OPhysicalPosition(clusterPosition);
    cluster.createRecord(bytes, 1, RECORD_TYPE_LINKED_NODE, ppos);
  }
  
  protected byte[] serializeInternal(){
    int size = getSerializedSize(rids.length);
    byte[] stream = new byte[size];
    int pos = 0;
    
    //serialize currentIndex
    OIntegerSerializer.INSTANCE.serialize(currentIndex, stream, pos);
    pos += OIntegerSerializer.INT_SIZE;
    
    //serialize reference to next node
    if (nextNode == null){
      OLongSerializer.INSTANCE.serialize(-1l, stream, pos);
    }
    else{
      OLongSerializer.INSTANCE.serialize(nextNode, stream, pos);
    }
    
    //serialize reference to previous node
    if (previousNode == null){
      OLongSerializer.INSTANCE.serialize(-1l, stream, pos);
    }
    else{
      OLongSerializer.INSTANCE.serialize(previousNode, stream, pos);
    }
    
    pos += OLongSerializer.LONG_SIZE;
    //serialize number of stored rids
    OIntegerSerializer.INSTANCE.serialize(rids.length, stream, pos);
    pos += OIntegerSerializer.INT_SIZE;
    //serialize rids
    for (OIdentifiable value : rids){
      OLinkSerializer.INSTANCE.serialize(value, stream, pos);
      pos += OLinkSerializer.RID_SIZE;
    }
    return stream;
  }
  
  /**
   * for internal use, caller have to take care of index bounds
   * @param value
   * @param index 
   */
  protected void setAt(OIdentifiable value, int index){
    rids[index] = value;
  }
  
  protected void addInDeserializeInternal(OIdentifiable value, int index){
    rids[index] = value;
  }
  
  protected void loadMetadata() throws IOException{    
    HelperClasses.Tuple<Long, Long> prevNextPosition = cluster.getNodePreviousNextNodePositons(clusterPosition);
    previousNode = prevNextPosition.getFirstVal();
    nextNode = prevNextPosition.getSecondVal();
  }
  
  protected boolean isLoadedMetdata(){
    return loadedMetadata;
  }
    
};
