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
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OVarIntSerializer;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.cluster.linkedridbags.OFastRidBagPaginatedCluster;
import java.io.IOException;
import java.util.Objects;

/**
 *
 * @author marko
 */
abstract class ORidbagNode{    
    
  public ORidbagNode(long rid, boolean considerLoaded){
    clusterPosition = rid;
    loaded = considerLoaded;
  }
  
  private final long clusterPosition;
  int pageIndex; 
  private int version;
  private boolean loaded = false;    
  int currentIndex = 0;
  static byte RECORD_TYPE = 'l';
  boolean stored = false;

  protected abstract int capacity();
  protected abstract void addInternal(OIdentifiable value);
  protected abstract void addAllInternal(OIdentifiable[] values);
  protected abstract OIdentifiable getAt(int index);
  protected abstract boolean remove(OIdentifiable value);
  protected abstract boolean contains(OIdentifiable value);
//  protected abstract void loadInternal();
  protected abstract boolean isTailNode();
  protected abstract OIdentifiable[] getAllRids();
  protected abstract byte getNodeType();
  protected abstract byte[] serializeInternal();
  /**
   * for internal use, caller have to take care of index bounds
   * @param value
   * @param index 
   */
  protected abstract void setAt(OIdentifiable value, int index);
  protected abstract void addInDeserializeInternal(OIdentifiable value, int index);
  
  protected int currentIndex(){
    return currentIndex;
  }    
  
  protected boolean add(OIdentifiable value){
    if (currentIndex() < capacity()){
      addInternal(value);
      currentIndex++;
      stored = false;
      return true;
    }
    return false;
  }

  protected boolean addAll(OIdentifiable[] values){
    if (currentIndex + values.length <= capacity()){
      addAllInternal(values);              
      currentIndex += values.length;
      stored = false;
      return true;
    }
    return false;
  }  

  protected long getClusterPosition(){
    return clusterPosition;
  }

  protected void load(OFastRidBagPaginatedCluster cluster) throws IOException{
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
    BytesContainer conatiner = new BytesContainer(content);
    long size = OVarIntSerializer.readAsLong(conatiner);
    for (int i = 0; i < size; i++){
      OIdentifiable value = HelperClasses.readOptimizedLink(conatiner, false);
      addInDeserializeInternal(value, i);
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

};
