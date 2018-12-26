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
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.cluster.linkedridbags.OFastRidBagPaginatedCluster;
import java.io.IOException;

/**
 *
 * @author mdjurovi
 */
class ORidBagArrayNode extends ORidbagNode{
  
  protected static byte RIDBAG_ARRAY_NODE_TYPE = 'a';
  private OIdentifiable[] rids;   
  
  @Override
  protected byte getNodeType(){
    return RIDBAG_ARRAY_NODE_TYPE;
  }
  
  protected ORidBagArrayNode(long physicalPosition, boolean considerLoaded, OFastRidBagPaginatedCluster cluster) {
    super(physicalPosition, considerLoaded, cluster);    
  }
  
  //will also pre allocate size for rids in cluster
  protected ORidBagArrayNode(long physicalPosition, int initialCapacity, boolean considerLoaded, OFastRidBagPaginatedCluster cluster){
    super(physicalPosition, considerLoaded, cluster);
    rids = new OIdentifiable[initialCapacity];
    for (int i = 0; i < initialCapacity; i++){
      rids[i] = new ORecordId(-1, -1);
    }
  }
  
  @Override
  protected void initInCluster() throws IOException{
    byte[] bytes = serialize();
    OPhysicalPosition ppos = new OPhysicalPosition(clusterPosition);
    cluster.createRecord(bytes, 1, RECORD_TYPE, ppos);
  }
  
  @Override
  protected int capacity(){
    return rids.length;
  }    
    
  @Override
  protected void addInternal(OIdentifiable value){
    rids[currentIndex] = value;
  }
  
  @Override
  protected void addAllInternal(OIdentifiable[] values){
    System.arraycopy(values, 0, rids, currentIndex, values.length);
  }
  
  @Override
  protected OIdentifiable getAt(int index){
    return rids[index];
  }
  
  @Override
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
  
  @Override
  protected boolean contains(OIdentifiable value){
    for (int i = 0; i < rids.length; i++){
      OIdentifiable val = rids[i];
      if (val.equals(value)){
        return true;
      }
    }

    return false;
  }
  
  @Override
  protected boolean isTailNode(){    
    return capacity() == 1 && currentIndex == 1;
  }
  
  @Override
  protected OIdentifiable[] getAllRids(){
    return rids;
  }
  
  @Override
  protected void setAt(OIdentifiable value, int index){
    rids[index] = value;
  }
  
  @Override
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
  
  @Override
  protected void addInDeserializeInternal(OIdentifiable value, int index){
    rids[index] = value;
  }
}
