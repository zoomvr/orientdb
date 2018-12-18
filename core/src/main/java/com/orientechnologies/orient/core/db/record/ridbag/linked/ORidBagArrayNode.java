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
  
  protected ORidBagArrayNode(OIdentifiable rid, boolean initContainer) {
    super(rid);
    if (initContainer){
      rids = new OIdentifiable[1];
    }
  }
  
  protected ORidBagArrayNode(OIdentifiable rid, int initialCapacity){
    super(rid);
    rids = new OIdentifiable[initialCapacity];
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
    return false;
//    return capacity() == 1 && currentIndex == 1;
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
  protected void loadInternal() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }
}
