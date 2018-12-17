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
 * @author marko
 */
abstract class ORidbagNode{    
    
  public ORidbagNode(OIdentifiable rid){
    ridBagNodeRid = rid;
  }
  
  private final OIdentifiable ridBagNodeRid;  
  
  private boolean loaded = false;    
  int currentIndex = 0;

  protected abstract int capacity();
  protected abstract void addInternal(OIdentifiable value);
  protected abstract void addAllInternal(OIdentifiable[] values);
  protected abstract OIdentifiable getAt(int index);
  protected abstract boolean remove(OIdentifiable value);
  protected abstract boolean contains(OIdentifiable value);
  protected abstract void loadInternal();
  protected abstract boolean isTailNode();
  protected abstract OIdentifiable[] getAllRids();
  protected abstract byte getNodeType();
  /**
   * for internal use, caller have to take care of index bounds
   * @param value
   * @param index 
   */
  protected abstract void setAt(OIdentifiable value, int index);
  
  protected int currentIndex(){
    return currentIndex;
  }    
  
  protected boolean add(OIdentifiable value){
    if (currentIndex() < capacity()){
      addInternal(value);
      return true;
    }
    return false;
  }

  protected boolean addAll(OIdentifiable[] values){
    if (currentIndex + values.length <= capacity()){
      addAllInternal(values);              
      currentIndex += values.length;
      return true;
    }
    return false;
  }  

  protected OIdentifiable getRid(){
    return ridBagNodeRid;
  }

  protected void load(){      
    if (!loaded){
      loadInternal();
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
  }  

  protected int getFreeSpace(){
    return capacity() - currentIndex;
  }   

};
