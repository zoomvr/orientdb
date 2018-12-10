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
class ORidbagNode{
    public ORidbagNode(OIdentifiable rid, boolean initContainer){
      ridBagNodeRid = rid;
      if (initContainer){
        rids = new OIdentifiable[1];
      }
    }
    
    public ORidbagNode(OIdentifiable rid, int initialCapacity){
      ridBagNodeRid = rid;
      rids = new OIdentifiable[initialCapacity];
    }
    
    private final OIdentifiable ridBagNodeRid;
    private OIdentifiable[] rids;
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
        System.arraycopy(values, 0, rids, currentIndex, values.length);        
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
    
    public OIdentifiable getRid(){
      return ridBagNodeRid;
    }
    
    public void load(){      
      if (!loaded){
        throw new UnsupportedOperationException("Not implemented");
      }
      loaded = true;
    }

    public boolean isLoaded() {
      return loaded;
    }
    
    public boolean isMaxSizeNodeFullNode(){
      return rids.length == OLinkedListRidBag.MAX_RIDBAG_NODE_SIZE && currentIndex == OLinkedListRidBag.MAX_RIDBAG_NODE_SIZE;
    }
    
    public void reset(){
      currentIndex = 0;
    }
    
    protected OIdentifiable[] getAllRids(){
      return rids;
    }
    
    protected int getFreeSpace(){
      return rids.length - currentIndex;
    }
        
  };
