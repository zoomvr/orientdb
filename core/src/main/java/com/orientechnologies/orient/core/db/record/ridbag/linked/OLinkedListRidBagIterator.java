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
import java.util.Iterator;

/**
 *
 * @author marko
 */
public class OLinkedListRidBagIterator implements Iterator<OIdentifiable>{

  private static final int iteratingIndexInitialvalue = -1;
  
  private final OLinkedListRidBag ridbag;
  private int currentNodeIndex = -1;
  private ORidbagNode currentNode = null;
  private int currentNodeIteratingIndex = iteratingIndexInitialvalue;
  private boolean calledNext = true;
  
  public OLinkedListRidBagIterator(OLinkedListRidBag ridbag){
    this.ridbag = ridbag;
  }
  
  @Override
  public boolean hasNext() {
    try{
      if (currentNode == null){
        getNextNode();
      }
      if (currentNode == null){
        return false;
      }
      boolean found = false;
      while (!found){
        if (!calledNext){
          --currentNodeIteratingIndex;
        }
        if (currentNode.currentIndex() > 0 && currentNodeIteratingIndex < currentNode.capacity() - 1){
          ++currentNodeIteratingIndex;
          found = true;
          break;
        }
        //go for next node
        getNextNode();
        if (currentNode == null){
          break;
        }
      }
      return found;
    }
    finally{
      calledNext = false;
    }
  }

  @Override
  public OIdentifiable next() {
    calledNext = true;
    return currentNode.getAt(currentNodeIteratingIndex);
  }
  
  private void getNextNode(){
    currentNode = ridbag.getAtIndex(++currentNodeIndex);
    currentNodeIteratingIndex = iteratingIndexInitialvalue;
    if (currentNode != null && !currentNode.isLoaded()){
      currentNode.load();
    }
  }
  
  @Override
  public void remove(){
    
  }
  
}
