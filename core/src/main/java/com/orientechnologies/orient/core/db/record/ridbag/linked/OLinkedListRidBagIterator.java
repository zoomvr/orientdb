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
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.storage.cluster.linkedridbags.OFastRidBagPaginatedCluster;
import java.io.IOException;
import java.util.Iterator;

/**
 *
 * @author marko
 */
public class OLinkedListRidBagIterator implements Iterator<OIdentifiable>{

  private static final int iteratingIndexInitialvalue = -1;
  
  private final OLinkedListRidBag ridbag;  
  private ORidbagNode currentNode = null;
  private int currentNodeIteratingIndex = iteratingIndexInitialvalue;
  private boolean calledNext = true;
  private final OFastRidBagPaginatedCluster cluster;
  
  public OLinkedListRidBagIterator(OLinkedListRidBag ridbag, OFastRidBagPaginatedCluster cluster){
    this.ridbag = ridbag;
    this.cluster = cluster;
    currentNode = ridbag.getFirstNode();
  }
  
  @Override
  public boolean hasNext() {
    try{
      boolean found = false;
      while (!found){
        if (!calledNext){
          --currentNodeIteratingIndex;
        }
        if (currentNode.currentIndex() > 0 && currentNodeIteratingIndex < currentNode.currentIndex() - 1){
          ++currentNodeIteratingIndex;
          found = true;
          break;
        }
        //go for next node        
        try{
          getNextNode();
        }
        catch (IOException exc){
          OLogManager.instance().errorStorage(this, exc.getMessage(), exc, (Object[])null);          
          throw new ODatabaseException(exc.getMessage());
        }
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
  
  private void getNextNode() throws IOException{
    currentNode = ridbag.getNextNodeOfNode(currentNode);
    currentNodeIteratingIndex = iteratingIndexInitialvalue;
    if (currentNode != null && !currentNode.isLoaded()){
      currentNode.load();
    }
  }
  
  @Override
  public void remove(){
    
  }
  
}
