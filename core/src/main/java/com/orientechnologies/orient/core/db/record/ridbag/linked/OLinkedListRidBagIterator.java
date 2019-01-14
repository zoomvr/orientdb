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
import com.orientechnologies.orient.core.id.ORID;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author marko
 */
public class OLinkedListRidBagIterator implements Iterator<OIdentifiable>{      
  
  private final OLinkedListRidBag ridbag;      
  private ORID prevDetectedValue = null;
  private boolean calledNextAfterHasNext = true;  
  private int lastDetectedIndex = -1;
  
  public OLinkedListRidBagIterator(OLinkedListRidBag ridbag){
    this.ridbag = ridbag;    
  }
  
  /**
   * caller should take care of synchronization
   * @param index
   * @param firstNode
   * @return 
   */
  private ORID getElementAt(int index, Long iteratingNode, List<OIdentifiable> pendingElements) throws IOException{
    int size = ridbag.size();
    if (index >= size){
      return null;
    }
    Object adequateContainer = null;
    size = 0;
    int indexDiff = 0;
    while (adequateContainer == null){
      if (iteratingNode == null){
        int tmpSize = pendingElements.size();        
        if (size + tmpSize <= index){
          return null;
        }
        adequateContainer = pendingElements;        
      }
      else{
        int tmpSz = ridbag.getCluster().getNodeSize(iteratingNode, true);        
        if (size + tmpSz > index){
          adequateContainer = iteratingNode;          
        }
        else{
          iteratingNode = ridbag.getCluster().getNextNode(iteratingNode, true);
          size += tmpSz;
        }
      }
      if (adequateContainer != null){
        indexDiff = index - size;
      }
    }
    
    if (adequateContainer instanceof Long){
      long nodePosition = (Long)adequateContainer;
      ORID[] nodeRids = ridbag.getCluster().getAllRidsFromNode(nodePosition, true);
      return nodeRids[indexDiff];
    }
    else if (adequateContainer instanceof List){
      return ((List<OIdentifiable>)adequateContainer).get(indexDiff).getIdentity();
    }
    else{
      throw new ODatabaseException("Illegal state of iterator");
    }
  }
  
  @Override
  public boolean hasNext() {
    if (!calledNextAfterHasNext){
      if (prevDetectedValue == null){
        return false;
      }
      else{
        return true;
      }
    }
    
    boolean detectedRetrnVal;
    synchronized(OLinkedListRidBag.getLockObject(ridbag.getUUID())){
      try{
        ++lastDetectedIndex;
        OLinkedListRidBag.CurrentPosSizeStoredSize info = ridbag.getCurrentMetadataState();
        long firstNodePosition = info.getFirstNodeClusterPosition();
        List<OIdentifiable> pendingElements = ridbag.getPendingRids();
        prevDetectedValue = getElementAt(lastDetectedIndex, firstNodePosition, pendingElements);
        if (prevDetectedValue == null){
          detectedRetrnVal = false;
        }
        else{
          detectedRetrnVal = true;
        }
      }
      catch (IOException exc){
        OLogManager.instance().errorStorage(this, exc.getMessage(), exc);
        throw new ODatabaseException("Illegal state of iterator");
      }
    }
        
    calledNextAfterHasNext = false;
    
    return detectedRetrnVal;
  }

  @Override
  public OIdentifiable next() {
    calledNextAfterHasNext = true;
    
    return prevDetectedValue;
    
    
  }
  
  @Override
  public void remove(){
    throw new UnsupportedOperationException("Not implemented");
  }
  
}
