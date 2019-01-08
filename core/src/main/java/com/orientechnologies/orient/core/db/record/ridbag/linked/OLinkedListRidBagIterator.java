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

/**
 *
 * @author marko
 */
public class OLinkedListRidBagIterator implements Iterator<OIdentifiable>{
  
  private static final int nodeStartIteratingIndex = -1;
  
  private final OLinkedListRidBag ridbag;
  private Long currentIterRidbagNode;
  private ORID[] currentNodeContent;
  private int currentNodeIteartionIndex = nodeStartIteratingIndex;
  
  private boolean noMore = false;
  
  private boolean calledNextAfterHasNext = true;
  
  public OLinkedListRidBagIterator(OLinkedListRidBag ridbag){
    this.ridbag = ridbag;
    currentIterRidbagNode = ridbag.getFirstNodeClusterPos();
  }
  
  @Override
  public boolean hasNext() {
    try{
      if (noMore){
        return false;
      }

      if (!calledNextAfterHasNext){
        return true;
      }
      
      try{
        if (currentNodeContent == null || currentNodeIteartionIndex == currentNodeContent.length - 1){
          if (currentNodeContent != null){
            currentIterRidbagNode = ridbag.getCluster().getNextNode(currentIterRidbagNode);
            if (currentIterRidbagNode == null){
              noMore = true;
              return false;
            }
          }
          currentNodeContent = ridbag.getCluster().getAllRidsFromNode(currentIterRidbagNode);
          currentNodeIteartionIndex = nodeStartIteratingIndex;
        }
      }
      catch (IOException exc){
        OLogManager.instance().errorStorage(this, exc.getMessage(), exc);
        throw new ODatabaseException(exc.getMessage());
      }

      ++currentNodeIteartionIndex;
      //this should never happen, bu just in case
      if (currentNodeIteartionIndex >= currentNodeContent.length){
        noMore = true;
        return false;
      }

      return true;
    }
    finally{
      calledNextAfterHasNext = false;
    }
  }

  @Override
  public OIdentifiable next() {
    try{
      if (noMore){
        throw new IllegalStateException("No more elements");
      }

      return currentNodeContent[currentNodeIteartionIndex];
    }
    finally{
      calledNextAfterHasNext = true;
    }
  }
  
  @Override
  public void remove(){
    throw new UnsupportedOperationException("Not implemented");
  }
  
}
