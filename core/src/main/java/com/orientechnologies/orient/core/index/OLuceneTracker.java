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
package com.orientechnologies.orient.core.index;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author marko
 */
public class OLuceneTracker {
  
  private static OLuceneTracker instance = null;
  
  public static OLuceneTracker instance(){
    if (instance == null){
      synchronized(OLuceneTracker.class){
        if (instance == null){
          instance = new OLuceneTracker();                  
        }
      }
    }
    
    return instance;
  }
  
  private final Map<Long, Long> highestSequenceNumberCanBeFlushed = new HashMap<>();
  private final Map<Long, Long> highestSequenceNumberFlushed = new HashMap<>();
  private final Map<Long, Boolean> hasUnflushed = new HashMap<>();
  
  public synchronized Long getHighestSequenceNumberCanBeFlushed(long writerIndex){
    return highestSequenceNumberCanBeFlushed.get(writerIndex);
  }
  
  public synchronized long mapHighestSequenceNumberCanBeFLushed(long writerIndex, long sequenceNumber){
    Long currVal = highestSequenceNumberCanBeFlushed.get(writerIndex);
    if (currVal == null || currVal < sequenceNumber){
      highestSequenceNumberCanBeFlushed.put(writerIndex, sequenceNumber);
      return sequenceNumber;
    }
    else{
      return currVal;
    }
  }
  
  public synchronized long getHighestSequenceNumberFlushed(long writerIndex){
    return highestSequenceNumberFlushed.get(writerIndex);
  }
  
  public synchronized long mapHighestSequenceNumberFlushed(long writerIndex, long sequenceNumber){
    Long currVal = highestSequenceNumberFlushed.get(writerIndex);
    if (currVal == null || currVal < sequenceNumber){
      highestSequenceNumberFlushed.put(writerIndex, sequenceNumber);
      return sequenceNumber;
    }
    else{
      return currVal;
    }
  }
  
  public synchronized boolean hasUnflushed(long writerIndex){
    Boolean val = hasUnflushed.get(writerIndex);
    if (val == null){
      val = false;
    }
    return val;
  }
  
  public synchronized void clearHasUnflushed(long writerIndex){
    hasUnflushed.remove(writerIndex);
  }
}
