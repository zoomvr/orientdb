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

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
  
  private final Map<ORecordId, Long> mappedHighestsequnceNumbers = new HashMap<>();
  private final Map<OLogSequenceNumber, Long> highestSequenceNumberForLSN = new HashMap<>();
  private final Map<Long, OLogSequenceNumber> LSNForHighestSequenceNumber = new HashMap<>();
  
  private Long highestSequnceNumberCanBeFlushed = null;
  private Long highestFlushedSequenceNumber = null;
  
  public void track(ORecordId rec, long sequenceNumber){
    if (rec == null){
      return;
    }
    System.out.println("----------------------------SEQUNCE NUMBER: " + sequenceNumber);
    //TODO need better synchronization
    synchronized(this){
      Long val = mappedHighestsequnceNumbers.get(rec);
      if (val == null || val < sequenceNumber){
        mappedHighestsequnceNumbers.put(rec, sequenceNumber);
      }
    }
  }
  
  public long getLargestsequenceNumber(List<ORecordId> observedIds){
    long retVal = -1l;
    synchronized(this){
      for (ORecordId rec : observedIds){
        Long val = mappedHighestsequnceNumbers.get(rec);
        if (val != null && val > retVal){
          retVal = val;          
        }
        else if (val != null){
          mappedHighestsequnceNumbers.remove(rec);
        }
      }
    }
    return retVal;
  }
  
  public void mapLSNToHighestSequenceNumber(OLogSequenceNumber lsn, Long sequenceNumber){
    if (lsn == null || sequenceNumber == null){
      return;
    }
    synchronized(this){
      highestSequenceNumberForLSN.put(lsn, sequenceNumber);
      LSNForHighestSequenceNumber.put(sequenceNumber, lsn);
    }
  }
  
  public Long getNearestSmallerOrEqualSequenceNumber(Long referentVal){
    synchronized(this){
      Long[] tmpListForSort = LSNForHighestSequenceNumber.keySet().toArray(new Long[0]);
      Arrays.sort(tmpListForSort);
      //find last smaller then specified
      for (int i = tmpListForSort.length - 1; i >= 0; i--){
        if (tmpListForSort[i] == null){
          System.out.println("Index is: " + i);
        }
        if (tmpListForSort[i] <= referentVal){
          return tmpListForSort[i];
        }
      }
    }
    
    return null;
  }
  
  public OLogSequenceNumber getNearestSmallerOrEqualLSN(OLogSequenceNumber toLsn){
    synchronized(this){
      OLogSequenceNumber[] tmpListForSort = highestSequenceNumberForLSN.keySet().toArray(new OLogSequenceNumber[0]);
      Arrays.sort(tmpListForSort);
      //find last smaller then specified
      for (int i = tmpListForSort.length - 1; i >= 0; i--){
        if (tmpListForSort[i].compareTo(toLsn) <= 0){
          return tmpListForSort[i];
        }
      }
    }
    
    return null;
  }
  
  public Long getMappedSequenceNumber(OLogSequenceNumber lsn){
    return highestSequenceNumberForLSN.get(lsn);
  }
  
  public OLogSequenceNumber getMappedLSN(Long sequenceNumber){
    return LSNForHighestSequenceNumber.get(sequenceNumber);
  }
  
  public void setHighestSequnceNumberCanBeFlushed(Long value){
    highestSequnceNumberCanBeFlushed = value;
  }
  
  public void setHighestFlushedSequenceNumber(Long value){
    highestFlushedSequenceNumber = value;
  }

  public Long getHighestSequnceNumberCanBeFlushed() {
    return highestSequnceNumberCanBeFlushed;
  }

  public Long getHighestFlushedSequenceNumber() {
    return highestFlushedSequenceNumber;
  }      
     
}
