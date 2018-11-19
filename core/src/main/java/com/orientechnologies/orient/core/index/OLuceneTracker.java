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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 *
 * @author marko
 */
public class OLuceneTracker {

  private static OLuceneTracker instance = null;
  private final Map<Long, PerIndexWriterTRracker> mappedTrackers = new HashMap<>();
  private final Long[] locks = new Long[128];  
//  private final Map<OLogSequenceNumber, Collection<Long>> involvedWriterIdsInLSN = new HashMap<>();
  private final Map<OLogSequenceNumber, Collection<OIndexEngine>> involvedWritersInLSN = new HashMap<>();
  
  public static OLuceneTracker instance() {
    if (instance == null) {
      synchronized (OLuceneTracker.class) {
        if (instance == null) {
          instance = new OLuceneTracker();
        }
      }
    }

    return instance;
  }

  private Long getLockObject(Long writerIndex){
    return writerIndex % locks.length;
  }
  
  
  public synchronized void mapWritersToSpecificLSN(OLogSequenceNumber lsn, Collection<OIndexEngine> indexes){
    involvedWritersInLSN.put(lsn, indexes);
  }
  
  /**
   * inclusive
   */
  public synchronized Collection<OLogSequenceNumber> getPreviousLSNsTill(OLogSequenceNumber referent){
    List<OLogSequenceNumber> ret = new ArrayList<>();
    for (OLogSequenceNumber lsn : involvedWritersInLSN.keySet()){
      if (lsn.compareTo(referent) <= 0){
        ret.add(lsn);
      }
    }
    return ret;
  }
  
  public synchronized Collection<Long> getMappedIndexWritersIds(Collection<OLogSequenceNumber> referentLSNs){
    Set<Long> ret = new HashSet<>();
    for (OLogSequenceNumber lsn : referentLSNs){
      Collection<OIndexEngine> indexes = involvedWritersInLSN.get(lsn);
      List<Long> writerIds = indexes.stream().map(OIndexEngine::getBackEndWriterId).collect(Collectors.toList());
      if (writerIds != null){
        ret.addAll(writerIds);
      }
    }
    return ret;
  }
  
  public synchronized void flushMappedIndexes(Collection<OLogSequenceNumber> referentLSNs){
    Set<Long> ret = new HashSet<>();
    for (OLogSequenceNumber lsn : referentLSNs){
      Collection<OIndexEngine> indexes = involvedWritersInLSN.get(lsn);
      for (OIndexEngine index : indexes){
        try {
          if (index != null)
            index.flush();
        } catch (Throwable t) {
          OLogManager.instance().error(this, "Error while flushing index via index engine of class %s.", t,
              index.getClass().getSimpleName());
        }
      }
    }
  }
  
  public synchronized void clearInvolvedWriterIdsInLSN(OLogSequenceNumber referent){
    Collection<OLogSequenceNumber> allTill = getPreviousLSNsTill(referent);
    for (OLogSequenceNumber lsn : allTill){
      involvedWritersInLSN.remove(lsn);
    }
  }
  
  public void track(long sequenceNumber, Long writerIndex){
    synchronized(getLockObject(writerIndex)){
      System.out.println("----------------------------SEQUENCE NUMBER: " + sequenceNumber + ", WRITER INDEX: " + writerIndex);      
      PerIndexWriterTRracker tracker = mappedTrackers.get(writerIndex);
      if (tracker == null){
        tracker = new PerIndexWriterTRracker();
        mappedTrackers.put(writerIndex, tracker);
      }
      tracker.track(sequenceNumber);
    }
  }
  
  public long getLargestSequenceNumber(Long writerIndex){
    long retVal = -1;
    if (writerIndex == null){
      return retVal;
    }    
    synchronized(getLockObject(writerIndex)){
      PerIndexWriterTRracker tracker = mappedTrackers.get(writerIndex);
      if (tracker != null){
        Long val = tracker.getLargestSequenceNumber();
        if (val != null && val > retVal){
          System.out.println("GET LARGEST SEQUENCE NUMBER: " + val + " WriterID: " + writerIndex);
          retVal = val;
        }
      }      
    }
    return retVal;
  }
  
  public void mapLSNToHighestSequenceNumber(OLogSequenceNumber lsn, Long sequenceNumber, Long writerIndex){
    if (writerIndex == null){
      return;
    }    
    synchronized(getLockObject(writerIndex)){
      PerIndexWriterTRracker tracker = mappedTrackers.get(writerIndex);
      if (tracker != null){
        System.out.println("MAPPED LARGEST SEQUENCE NUMBER: " + sequenceNumber + " TO LSN: " + lsn + " WriterID: " + writerIndex);
        tracker.mapLSNToHighestSequenceNumber(lsn, sequenceNumber);
      }
    }    
  }
  
  public void clearMappedHighestSequenceNumbers(List<Long> writerIds, List<Long> highestSequenceNumbers){
    if (writerIds == null){
      return;
    }
    for (int i = 0; i < writerIds.size(); i++){
      Long writerId = writerIds.get(i);
      Long tillSequenceNumber = highestSequenceNumbers.get(i);
      if (tillSequenceNumber <= 0){
        continue;
      }
      synchronized(getLockObject(writerId)){
        PerIndexWriterTRracker tracker = mappedTrackers.get(writerId);
        if (tracker != null){
          tracker.clearMappedHighestSequenceNumbers(tillSequenceNumber);
        }
      }
    }
  }
  
  public synchronized Long getHighestSequnceNumberCanBeFlushed(Long indexWriterId){    
    if (indexWriterId == null){
      return null;
    }
    
    synchronized(getLockObject(indexWriterId)){
      PerIndexWriterTRracker tracker = mappedTrackers.get(indexWriterId);
      if (tracker != null){
        return tracker.getHighestSequnceNumberCanBeFlushed();
      }
    }
    
    return null;
  }
  
  public void setHighestSequnceNumberCanBeFlushed(Long value, Long indexWriterId) {
    if (indexWriterId == null){
      return;
    }
    
    synchronized(getLockObject(indexWriterId)){
      PerIndexWriterTRracker tracker = mappedTrackers.get(indexWriterId);
      if (tracker != null){
        tracker.setHighestSequnceNumberCanBeFlushed(value);
      }
    }
    
  }
  
  public void setHighestFlushedSequenceNumber(Long value, Long writerId) {
    synchronized(getLockObject(writerId)){
      PerIndexWriterTRracker tracker = mappedTrackers.get(writerId);
      if (tracker != null){
        tracker.setHighestFlushedSequenceNumber(value);
      }
    }
  }
  
//  public Long getNearestSmallerOrEqualSequenceNumber(Long referentVal){    
//    Long retVal = null;
//    for (Long writerId : mappedTrackers.keySet()){
//      synchronized(getLockObject(writerId)){
//        PerIndexWriterTRracker tracker = mappedTrackers.get(writerId);
//        Long tmpVal = tracker.getNearestSmallerOrEqualSequenceNumber(referentVal);
//        if ((tmpVal != null) && (retVal == null || tmpVal > retVal)){
//          retVal = tmpVal;
//        }
//      }
//    }
//    
//    return retVal;
//  }
  
  public OLogSequenceNumber getNearestSmallerOrEqualLSN(OLogSequenceNumber toLsn){
    OLogSequenceNumber retVal = null;
    for (Long writerId : mappedTrackers.keySet()){
      synchronized(getLockObject(writerId)){
        PerIndexWriterTRracker tracker = mappedTrackers.get(writerId);
        OLogSequenceNumber tmpVal = tracker.getNearestSmallerOrEqualLSN(toLsn);
        if ((tmpVal != null) && (retVal == null || tmpVal.compareTo(retVal) > 0)){
          retVal = tmpVal;
        }
      }
    }
    
    return retVal;  
  }
  
  public Long getHighestMappedSequenceNumber(Collection<OLogSequenceNumber> observedLSNs, Long indexWriterId){
    if (indexWriterId == null){
      return null;
    }   
    synchronized(getLockObject(indexWriterId)){
      PerIndexWriterTRracker tracker = mappedTrackers.get(indexWriterId);
      Long tmpVal = tracker.getHighestMappedSequenceNumber(observedLSNs);
      if (tmpVal != null){
        return tmpVal;
      }
    }    
    
    return null;
  }
    
//  public Long getHighestFlushedSequenceNumber(){
//    Long retVal = null;
//    for (Long writerId : mappedTrackers.keySet()){
//      synchronized(getLockObject(writerId)){
//        PerIndexWriterTRracker tracker = mappedTrackers.get(writerId);
//        Long tmpVal = tracker.getHighestFlushedSequenceNumber();
//        if ((tmpVal != null) && (retVal == null || tmpVal > retVal)){
//          retVal = tmpVal;
//        }
//      }
//    }
//    return retVal;
//  }
  
  public OLogSequenceNumber getMappedLSN(Long sequenceNumber) {
    OLogSequenceNumber retVal = null;
    for (Long writerId : mappedTrackers.keySet()){
      synchronized(getLockObject(writerId)){
        PerIndexWriterTRracker tracker = mappedTrackers.get(writerId);
        OLogSequenceNumber tmpVal = tracker.getMappedLSN(sequenceNumber);
        if (tmpVal != null){
          retVal = tmpVal;
          break;
        }
      }
    }
    return retVal;
  }
  
  public void resetHasUnflushedSequences(Long writerId){        
    synchronized(getLockObject(writerId)){
      PerIndexWriterTRracker tracker = mappedTrackers.get(writerId);
      tracker.resetHasUnflushedSequences();
    }    
  }
  
  public synchronized boolean hasUnflushedSequences(Collection<Long> writersIds) {
    for (Long writerId : writersIds){
      synchronized(getLockObject(writerId)){
        PerIndexWriterTRracker tracker = mappedTrackers.get(writerId);
        if (tracker != null && tracker.hasUnflushedSequences()){
          return true;
        }
      }
    }
    return false;
  }
  
  public OLogSequenceNumber getMinimalFlushedLSNForAllWriters(Collection<Long> writerIds, OLogSequenceNumber referentLSN){
    OLogSequenceNumber ret = null;
    //in fact here we want to use minimum of LSNs mappped to each writers' highest flushed sequence number
    for (Long writerId : writerIds){      
      synchronized(getLockObject(writerId)){
        PerIndexWriterTRracker tracker = mappedTrackers.get(writerId);
        Long highestFlushed = tracker.getHighestFlushedSequenceNumber();
        //if some writer still doesn't have flushed return null is minimal
        if (highestFlushed == null){
          System.out.println("NOT FOUND SAFE LSN FOR: " + writerId);
          return null;
        }
        Long mappedEquivalent = tracker.getNearestSmallerOrEqualSequenceNumber(highestFlushed);
        OLogSequenceNumber lsn = tracker.getMappedLSN(mappedEquivalent);        
        //this lsn must be smaller or equals than referent LSN
        if (lsn.compareTo(referentLSN) > 0){
          throw new ODatabaseException("Lucene index is desynchronized with database");
        }
        if (ret == null || lsn.compareTo(ret) < 0){
          ret = lsn;
        }
      }
    }
    
    return ret;
  }
  
  public void cleanUpTillLSN(Collection<Long> writerIds, OLogSequenceNumber toLSN){
    for (Long writerId : writerIds){      
      synchronized(getLockObject(writerId)){
        PerIndexWriterTRracker tracker = mappedTrackers.get(writerId);
        tracker.cleanUpTillLSN(toLSN);
      }
    }
  }
  
  private class PerIndexWriterTRracker {

    private AtomicLong highestMappedSequenceNumber;
    private final Map<OLogSequenceNumber, Long> highestSequenceNumberForLSN = new HashMap<>();
    private final Map<Long, OLogSequenceNumber> LSNForHighestSequenceNumber = new HashMap<>();
    
    private Long highestFlushedSequenceNumber = null;
    private boolean hasUnflushedSequences = false;
    private AtomicLong highestSequenceNumberCanBeFlushed = null;

    public void track(long sequenceNumber) {
      hasUnflushedSequences = true;
       
      if (highestMappedSequenceNumber == null || highestMappedSequenceNumber.get() < sequenceNumber) {
        if (highestMappedSequenceNumber == null){
          synchronized (this){
            if (highestMappedSequenceNumber == null){
              highestMappedSequenceNumber = new AtomicLong();
            }
          }
        }
        highestMappedSequenceNumber.set(sequenceNumber);
      }
      
    }    
    
    public Long getLargestSequenceNumber() {
      if (highestMappedSequenceNumber == null){
        synchronized (this){
          if (highestMappedSequenceNumber == null){
            return null;
          }
        }
      }
      return highestMappedSequenceNumber.get();
    }

    public void clearMappedHighestSequenceNumbers(Long tillSequenceNumber) {
      synchronized(this){
        if (highestMappedSequenceNumber != null && highestMappedSequenceNumber.get() <= tillSequenceNumber){
          highestMappedSequenceNumber = null;
        }
      }
    }

    public void mapLSNToHighestSequenceNumber(OLogSequenceNumber lsn, Long sequenceNumber) {
      if (lsn == null || sequenceNumber == null) {
        return;
      }
      synchronized (this) {
        highestSequenceNumberForLSN.put(lsn, sequenceNumber);
        LSNForHighestSequenceNumber.put(sequenceNumber, lsn);
      }
    }

    public Long getNearestSmallerOrEqualSequenceNumber(Long referentVal) {
      if (referentVal == null){
        return null;
      }
      Long retVal = null;
      synchronized (this) {        
        //find last smaller then specified
        for (Long sequenceNumber : LSNForHighestSequenceNumber.keySet()){
          if (sequenceNumber < referentVal){
            if (retVal == null || sequenceNumber > retVal){
              retVal = sequenceNumber;
            }
          }
        }      
      }

      return retVal;
    }

    public OLogSequenceNumber getNearestSmallerOrEqualLSN(OLogSequenceNumber toLsn) {
      if (toLsn == null){
        return null;
      }
      OLogSequenceNumber retVal = null;
      synchronized (this) {
        for (OLogSequenceNumber lsn : highestSequenceNumberForLSN.keySet()){
          if (lsn.compareTo(toLsn) <= 0){
            if (retVal == null || lsn.compareTo(retVal) > 0){
              retVal = lsn;
            }
          }
        }                
      }

      return retVal;
    }

    public Long getHighestMappedSequenceNumber(Collection<OLogSequenceNumber> observedLSNs) {
      Long ret = null;
      synchronized(this){
        for (OLogSequenceNumber lsn : observedLSNs){
          Long val = highestSequenceNumberForLSN.get(lsn);
          if ((val != null) && (ret == null || val > ret)){
            ret = val;
          }
        }
      }
      
      return ret;
    }

    public synchronized OLogSequenceNumber getMappedLSN(Long sequenceNumber) {
      return LSNForHighestSequenceNumber.get(sequenceNumber);
    }    

    public synchronized void setHighestFlushedSequenceNumber(Long value) {
      highestFlushedSequenceNumber = value;
    }    

    public synchronized Long getHighestFlushedSequenceNumber() {
      return highestFlushedSequenceNumber;
    }

    public synchronized boolean hasUnflushedSequences() {
      return hasUnflushedSequences;
    }

//    public synchronized void setHasUnflushedSequences(boolean hasUnflushedSequences) {
//      this.hasUnflushedSequences = hasUnflushedSequences;
//    }

    public void resetHasUnflushedSequences() {
      synchronized(this){
        if (highestMappedSequenceNumber == null) {
          hasUnflushedSequences = false;
        }
      }
    }

    private void setHighestSequnceNumberCanBeFlushed(Long value) {
      if (value == null){
        return;
      }
      if (highestSequenceNumberCanBeFlushed == null){
        synchronized (this){
          if (highestSequenceNumberCanBeFlushed == null){
            highestSequenceNumberCanBeFlushed = new AtomicLong();
          }
        }
      }
      highestSequenceNumberCanBeFlushed.set(value);
    }

    private Long getHighestSequnceNumberCanBeFlushed() {
      if (highestSequenceNumberCanBeFlushed == null){
        return null;
      }
      
      return highestSequenceNumberCanBeFlushed.get();
    }

    private synchronized void cleanUpTillLSN(OLogSequenceNumber toLSN) {
      Set<OLogSequenceNumber> toBeRemoved = new HashSet<>();
      for (OLogSequenceNumber lsn : highestSequenceNumberForLSN.keySet()){
        if (lsn.compareTo(toLSN) <= 0){
          toBeRemoved.add(lsn);
        }
      }
      for (OLogSequenceNumber toRemove : toBeRemoved){
        highestSequenceNumberForLSN.remove(toRemove);
      }
      
      //remove from mirror map
      List<Long> toBeRemovedMirror = new ArrayList<>();
      for (Long sequenceNumber : LSNForHighestSequenceNumber.keySet()){
        if (toBeRemoved.contains(LSNForHighestSequenceNumber.get(sequenceNumber))){
          toBeRemovedMirror.add(sequenceNumber);
        }
      }
      for (Long toRemove : toBeRemovedMirror){
        LSNForHighestSequenceNumber.remove(toRemove);
      }
    }
  }

}
