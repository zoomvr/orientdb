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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author marko
 */
public class OLuceneTracker {

  private static OLuceneTracker instance = null;
  private final Map<Long, PerIndexWriterTRracker> mappedTrackers = new HashMap<>();
  private final Long[] locks = new Long[128];
  private AtomicLong highestSequnceNumberCanBeFlushed = null;
  
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
  
  public void track(ORecordId rec, long sequenceNumber, Long writerIndex){
    synchronized(getLockObject(writerIndex)){
      PerIndexWriterTRracker tracker = mappedTrackers.get(writerIndex);
      if (tracker == null){
        tracker = new PerIndexWriterTRracker();
        mappedTrackers.put(writerIndex, tracker);
      }
      tracker.track(rec, sequenceNumber);
    }
  }
  
  public long getLargestsequenceNumber(List<ORecordId> observedIds, Collection<Long> writerIndices){
    long retVal = -1;
    if (writerIndices == null){
      return retVal;
    }
    for (Long writerId : writerIndices){
      synchronized(getLockObject(writerId)){
        PerIndexWriterTRracker tracker = mappedTrackers.get(writerId);
        if (tracker != null){
          long val = tracker.getLargestsequenceNumber(observedIds);
          if (val > retVal){
            System.out.println("GET LARGEST SEQUENCE NUMBER: " + val);
            retVal = val;
          }
        }
      }
    }
    return retVal;
  }
  
  public void mapLSNToHighestSequenceNumber(OLogSequenceNumber lsn, Long sequenceNumber, Collection<Long> writerIds){
    if (writerIds == null){
      return;
    }
    for (Long writerId : writerIds){
      synchronized(getLockObject(writerId)){
        PerIndexWriterTRracker tracker = mappedTrackers.get(writerId);
        if (tracker != null){
          System.out.println("MAPPED LARGEST SEQUENCE NUMBER: " + sequenceNumber + " TO LSN: " + lsn);
          tracker.mapLSNToHighestSequenceNumber(lsn, sequenceNumber);
        }
      }
    }
  }
  
  public void clearMappedRidsToHighestSequenceNumbers(Collection<Long> writerIds){
    if (writerIds == null){
      return;
    }
    for (Long writerId : writerIds){
      synchronized(getLockObject(writerId)){
        PerIndexWriterTRracker tracker = mappedTrackers.get(writerId);
        if (tracker != null){
          tracker.clearMappedRidsToHighestSequenceNumbers();
        }
      }
    }
  }
  
  public Long getHighestSequnceNumberCanBeFlushed(Long writerId){
    System.out.println("INTO GET HIGHEST SEQUENCE CAN BE FLUSHED");
    if (highestSequnceNumberCanBeFlushed != null){
      System.out.println("INTO GET HIGHEST SEQUENCE CAN BE FLUSHED RETURN: " + highestSequnceNumberCanBeFlushed.get());
      return highestSequnceNumberCanBeFlushed.get();
    }
    System.out.println("INTO GET HIGHEST SEQUENCE CAN BE FLUSHED RETURN: null");
    return null;
  }
  
  public void setHighestSequnceNumberCanBeFlushed(Long value) {
    if (highestSequnceNumberCanBeFlushed == null){
      synchronized(this){
        if (highestSequnceNumberCanBeFlushed == null){
          highestSequnceNumberCanBeFlushed = new AtomicLong();
        }
      }
    }
    highestSequnceNumberCanBeFlushed.set(value);
    System.out.println("Sequence number can be flushed value: " + highestSequnceNumberCanBeFlushed.get());
  }
  
  public void setHighestFlushedSequenceNumber(Long value, Long writerId) {
    synchronized(getLockObject(writerId)){
      PerIndexWriterTRracker tracker = mappedTrackers.get(writerId);
      if (tracker != null){
        tracker.setHighestFlushedSequenceNumber(value);
      }
    }
  }
  
  public Long getNearestSmallerOrEqualSequenceNumber(Long referentVal){    
    Long retVal = null;
    for (Long writerId : mappedTrackers.keySet()){
      synchronized(getLockObject(writerId)){
        PerIndexWriterTRracker tracker = mappedTrackers.get(writerId);
        Long tmpVal = tracker.getNearestSmallerOrEqualSequenceNumber(referentVal);
        if ((tmpVal != null) && (retVal == null || tmpVal > retVal)){
          retVal = tmpVal;
        }
      }
    }
    
    return retVal;
  }
  
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
  
  public Long getMappedSequenceNumber(OLogSequenceNumber lsn){
    for (Long writerId : mappedTrackers.keySet()){
      synchronized(getLockObject(writerId)){
        PerIndexWriterTRracker tracker = mappedTrackers.get(writerId);
        Long tmpVal = tracker.getMappedSequenceNumber(lsn);
        if (tmpVal != null){
          return tmpVal;
        }
      }
    }
    
    return null;
  }
    
  public Long getHighestFlushedSequenceNumber(){
    Long retVal = null;
    for (Long writerId : mappedTrackers.keySet()){
      synchronized(getLockObject(writerId)){
        PerIndexWriterTRracker tracker = mappedTrackers.get(writerId);
        Long tmpVal = tracker.getHighestFlushedSequenceNumber();
        if ((tmpVal != null) && (retVal == null || tmpVal > retVal)){
          retVal = tmpVal;
        }
      }
    }
    return retVal;
  }
  
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
  
  public void resetHasUnflushedSequences(){    
    for (Long writerId : mappedTrackers.keySet()){
      synchronized(getLockObject(writerId)){
        PerIndexWriterTRracker tracker = mappedTrackers.get(writerId);
        tracker.resetHasUnflushedSequences();
      }
    }    
  }
  
  public synchronized boolean isHasUnflushedSequences() {
    for (Long writerId : mappedTrackers.keySet()){
      synchronized(getLockObject(writerId)){
        PerIndexWriterTRracker tracker = mappedTrackers.get(writerId);
        if (tracker.isHasUnflushedSequences()){
          return true;
        }
      }
    }
    return false;
  }
  
  private class PerIndexWriterTRracker {

    private final Map<ORecordId, Long> mappedHighestsequnceNumbers = new HashMap<>();
    private final Map<OLogSequenceNumber, Long> highestSequenceNumberForLSN = new HashMap<>();
    private final Map<Long, OLogSequenceNumber> LSNForHighestSequenceNumber = new HashMap<>();
    
    private Long highestFlushedSequenceNumber = null;
    private boolean hasUnflushedSequences = false;

    public void track(ORecordId rec, long sequenceNumber) {
      hasUnflushedSequences = true;
      if (rec == null) {
        return;
      }
      System.out.println("----------------------------SEQUNCE NUMBER: " + sequenceNumber);
      //TODO need better synchronization
      synchronized (this) {
        Long val = mappedHighestsequnceNumbers.get(rec);
        if (val == null || val < sequenceNumber) {
          mappedHighestsequnceNumbers.put(rec, sequenceNumber);
        }
      }
    }

    public long getLargestsequenceNumber(List<ORecordId> observedIds) {
      long retVal = -1l;
      synchronized (this) {
        for (ORecordId rec : observedIds) {
          Long val = mappedHighestsequnceNumbers.get(rec);
          if (val != null && val > retVal) {
            retVal = val;
          }
        }
      }
      return retVal;
    }

    public void clearMappedRidsToHighestSequenceNumbers() {
      mappedHighestsequnceNumbers.clear();
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
      synchronized (this) {
        Long[] tmpListForSort = LSNForHighestSequenceNumber.keySet().toArray(new Long[0]);
        Arrays.sort(tmpListForSort);
        //find last smaller then specified
        for (int i = tmpListForSort.length - 1; i >= 0; i--) {
          if (tmpListForSort[i] == null) {
            System.out.println("Index is: " + i);
          }
          if (tmpListForSort[i] <= referentVal) {
            return tmpListForSort[i];
          }
        }
      }

      return null;
    }

    public OLogSequenceNumber getNearestSmallerOrEqualLSN(OLogSequenceNumber toLsn) {
      synchronized (this) {
        OLogSequenceNumber[] tmpListForSort = highestSequenceNumberForLSN.keySet().toArray(new OLogSequenceNumber[0]);
        Arrays.sort(tmpListForSort);
        //find last smaller then specified
        for (int i = tmpListForSort.length - 1; i >= 0; i--) {
          if (tmpListForSort[i].compareTo(toLsn) <= 0) {
            return tmpListForSort[i];
          }
        }
      }

      return null;
    }

    public Long getMappedSequenceNumber(OLogSequenceNumber lsn) {
      synchronized(highestSequenceNumberForLSN){
        return highestSequenceNumberForLSN.get(lsn);
      }
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

    public synchronized boolean isHasUnflushedSequences() {
      return hasUnflushedSequences;
    }

    public synchronized void setHasUnflushedSequences(boolean hasUnflushedSequences) {
      this.hasUnflushedSequences = hasUnflushedSequences;
    }

    public synchronized void resetHasUnflushedSequences() {
      if (mappedHighestsequnceNumbers.isEmpty()) {
        hasUnflushedSequences = false;
      }
    }
  }

}
