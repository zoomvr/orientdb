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
package com.orientechnologies.lucene;

import com.orientechnologies.orient.core.index.OLuceneTracker;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import java.util.LinkedList;
import java.util.List;
import org.apache.lucene.index.IndexWriter;

/**
 *
 * @author mdjurovi
 */
public class OLuceneBlockingCallbackContainer {
  
  private static void waitSomeTime(long time){
    try{
      Thread.sleep(time);
    }
    catch (InterruptedException ignore){}
  }
  
  public static class OLuceneSynchCallbackBefore extends IndexWriter.FlushCallback{
    
    private int cycleNo = 0;
    
    public OLuceneSynchCallbackBefore(){      
    }
    
    @Override
    public Long call() throws IndexWriter.RetryOvercount{      
      if (isRAMDirectory()){
        System.out.println("RAM DIRECTORY NO NEED TO WAIT");
        return getSequenceNumber();
      }
      Long highestSequnceCanBeFlushed = OLuceneTracker.instance().getHighestSequnceNumberCanBeFlushed(getWriterIndex());
      final List<Long> tmpCollection = new LinkedList<>();
      tmpCollection.add(getWriterIndex());
      if (OLuceneTracker.instance().hasUnflushedSequences(tmpCollection)){
        ++cycleNo;
        int counter = 0;
        System.out.println("BEFORE CALLBACK INDEX WRITER: " + getWriterIndex() + ", SEQ NO: " + getSequenceNumber() + ", HIGHEST CAN BE FLUSHED: " + highestSequnceCanBeFlushed + ", CYCLE NO: " + cycleNo);
        while (highestSequnceCanBeFlushed == null || getSequenceNumber() > highestSequnceCanBeFlushed + (cycleNo * getLuceneMagicNumber())){
//          if (counter == 10){
//            System.out.println("RETRY OVERCOUNT FOR WRITER: " + getWriterIndex() + ", CYCLE NO: " + cycleNo);
//            System.out.println("PERFORMING SYNC FOR WRITER: " + getWriterIndex());
//            synchronized(storage){
//              if (storage instanceof OAbstractPaginatedStorage){
//                ((OAbstractPaginatedStorage)storage).makeFuzzyCheckpoint();
//              }              
//            }
//          }
          System.out.println("WAITING for: " + getSequenceNumber() + ", " + System.currentTimeMillis() + " Writer id: " + getWriterIndex());
          waitSomeTime(1000l);
          highestSequnceCanBeFlushed = OLuceneTracker.instance().getHighestSequnceNumberCanBeFlushed(getWriterIndex());          
          System.out.println("DETECTED HIGHEST CAN BE FLUSHED: " + highestSequnceCanBeFlushed + ", " + System.currentTimeMillis() + " Writer id: " + getWriterIndex());
          counter++;
        }
        System.out.println("RELEASED LUCENE LOCK for: " + getSequenceNumber() + ", " + System.currentTimeMillis() + " Writer id: " + getWriterIndex());
        cycleNo = 0;
        OLuceneTracker.instance().resetHasUnflushedSequences(getWriterIndex());        
      }
      else{
        System.out.println("NOTHING TO FLUSH FOR INDEX WRITER: " + getWriterIndex() +  ", " + System.currentTimeMillis());
      }
            
      return getSequenceNumber();      
    }
    
  }    
  
  public static class OLuceneSynchCallbackAfter extends IndexWriter.FlushCallback{

    @Override
    public Long call(){
      System.out.println("HIGHEST FLUSHED FOR: " + getWriterIndex() + ", IS: " + getSequenceNumber());
      OLuceneTracker.instance().setHighestFlushedSequenceNumber(this.getSequenceNumber(), getWriterIndex());
      return getSequenceNumber();
    }
    
  }  
  
}
