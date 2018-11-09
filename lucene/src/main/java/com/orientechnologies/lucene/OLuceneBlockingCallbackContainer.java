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

    @Override
    public void run() {
      if (isRAMDirectory()){
        System.out.println("RAM DIRECTORY NO NEED TO WAIT");
        return;
      }
      Long highestSequnceCanBeFlushed = OLuceneTracker.instance().getHighestSequnceNumberCanBeFlushed(getWriterIndex());
      List<Long> tmpCollection = new LinkedList<>();
      tmpCollection.add(getWriterIndex());
      if (OLuceneTracker.instance().hasUnflushedSequences(tmpCollection)){
        while (highestSequnceCanBeFlushed == null || getSequenceNumber() > highestSequnceCanBeFlushed){
          System.out.println("WAITING for: " + getSequenceNumber() + ", " + System.currentTimeMillis() + " Writer id: " + getWriterIndex());
          waitSomeTime(1000l);
          highestSequnceCanBeFlushed = OLuceneTracker.instance().getHighestSequnceNumberCanBeFlushed(getWriterIndex());          
          System.out.println("DETECTED HIGHEST CAN BE FLUSHED: " + highestSequnceCanBeFlushed + ", " + System.currentTimeMillis() + " Writer id: " + getWriterIndex());
        }
        System.out.println("RELEASED LUCENE LOCK for: " + getSequenceNumber() + ", " + System.currentTimeMillis() + " Writer id: " + getWriterIndex());
        OLuceneTracker.instance().resetHasUnflushedSequences(getWriterIndex());
      }
      else{
        System.out.println("NOTHING TO FLUSH FOR INDEX WRITER: " + getWriterIndex() +  ", " + System.currentTimeMillis());
      }
    }
    
  }    
  
  public static class OLuceneSynchCallbackAfter extends IndexWriter.FlushCallback{

    @Override
    public void run() {
      OLuceneTracker.instance().setHighestFlushedSequenceNumber(this.getSequenceNumber(), getWriterIndex());
    }
    
  }  
  
}
