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
package com.orientechnologies.lucene.engine;

import java.lang.ref.WeakReference;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import org.apache.lucene.store.AlreadyClosedException;

/**
 *
 * @author marko
 */
public class OLuceneIndexEngineCollection {
  private class WeakRefToLuceneIndexEngine extends WeakReference<OLuceneIndexEngineAbstract>{
    
    public WeakRefToLuceneIndexEngine(OLuceneIndexEngineAbstract indexEngine) {
      super(indexEngine);
    }
    
    @Override
    public int hashCode(){
      if (get() != null){
        return get().hashCode();
      }
      return 0;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final WeakRefToLuceneIndexEngine other = (WeakRefToLuceneIndexEngine) obj;
      return Objects.equals(this.get(), other.get());
    }
    
  };
  
  
  private static OLuceneIndexEngineCollection instance;
  
  public static OLuceneIndexEngineCollection instance(){
    if (instance == null){
      synchronized(OLuceneIndexEngineCollection.class){
        if (instance == null){
          instance = new OLuceneIndexEngineCollection();
        }
      }
    }
    
    return instance;
  }
  
  
  private final Set<WeakRefToLuceneIndexEngine> collection = new HashSet<>();
  
  public synchronized void trackLuceneIndexEngine(OLuceneIndexEngineAbstract indexEngine){
    //may hapend that some engienn is not alive any more , and then new index engien with same
    //unique index is added. So first remove old and then add new
    WeakRefToLuceneIndexEngine wr = new WeakRefToLuceneIndexEngine(indexEngine);
    if (collection.contains(wr)){
      collection.remove(wr);
    }
    collection.add(wr);
  }
  
  public synchronized long getOverallRamSize(){
    long retVal = 0l;
    Iterator<WeakRefToLuceneIndexEngine> iter = collection.iterator();
    while(iter.hasNext()){
      WeakRefToLuceneIndexEngine refToIndexEngine = iter.next();
      OLuceneIndexEngineAbstract indexEngine = refToIndexEngine.get();
      if (indexEngine != null){
        try{
          retVal += indexEngine.indexWriter.ramBytesUsed();
        }
        catch (AlreadyClosedException exc){
          System.out.println("INDEX ENGINE CLOSED: " + indexEngine.indexWriter.getUniqueIndex());
          //cleanup non valid refs, on reopen it will be added to collection again
          iter.remove();
        }
      }
      else{
        //cleanup non valid refs
        iter.remove();
      }
    }
    System.out.println("$$LUCENE RAM SIZE: " + retVal);
    return retVal;
  }
  
}
