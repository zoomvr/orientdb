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

import com.orientechnologies.orient.core.db.record.OIdentifiable;

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
  
  public void track(OIdentifiable rec, long sequenceNumber){
    
  }
  
}
