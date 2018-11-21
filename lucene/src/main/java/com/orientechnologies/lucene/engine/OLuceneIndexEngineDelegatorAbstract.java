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

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import com.orientechnologies.spatial.engine.OLuceneSpatialIndexEngineAbstract;
import org.apache.lucene.document.Document;

/**
 *
 * @author marko
 */
public abstract class OLuceneIndexEngineDelegatorAbstract implements OLuceneIndexEngine{
  
  protected OLuceneSpatialIndexEngineAbstract delegate;
  
  @Override
  public boolean isDelegator(){
    return true;
  }
  
  @Override
  public boolean isLuceneIndex(){
    return delegate.isLuceneIndex();
  }
  
  protected void addDocument(Document doc, OWriteAheadLog writeAheadLog){
    delegate.addDocument(doc, writeAheadLog);
  }
}
