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
package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.orient.core.index.OIndexEngine;

/**
 *
 * @author marko
 */
public abstract class OLuceneEntryWALRecord extends OAbstractWALRecord{
  
  public OLuceneEntryWALRecord(OLogSequenceNumber previousCheckPoint){
    super(previousCheckPoint);
  }
  
  public abstract long getSequenceNumber();
  public abstract String getIndexName();
  //intention is to use it in restore
  public abstract void addToIndex(OIndexEngine index, OWriteAheadLog wal);
  public abstract long getLuceneWriterIndex();
  
  @Override
  public byte getId() {
    return WALRecordTypes.LUCENE_DOCUMENT_RECORD;
  }
}
