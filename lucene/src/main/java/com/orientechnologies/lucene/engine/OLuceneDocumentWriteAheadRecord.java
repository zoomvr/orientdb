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

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLuceneEntryWALRecord;
import java.nio.ByteBuffer;
import org.apache.lucene.document.Document;

/**
 *
 * @author marko
 */
public class OLuceneDocumentWriteAheadRecord extends OLuceneEntryWALRecord{

  private final Document document;
  private final long sequenceNumber;
  
  public OLuceneDocumentWriteAheadRecord(Document document, long sequenceNumber, OLogSequenceNumber previousCheckPoint){
    super(previousCheckPoint);
    this.document = document;
    this.sequenceNumber = sequenceNumber;
  }
  
  @Override
  public int toStream(byte[] content, int offset) {
    
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public int serializedSize() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public boolean isUpdateMasterRecord() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public long getSequenceNumber() {
    return sequenceNumber;
  }

  @Override
  public String getIndexName() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void addToIndex() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public long getLuceneWriterIndex() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }
  
}
