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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.builder.OLuceneDocumentBuilder;
import com.orientechnologies.orient.core.index.OIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinaryV1;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLuceneEntryWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import java.nio.ByteBuffer;
import org.apache.lucene.document.Document;

/**
 *
 * @author marko
 */
public class OLuceneDocumentWriteAheadRecord extends OLuceneEntryWALRecord{

  private Document document;
  private long sequenceNumber;
  private String indexName;
  private long luceneWriterIndex;
  private ORecordSerializerBinaryV1 serializer = new ORecordSerializerBinaryV1();
  
  public OLuceneDocumentWriteAheadRecord(OLogSequenceNumber previousCheckPoint){
    super(previousCheckPoint);
  }
  
  public OLuceneDocumentWriteAheadRecord(Document document, long sequenceNumber, 
          String indexName, long luceneWriterIndex, 
          OLogSequenceNumber previousCheckPoint){
    super(previousCheckPoint);
    this.document = document;
    this.sequenceNumber = sequenceNumber;
    this.indexName = indexName;
    this.luceneWriterIndex = luceneWriterIndex;
  }
  
  private byte[] internalSerialization(){
    //serialize document
    byte[] stream = OLuceneDocumentBuilder.serializeDocument(document);
    BytesContainer bytes = new BytesContainer(stream);
    bytes.offset = stream.length;
    //serialize sequence number
    serializer.serializeValue(bytes, sequenceNumber, OType.LONG, null);
    //serialize indexName
    serializer.serializeValue(bytes, indexName, OType.STRING, null);
    //serialize lucene writer index
    serializer.serializeValue(bytes, luceneWriterIndex, OType.LONG, null);
    return bytes.fitBytes();
  }
  
  @Override
  public int toStream(byte[] content, int offset) {
    byte[] serialized = internalSerialization();
    System.arraycopy(serialized, 0, content, offset, serialized.length);
    return offset + serialized.length;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    byte[] stream = internalSerialization();
    buffer.put(stream);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    HelperClasses.Tuple<Integer, Document> ret = null;
    ret = OLuceneDocumentBuilder.deserializeDocument(content, offset);
    BytesContainer bytes = new BytesContainer(content);
    bytes.offset = ret.getFirstVal();
    document = ret.getSecondVal();
    sequenceNumber = (long)serializer.deserializeValue(bytes, OType.LONG, null);
    indexName = (String)serializer.deserializeValue(bytes, OType.STRING, null);
    luceneWriterIndex = (long)serializer.deserializeValue(bytes, OType.LONG, null);
    return bytes.offset;
  }

  @Override
  public int serializedSize() {
    return internalSerialization().length;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public long getSequenceNumber() {
    return sequenceNumber;
  }

  @Override
  public String getIndexName() {
    return indexName;
  }

  @Override
  public void addToIndex(OIndexEngine index, OWriteAheadLog wal) {
    if (index == null){
      OLogManager.instance().warn(this, "Passed null index engine", (Object[])null);
      return;
    }
    if (!index.isLuceneIndex()){
      OLogManager.instance().warn(this, "Lucene WAL record can be added only to lucene index", (Object[])null);
      return;
    }
    
    OLuceneIndexEngine luceneIndexEngine = (OLuceneIndexEngine)index;
    if (luceneIndexEngine.isDelegator()){
      OLuceneIndexEngineDelegatorAbstract delegator = (OLuceneIndexEngineDelegatorAbstract)index;
      delegator.addDocument(document, wal);
    }
    else{
      OLuceneIndexEngineAbstract abstractLuceneIndexEngine = (OLuceneIndexEngineAbstract)index;
      abstractLuceneIndexEngine.addDocument(document, wal);
    }
  }

  @Override
  public long getLuceneWriterIndex() {
    return luceneWriterIndex;
  }

  public Document getDocument() {
    return document;
  }
  
}
