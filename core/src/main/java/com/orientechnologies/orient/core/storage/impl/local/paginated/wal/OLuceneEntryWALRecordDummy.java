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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.index.OIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinaryV1;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 * @author marko
 */
public class OLuceneEntryWALRecordDummy extends OLuceneEntryWALRecord{
  private long sequenceNumber;
  private String indexName;
  private long luceneWriterIndex;
  private ORecordSerializerBinaryV1 serializer = new ORecordSerializerBinaryV1();
  private byte[] documentBytes;

  public OLuceneEntryWALRecordDummy() {
    super(null);
  }
  
  OLuceneEntryWALRecordDummy(OLogSequenceNumber lsn){
    super(lsn);
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
  public void addToIndex(OIndexEngine index, OWriteAheadLog wal){
    try{
      index.addWalRecordToIndex(this);
    }
    catch (IOException exc){
      OLogManager.instance().error(this, exc.getMessage(), exc, (Object[])null);
    }
  }

  @Override
  public long getLuceneWriterIndex() {
    return luceneWriterIndex;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    BytesContainer bytes = new BytesContainer(content);
    bytes.offset = offset;
    sequenceNumber = (long)serializer.deserializeValue(bytes, OType.LONG, null);
    indexName = (String)serializer.deserializeValue(bytes, OType.STRING, null);
    luceneWriterIndex = (long)serializer.deserializeValue(bytes, OType.LONG, null);
    HelperClasses.Tuple<Integer, byte[]> ret = null;
    ret = deserializeDocumentAsBytes(content, bytes.offset);
    bytes.offset = ret.getFirstVal();
    documentBytes = ret.getSecondVal();
    return bytes.offset;
  }

  @Override
  public int serializedSize() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public boolean isUpdateMasterRecord() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }
  
  private HelperClasses.Tuple<Integer, byte[]> deserializeDocumentAsBytes(byte[] bytes, int offset) {
    BytesContainer container = new BytesContainer(bytes);
    container.offset = offset;
    int fieldsSize = (int)serializer.deserializeValue(container, OType.INTEGER, null);
    for (int i = 0; i < fieldsSize; i++){
      Integer nameLength = (Integer)serializer.deserializeValue(container, OType.INTEGER, null);
      if (nameLength > 0){
        String name = HelperClasses.stringFromBytes(bytes, container.offset, nameLength);
        container.offset += nameLength;
      }
      
      String fieldClass = (String)serializer.deserializeValue(container, OType.STRING, null);
      
      OType type = HelperClasses.readOType(container, false);
      Object val = serializer.deserializeValue(container, type, null);             
    }
    
    int delta = container.offset - offset;
    byte[] retBytes = new byte[delta];
    System.arraycopy(container.bytes, offset, retBytes, 0, delta);
    return new HelperClasses.Tuple<>(container.offset, retBytes);
  }

  public byte[] getDocumentBytes() {
    return documentBytes;
  }
 
}
