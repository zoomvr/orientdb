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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.util.BytesRef;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author marko
 */
public class OLuceneDocumentWriteAheadLogTest {
  
  @Test
  public void testSerializeDeserialize(){
    Document doc = new Document();
    doc.add(new StringField("strField", "testValue", Field.Store.YES));
    doc.add(new TextField("txtField", "testValue2", Field.Store.YES));
    doc.add(new NumericDocValuesField("numericField", 1l));
    doc.add(new DoubleDocValuesField("doubleValuesField", 2.33));
    doc.add(new SortedDocValuesField("sortedDoc", new BytesRef("testValue3".getBytes())));
    
    OLuceneDocumentWriteAheadRecord walRecord = new OLuceneDocumentWriteAheadRecord(doc, 1, "testName", 0, null);
    byte[] stream = new byte[walRecord.serializedSize()];
    walRecord.toStream(stream, 0);
    OLuceneDocumentWriteAheadRecord deserialized = new OLuceneDocumentWriteAheadRecord(null);
    deserialized.fromStream(stream, 0);
    Assert.assertEquals(walRecord.getIndexName(), deserialized.getIndexName());
    Assert.assertEquals(walRecord.getLuceneWriterIndex(), deserialized.getLuceneWriterIndex());
    Assert.assertEquals(walRecord.getSequenceNumber(), deserialized.getSequenceNumber());
    Assert.assertEquals(walRecord.getDocument().getFields().size(), deserialized.getDocument().getFields().size());
    Assert.assertArrayEquals(walRecord.getDocument().getField("sortedDoc").binaryValue().bytes, 
            deserialized.getDocument().getField("sortedDoc").binaryValue().bytes);
  }
  
}
