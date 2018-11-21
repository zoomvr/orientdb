/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.lucene.builder;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.orientechnologies.lucene.builder.OLuceneIndexType.createField;
import static com.orientechnologies.lucene.builder.OLuceneIndexType.createFields;
import static com.orientechnologies.lucene.engine.OLuceneIndexEngineAbstract.RID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinaryV1;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.util.Arrays;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;

/**
 * Created by Enrico Risa on 02/09/15.
 */
public class OLuceneDocumentBuilder {

  public Document newBuild(OIndexDefinition indexDefinition, Object key, OIdentifiable oid) {

    if (oid != null) {
      ORecord record = oid.getRecord();

      OElement element = record.load();

    }

    return null;
  }

  public Document build(OIndexDefinition definition,
      Object key,
      OIdentifiable value,
      Map<String, Boolean> fieldsToStore,
      ODocument metadata) {

    Document doc = new Document();

    if (value != null) {
      doc.add(createField(RID, value.getIdentity().toString(), Field.Store.YES));
      doc.add(createField("_CLUSTER", "" + value.getIdentity().getClusterId(), Field.Store.YES));
      doc.add(createField("_CLASS", definition.getClassName(), Field.Store.YES));

    }

    List<Object> formattedKey = formatKeys(definition, key);

    int i = 0;
    for (String field : definition.getFields()) {
      Object val = formattedKey.get(i);
      i++;
      if (val != null) {
//        doc.add(createField(field, val, Field.Store.YES));
        createFields(field, val, Field.Store.YES)
            .forEach(f -> doc.add(f));

        //for cross class index
        createFields(definition.getClassName() + "." + field, val, Field.Store.YES)
            .forEach(f -> doc.add(f));

      }
    }

//    System.out.println("doc = " + doc);
    return doc;
  }

  private List<Object> formatKeys(OIndexDefinition definition, Object key) {
    List<Object> keys;

    if (key instanceof OCompositeKey) {
      keys = ((OCompositeKey) key).getKeys();
    } else if (key instanceof List) {
      keys = ((List) key);
    } else {
      keys = new ArrayList<Object>();
      keys.add(key);
    }

    // a sort of padding
    for (int i = keys.size(); i < definition.getFields().size(); i++) {
      keys.add("");
    }
    return keys;
  }

  protected Field.Store isToStore(String f, Map<String, Boolean> collectionFields) {
    return collectionFields.get(f) ? Field.Store.YES : Field.Store.NO;
  }
  
  public static byte[] serializeDocument(final Document doc){
    final BytesContainer container = new BytesContainer();
    final ORecordSerializerBinaryV1 serializer = new ORecordSerializerBinaryV1();
    
    final List<IndexableField> fields = doc.getFields();
    //write nuymber of the fields
//    container.alloc(1);
//    HelperClasses.writeOType(container, 0, OType.INTEGER);
    serializer.serializeValue(container, fields.size(), OType.INTEGER, null);
    for (IndexableField field : fields){
      String name = field.name();
      if (name == null || name.equals("")){
        serializer.writeEmptyString(container);
      }
      else{
        serializer.serializeValue(container, name, OType.STRING, null);
      }
      
      //store class name , so it will be used to create appropriate class
      String fieldClass = field.getClass().getCanonicalName();
      serializer.serializeValue(container, fieldClass, OType.STRING, null);
      
      if (field.binaryValue() != null){
        BytesRef byteRef = field.binaryValue();
        byte[] bytes = Arrays.copyOfRange(byteRef.bytes, byteRef.offset, byteRef.offset + byteRef.length);
        int pos = container.alloc(1);
        HelperClasses.writeOType(container, pos, OType.BINARY);
        serializer.serializeValue(container, bytes, OType.BINARY, null);
      }
      else if (field.numericValue() != null){
        Number number = field.numericValue();
        int pos = container.alloc(1);
        OType valType;
        Object val;
        if (number instanceof Byte){
          valType = OType.BYTE;  
          val = number.byteValue();
        }
        else if (number instanceof Integer || number instanceof Long || number instanceof Short){
          valType = OType.LONG;
          val = number.longValue();
        }
        else if (number instanceof Double){
          valType = OType.DOUBLE;
          val = number.doubleValue();
        }
        else if (number instanceof Float){
          valType = OType.FLOAT;
          val = number.floatValue();
        }
        else{ //this means that value should be big decimal
          valType = OType.DECIMAL;
          val = (BigDecimal)number;
        }
        HelperClasses.writeOType(container, pos, valType);
        serializer.serializeValue(container, val, valType, null);
      }
      else if (field.stringValue() != null){
        String val = field.stringValue();
        int pos = container.alloc(1);
        HelperClasses.writeOType(container, pos, OType.STRING);
        serializer.serializeValue(container, val, OType.STRING, null);
      }      
      else{
        //this value can't handle for now
        
      }
    }
    
    return container.fitBytes();
  }
  
  public static HelperClasses.Tuple<Integer, Document> deserializeDocument(byte[] bytes, int offset) {
    Document doc = new Document();
    ORecordSerializerBinaryV1 serializer = new ORecordSerializerBinaryV1();
    BytesContainer container = new BytesContainer(bytes);
    container.offset = offset;
    int fieldsSize = (int)serializer.deserializeValue(container, OType.INTEGER, null);
    for (int i = 0; i < fieldsSize; i++){
      String name = "";
      Integer nameLength = (Integer)serializer.deserializeValue(container, OType.INTEGER, null);
      if (nameLength > 0){
        name = HelperClasses.stringFromBytes(bytes, container.offset, nameLength);
        container.offset += nameLength;
      }
      
      String fieldClass = (String)serializer.deserializeValue(container, OType.STRING, null);
      
      OType type = HelperClasses.readOType(container, false);
      Object val = serializer.deserializeValue(container, type, null);
      
      IndexableField field = createFieldBasedOnClassNameAndValue(fieldClass, name, type, val);
      doc.add(field);
               
    }
    
    return new HelperClasses.Tuple<>(container.offset, doc);
  }
  
  protected static IndexableField createFieldBasedOnClassNameAndValue(final String className, String name, final OType type, Object val){
    //can't do with refelection, because stored types are different than actaul types 
    //(double is stored as long, and when get numeric value long is returned not double)
    if (className == null){
      return null;
    }
    if (className.equals(StringField.class.getCanonicalName())){
      if (type == OType.STRING){
        return new StringField(name, (String)val, Field.Store.YES);
      }
    }
    else if (className.equals(TextField.class.getCanonicalName())){
      if (type == OType.STRING){
        return new TextField(name, (String)val, Field.Store.YES);
      }
    }
    else if (className.equals(NumericDocValuesField.class.getCanonicalName())){
      if (type == OType.LONG){
        return new NumericDocValuesField(name, (Long)val);
      }
    }
    else if (className.equals(FloatDocValuesField.class.getCanonicalName())){
      if (type == OType.LONG){
        return new FloatDocValuesField(name, Float.intBitsToFloat(((Long)val).intValue()));
      }
      else if (type == OType.DOUBLE){
        return new FloatDocValuesField(name, (Float)val);
      }
    }
    else if (className.equals(DoubleDocValuesField.class.getCanonicalName())){
      if (type == OType.LONG){
        return new DoubleDocValuesField(name, Double.longBitsToDouble((Long)val));
      }
      else if (type == OType.DOUBLE){
        return new DoubleDocValuesField(name, (Double)val);
      }
    }
    else if (className.equals(FloatPoint.class.getCanonicalName())){
      if (type == OType.FLOAT){
        return new FloatPoint(name, (Float)val);
      }
    }
    else if (className.equals(LongPoint.class.getCanonicalName())){
      if (type == OType.LONG){
        return new LongPoint(name, (Long)val);
      }
    }
    else if (className.equals(DoublePoint.class.getCanonicalName())){
      if (type == OType.DOUBLE){
        return new DoublePoint(name, (Double)val);
      }
    }
    else if (className.equals(IntPoint.class.getCanonicalName())){
      if (type == OType.INTEGER){
        return new IntPoint(name, (Integer)val);
      }
    }
    else if (className.equals(SortedDocValuesField.class.getCanonicalName())){
      if (type == OType.BINARY){
        BytesRef byteRef = new BytesRef((byte[])val);
        return new SortedDocValuesField(name, byteRef);
      }
    }
    
    return null;
  }
}
