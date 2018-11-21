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
package com.orientechnologies.lucene.builder;

import com.orientechnologies.orient.core.metadata.schema.OType;
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
import org.apache.lucene.index.NumericDocValues;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author marko
 */
public class CreateLuceneFieldTest {
  
  @Test
  public void testCreateLuceneStringField(){
    String className  = StringField.class.getCanonicalName();
    String testVal = "testVal";
    IndexableField field = OLuceneDocumentBuilder.createFieldBasedOnClassNameAndValue(className, "testFiled", OType.STRING, testVal);
    Assert.assertNotNull("Field not created", field);
    Assert.assertEquals(testVal, field.stringValue());
  }
  
  @Test
  public void testCreateLuceneTextField(){
    String className  = TextField.class.getCanonicalName();
    String testVal = "testVal";
    IndexableField field = OLuceneDocumentBuilder.createFieldBasedOnClassNameAndValue(className, "testFiled", OType.STRING, testVal);
    Assert.assertNotNull("Field not created", field);
    Assert.assertEquals(testVal, field.stringValue());
  }
  
  @Test
  public void testCreateNumericDocValuesField(){
    String className  = NumericDocValuesField.class.getCanonicalName();
    long testVal = 1l;
    IndexableField field = OLuceneDocumentBuilder.createFieldBasedOnClassNameAndValue(className, "testFiled", OType.LONG, testVal);
    Assert.assertNotNull("Field not created", field);
    Assert.assertEquals(testVal, field.numericValue());
  }
  
  @Test
  public void testCreateFloatDocValuesField(){
    FloatDocValuesField dp = new FloatDocValuesField("kk", 1.0f);
    Object testVal = dp.numericValue();
    
    String className  = FloatDocValuesField.class.getCanonicalName();
    IndexableField field = OLuceneDocumentBuilder.createFieldBasedOnClassNameAndValue(className, "testFiled", OType.getTypeByValue(testVal), testVal);
    Assert.assertNotNull("Field not created", field);
    Assert.assertEquals(1.0f, Float.intBitsToFloat(field.numericValue().intValue()), 0.001f);
  }
  
  @Test
  public void testCreateDoubleDocValuesField(){
    DoubleDocValuesField dp = new DoubleDocValuesField("kk", 1.0);
    Object testVal = dp.numericValue();
    
    String className  = DoubleDocValuesField.class.getCanonicalName();
    IndexableField field = OLuceneDocumentBuilder.createFieldBasedOnClassNameAndValue(className, "testFiled", OType.getTypeByValue(testVal), testVal);
    Assert.assertNotNull("Field not created", field);
    Assert.assertEquals(1.0, Double.longBitsToDouble(field.numericValue().longValue()), 0.001);
  }
  
  @Test
  public void testCreateFloatPoint(){
    FloatPoint fp = new FloatPoint("kk", 1.0f);
    Number testVal = fp.numericValue();
    
    String className  = FloatPoint.class.getCanonicalName();
    IndexableField field = OLuceneDocumentBuilder.createFieldBasedOnClassNameAndValue(className, "testFiled", OType.getTypeByValue(testVal), testVal);
    Assert.assertNotNull("Field not created", field);
    Assert.assertEquals(1.0, field.numericValue().floatValue(), 0.001f);
  }
  
  @Test
  public void testCreateLongPoint(){
    LongPoint lp = new LongPoint("kk", 1l);
    Number testVal = lp.numericValue();
    
    String className  = LongPoint.class.getCanonicalName();
    IndexableField field = OLuceneDocumentBuilder.createFieldBasedOnClassNameAndValue(className, "testFiled", OType.getTypeByValue(testVal), testVal);
    Assert.assertNotNull("Field not created", field);
    Assert.assertEquals(1l, field.numericValue().longValue());
  }
  
  @Test
  public void testCreateDoublePoint(){
    DoublePoint dp = new DoublePoint("kk", 1.0);
    Number testVal = dp.numericValue();
    
    String className  = DoublePoint.class.getCanonicalName();
    IndexableField field = OLuceneDocumentBuilder.createFieldBasedOnClassNameAndValue(className, "testFiled", OType.getTypeByValue(testVal), testVal);
    Assert.assertNotNull("Field not created", field);
    Assert.assertEquals(1.0, field.numericValue().doubleValue(), 0.001);
  }
  
  @Test
  public void testCreateIntPoint(){
    IntPoint ip = new IntPoint("kk", 1);
    Number testVal = ip.numericValue();
    
    String className  = IntPoint.class.getCanonicalName();
    IndexableField field = OLuceneDocumentBuilder.createFieldBasedOnClassNameAndValue(className, "testFiled", OType.getTypeByValue(testVal), testVal);
    Assert.assertNotNull("Field not created", field);
    Assert.assertEquals(1, field.numericValue().intValue());
  }
  
  @Test
  public void testCreateSortedDocValuesField(){
    String className  = SortedDocValuesField.class.getCanonicalName();
    byte[] testVal = new byte[1];
    IndexableField field = OLuceneDocumentBuilder.createFieldBasedOnClassNameAndValue(className, "testFiled", OType.BINARY, testVal);
    Assert.assertNotNull("Field not created", field);
    Assert.assertArrayEquals(testVal, field.binaryValue().bytes);
  }
}
