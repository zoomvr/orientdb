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
    try{
      IndexableField field = OLuceneDocumentBuilder.createFieldBasedOnClassNameAndValue(className, "testFiled", OType.STRING, testVal);
      Assert.assertNotNull("Field not created", field);
    }
    catch (ClassNotFoundException | InstantiationException exc){
      exc.printStackTrace();
      Assert.assertTrue(exc.getMessage(), false);
    }
  }
  
  @Test
  public void testCreateLuceneTextField(){
    String className  = TextField.class.getCanonicalName();
    String testVal = "testVal";
    try{
      IndexableField field = OLuceneDocumentBuilder.createFieldBasedOnClassNameAndValue(className, "testFiled", OType.STRING, testVal);
      Assert.assertNotNull("Field not created", field);
    }
    catch (ClassNotFoundException | InstantiationException exc){
      exc.printStackTrace();
      Assert.assertTrue(exc.getMessage(), false);
    }
  }
  
  @Test
  public void testCreateNumericDocValuesField(){
    String className  = NumericDocValuesField.class.getCanonicalName();
    long testVal = 1l;
    try{
      IndexableField field = OLuceneDocumentBuilder.createFieldBasedOnClassNameAndValue(className, "testFiled", OType.LONG, testVal);
      Assert.assertNotNull("Field not created", field);
    }
    catch (ClassNotFoundException | InstantiationException exc){
      exc.printStackTrace();
      Assert.assertTrue(exc.getMessage(), false);
    }
  }
  
  @Test
  public void testCreateFloatDocValuesField(){
    String className  = FloatDocValuesField.class.getCanonicalName();
    float testVal = 1.0f;
    try{
      IndexableField field = OLuceneDocumentBuilder.createFieldBasedOnClassNameAndValue(className, "testFiled", OType.FLOAT, testVal);
      Assert.assertNotNull("Field not created", field);
    }
    catch (ClassNotFoundException | InstantiationException exc){
      exc.printStackTrace();
      Assert.assertTrue(exc.getMessage(), false);
    }
  }
  
  @Test
  public void testCreateDoubleDocValuesField(){
    String className  = DoubleDocValuesField.class.getCanonicalName();
    double testVal = 1.0;
    try{
      IndexableField field = OLuceneDocumentBuilder.createFieldBasedOnClassNameAndValue(className, "testFiled", OType.DOUBLE, testVal);
      Assert.assertNotNull("Field not created", field);
    }
    catch (ClassNotFoundException | InstantiationException exc){
      exc.printStackTrace();
      Assert.assertTrue(exc.getMessage(), false);
    }
  }
  
  @Test
  public void testCreateFloatPoint(){
    String className  = FloatPoint.class.getCanonicalName();
    float testVal = 1.0f;
    try{
      IndexableField field = OLuceneDocumentBuilder.createFieldBasedOnClassNameAndValue(className, "testFiled", OType.FLOAT, testVal);
      Assert.assertNotNull("Field not created", field);
    }
    catch (ClassNotFoundException | InstantiationException exc){
      exc.printStackTrace();
      Assert.assertTrue(exc.getMessage(), false);
    }
  }
  
  @Test
  public void testCreateLongPoint(){
    String className  = LongPoint.class.getCanonicalName();
    long testVal = 1l;
    try{
      IndexableField field = OLuceneDocumentBuilder.createFieldBasedOnClassNameAndValue(className, "testFiled", OType.LONG, testVal);
      Assert.assertNotNull("Field not created", field);
    }
    catch (ClassNotFoundException | InstantiationException exc){
      exc.printStackTrace();
      Assert.assertTrue(exc.getMessage(), false);
    }
  }
  
  @Test
  public void testCreateDoublePoint(){
    String className  = DoublePoint.class.getCanonicalName();
    double testVal = 1.0;
    try{
      IndexableField field = OLuceneDocumentBuilder.createFieldBasedOnClassNameAndValue(className, "testFiled", OType.DOUBLE, testVal);
      Assert.assertNotNull("Field not created", field);
    }
    catch (ClassNotFoundException | InstantiationException exc){
      exc.printStackTrace();
      Assert.assertTrue(exc.getMessage(), false);
    }
  }
  
  @Test
  public void testCreateIntPoint(){
    String className  = IntPoint.class.getCanonicalName();
    int testVal = 1;
    try{
      IndexableField field = OLuceneDocumentBuilder.createFieldBasedOnClassNameAndValue(className, "testFiled", OType.INTEGER, testVal);
      Assert.assertNotNull("Field not created", field);
    }
    catch (ClassNotFoundException | InstantiationException exc){
      exc.printStackTrace();
      Assert.assertTrue(exc.getMessage(), false);
    }
  }
  
  @Test
  public void testCreateSortedDocValuesField(){
    String className  = SortedDocValuesField.class.getCanonicalName();
    byte[] testVal = new byte[1];
    try{
      IndexableField field = OLuceneDocumentBuilder.createFieldBasedOnClassNameAndValue(className, "testFiled", OType.BINARY, testVal);
      Assert.assertNotNull("Field not created", field);
    }
    catch (ClassNotFoundException | InstantiationException exc){
      exc.printStackTrace();
      Assert.assertTrue(exc.getMessage(), false);
    }
  }
}
