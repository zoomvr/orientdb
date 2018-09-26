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
package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OVertex;
import java.util.List;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * @author mdjurovi
 */
public class ODocumentTraverseTest {
  
  private static final String dbName                    = ODocumentTraverseTest.class.getSimpleName();
  private static final String defaultDbAdminCredentials = "admin";
  
  @Test
  public void testDepthFirst(){
    
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass vClass = db.createVertexClass("Vertice");
      OClass eClass = db.createEdgeClass("EdgeClass");
      
      //make some simple tree
      OVertex one = db.newVertex(vClass);
      ((ODocument)one.getRecord()).field("name", "one");
      OVertex two = db.newVertex(vClass);
      ((ODocument)two.getRecord()).field("name", "two");
      OVertex three = db.newVertex(vClass);
      ((ODocument)three.getRecord()).field("name", "three");
      OVertex four = db.newVertex(vClass);
      ((ODocument)four.getRecord()).field("name", "four");
      OVertex five = db.newVertex(vClass);
      ((ODocument)five.getRecord()).field("name", "five");
      OVertex six = db.newVertex(vClass);
      ((ODocument)six.getRecord()).field("name", "six");
      OVertex seven = db.newVertex(vClass);
      ((ODocument)seven.getRecord()).field("name", "seven");
      
      db.newEdge(one, two, eClass);
      db.newEdge(one, three, eClass);
      db.newEdge(two, four, eClass);
      db.newEdge(two, five, eClass);
      db.newEdge(three, six, eClass);
      db.newEdge(three, seven, eClass);
      
      ODocument doc = one.getRecord();
      ODocument.EdgeDirection ed = new ODocument.EdgeDirection();
      ed.direction = ODirection.OUT;
      ed.edgeName = "EdgeClass";
      List<ODocument.TraverseResult> res = doc.traverse(ODocument.TraverseStrategy.DEPTH_FIRST, ed);
      assertEquals(7, res.size());
      
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
    
  }
  
  @Test
  public void testBreadthFirst(){
    
    ODatabaseSession db = null;
    OrientDB odb = null;
    try {
      odb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
      odb.createIfNotExists(dbName, ODatabaseType.MEMORY);
      db = odb.open(dbName, defaultDbAdminCredentials, defaultDbAdminCredentials);

      OClass vClass = db.createVertexClass("Vertice");
      OClass eClass = db.createEdgeClass("EdgeClass");
      
      //make some simple tree
      OVertex one = db.newVertex(vClass);
      ((ODocument)one.getRecord()).field("name", "one");
      OVertex two = db.newVertex(vClass);
      ((ODocument)two.getRecord()).field("name", "two");
      OVertex three = db.newVertex(vClass);
      ((ODocument)three.getRecord()).field("name", "three");
      OVertex four = db.newVertex(vClass);
      ((ODocument)four.getRecord()).field("name", "four");
      OVertex five = db.newVertex(vClass);
      ((ODocument)five.getRecord()).field("name", "five");
      OVertex six = db.newVertex(vClass);
      ((ODocument)six.getRecord()).field("name", "six");
      OVertex seven = db.newVertex(vClass);
      ((ODocument)seven.getRecord()).field("name", "seven");
      
      db.newEdge(one, two, eClass);
      db.newEdge(one, three, eClass);
      db.newEdge(two, four, eClass);
      db.newEdge(two, five, eClass);
      db.newEdge(three, six, eClass);
      db.newEdge(three, seven, eClass);
      
      ODocument doc = one.getRecord();
      ODocument.EdgeDirection ed = new ODocument.EdgeDirection();
      ed.direction = ODirection.OUT;
      ed.edgeName = "EdgeClass";
      List<ODocument.TraverseResult> res = doc.traverse(ODocument.TraverseStrategy.BREADTH_FIRST, ed);
      assertEquals(7, res.size());
      
    } finally {
      if (db != null)
        db.close();
      if (odb != null) {
        odb.drop(dbName);
        odb.close();
      }
    }
    
  }
  
}
