package com.orientechnologies.orient.core.storage.index.sbtree.singlevalue;

import com.orientechnologies.common.exception.OHighLevelException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.txapprover.OTxApprover;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class OCellBtreeSingleValueTxIT {
  private OrientDB orientDB;

  private String dbName;

  private OAbstractPaginatedStorage storage;

  @Before
  public void before() throws Exception {
    final String buildDirectory =
        System.getProperty("buildDirectory", "./target") + File.separator + OCellBtreeSingleValueTxIT.class.getSimpleName();

    dbName = "localSingleCellBTreeTXTest";
    final File dbDirectory = new File(buildDirectory, dbName);
    OFileUtils.deleteRecursively(dbDirectory);

    orientDB = new OrientDB("plocal:" + buildDirectory, OrientDBConfig.defaultConfig());
    orientDB.create(dbName, ODatabaseType.PLOCAL);

    final ODatabaseSession databaseDocumentTx = orientDB.open(dbName, "admin", "admin");
    final OClass cls = databaseDocumentTx.createClass("TestClass");
    cls.createProperty("prop", OType.STRING);
    cls.createIndex("TestIndex", OClass.INDEX_TYPE.UNIQUE, "prop");

    storage = (OAbstractPaginatedStorage) (((ODatabaseInternal) databaseDocumentTx).getStorage());
    final int indexId = storage.loadIndexEngine("TestIndex");
    Assert.assertTrue(storage.checkIndexEngineType(indexId, OCellBTreeSingleValue.class));
    databaseDocumentTx.close();
  }

  @After
  public void afterMethod() {
    orientDB.drop(dbName);
    orientDB.close();
  }

  @Test
  public void testKeyPut() {
    final int keysCount = 1_000_000;

    String lastKey = null;

    try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
      final OIndexManager indexManager = session.getMetadata().getIndexManager();
      @SuppressWarnings("unchecked")
      final OIndex<ORID> index = (OIndex<ORID>) indexManager.getIndex("TestIndex");
      for (int i = 0; i < keysCount; i++) {

        final String key = Integer.toString(i);

        session.begin();
        try {
          if (i % 2 == 0) {
            storage.setTxApprover(new NoGoTxApprover());
          } else {
            storage.setTxApprover(new GoTxApprover());
          }

          final ODocument document = new ODocument("TestClass");
          document.field("prop", key);
          document.save();

          if (i % 100_000 == 0) {
            System.out.printf("%d items loaded out of %d%n", i, keysCount);
          }

          if (i % 2 == 1) {
            if (lastKey == null) {
              lastKey = key;
            } else if (key.compareTo(lastKey) > 0) {
              lastKey = key;
            }
          }
          session.commit();
        } catch (NoGoException e) {
          //do nothing
        }

        if (i > 0) {
          Assert.assertEquals("1", index.getFirstKey());
          Assert.assertEquals(lastKey, index.getLastKey());
        }
      }

      for (int i = 0; i < keysCount; i++) {
        final String key = Integer.toString(i);
        final ORID rid = index.get(key);
        if (i % 2 == 0) {
          Assert.assertNull(rid);
        } else {
          Assert.assertNotNull(rid);
          final ODocument document = session.load(rid);

          Assert.assertEquals(i + " key is absent", key, document.field("prop"));
        }

        if (i % 100_000 == 0) {
          System.out.printf("%d items tested out of %d%n", i, keysCount);
        }
      }
    }
  }

  @Test
  public void testKeyDelete() {
    final int keysCount = 1_000_000;

    try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
      for (int i = 0; i < keysCount; i++) {
        final ODocument document = new ODocument("TestClass");
        document.field("prop", Integer.toString(i));
        document.save();
      }

      for (int i = 0; i < keysCount; i++) {
        session.begin();
        try {
          if (i % 3 == 0) {
            storage.setTxApprover(new GoTxApprover());
          } else {
            storage.setTxApprover(new NoGoTxApprover());
          }

          try (OResultSet resultSet = session.query("select * from TestClass where prop = ?", Integer.toString(i))) {
            Assert.assertTrue(resultSet.hasNext());
            final OResult result = resultSet.next();
            final ODocument document = (ODocument) result.getRecord().orElseThrow(RuntimeException::new);
            document.delete();
          }

          session.commit();
        } catch (NoGoException e) {
          //skip
        }
      }

      final OIndexManager indexManager = session.getMetadata().getIndexManager();
      @SuppressWarnings("unchecked")
      final OIndex<ORID> index = (OIndex<ORID>) indexManager.getIndex("TestIndex");

      for (int i = 0; i < keysCount; i++) {
        if (i % 3 == 0) {
          Assert.assertNull(index.get(Integer.toString(i)));
        } else {
          final String key = Integer.toString(i);
          final ORID rid = index.get(key);
          final ODocument document = session.load(rid);

          Assert.assertEquals(key, document.field("prop"));
        }
      }
    }
  }

  @Test
  public void testKeyAddDelete() throws Exception {
    final int keysCount = 1_000_000;

    try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
      for (int i = 0; i < keysCount; i++) {
        final ODocument document = new ODocument("TestClass");
        document.field("prop", Integer.toString(i));
        document.save();
      }

      for (int i = 0; i < keysCount; i++) {
        session.begin();
        try {
          if (i % 3 == 0) {
            storage.setTxApprover(new GoTxApprover());
          } else {
            storage.setTxApprover(new NoGoTxApprover());
          }

          try (OResultSet resultSet = session.query("select * from TestClass where prop = ?", Integer.toString(i))) {
            Assert.assertTrue(resultSet.hasNext());
            final OResult result = resultSet.next();
            final ODocument document = (ODocument) result.getRecord().orElseThrow(RuntimeException::new);
            document.delete();
          }

          final ODocument document = new ODocument("TestClass");
          document.field("prop", Integer.toString(i + keysCount));
          document.save();

          session.commit();
        } catch (NoGoException e) {
          //skip
        }
      }

      final OIndexManager indexManager = session.getMetadata().getIndexManager();
      @SuppressWarnings("unchecked")
      final OIndex<ORID> index = (OIndex<ORID>) indexManager.getIndex("TestIndex");

      for (int i = 0; i < keysCount; i++) {
        if (i % 3 == 0) {
          Assert.assertNull(index.get(Integer.toString(i)));

          final String key = Integer.toString(i + keysCount);
          final ORID rid = index.get(key);
          final ODocument document = session.load(rid);

          Assert.assertEquals(key, document.field("prop"));
        } else {
          Assert.assertNull(index.get(Integer.toString(i + keysCount)));

          final String key = Integer.toString(i);
          final ORID rid = index.get(key);
          final ODocument document = session.load(rid);

          Assert.assertEquals(key, document.field("prop"));
        }
      }
    }

  }

  private static class GoTxApprover implements OTxApprover {
    @Override
    public void approveTx() {

    }
  }

  private static class NoGoTxApprover implements OTxApprover {
    @Override
    public void approveTx() {
      throw new NoGoException();
    }
  }

  private static class NoGoException extends RuntimeException implements OHighLevelException {
    NoGoException() {
    }
  }
}


