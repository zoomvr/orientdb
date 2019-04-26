package com.orientechnologies.orient.core.storage.index.hashindex.local;

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
import com.orientechnologies.orient.core.storage.index.engine.OHashTableIndexEngine;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class LocalHashTableTxIT {
  private OrientDB orientDB;

  private String dbName;

  private OAbstractPaginatedStorage storage;

  @Before
  public void before() throws Exception {
    final String buildDirectory =
        System.getProperty("buildDirectory", "./target") + File.separator + LocalHashTableTxIT.class.getSimpleName();

    dbName = "localHashTableTXTest";
    final File dbDirectory = new File(buildDirectory, dbName);
    OFileUtils.deleteRecursively(dbDirectory);

    orientDB = new OrientDB("plocal:" + buildDirectory, OrientDBConfig.defaultConfig());
    orientDB.create(dbName, ODatabaseType.PLOCAL);

    final ODatabaseSession databaseDocumentTx = orientDB.open(dbName, "admin", "admin");

    final OClass cls = databaseDocumentTx.createClass("TestClass");
    cls.createProperty("prop", OType.STRING);
    cls.createIndex("TestIndex", OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "prop");

    final OClass cls2 = databaseDocumentTx.createClass("TestClassTwo");
    cls2.createProperty("prop", OType.STRING);
    cls2.createIndex("TestIndex2", OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, "prop");

    storage = (OAbstractPaginatedStorage) (((ODatabaseInternal) databaseDocumentTx).getStorage());
    final int indexIdOne = storage.loadIndexEngine("TestIndex");
    Assert.assertTrue(storage.checkIndexEngineType(indexIdOne, OHashTableIndexEngine.class));

    final int indexIdTwo = storage.loadIndexEngine("TestIndex2");
    Assert.assertTrue(storage.checkIndexEngineType(indexIdTwo, OHashTableIndexEngine.class));

    databaseDocumentTx.command("create index TestIndex3 DICTIONARY_HASH_INDEX STRING");

    final int indexIdThree = storage.loadIndexEngine("TestIndex3");
    Assert.assertTrue(storage.checkIndexEngineType(indexIdThree, OHashTableIndexEngine.class));

    databaseDocumentTx.close();
  }

  @After
  public void afterMethod() {
    orientDB.drop(dbName);
    orientDB.close();
  }

  @Test
  public void testKeyPut() {
    final int cycles = 1_000_000;

    try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
      for (int i = 0; i < cycles; i++) {
        if (i % 2 == 0) {
          storage.setTxApprover(new GoTxApprover());
        } else {
          storage.setTxApprover(new NoGoTxApprover());
        }

        session.begin();
        try {
          for (int n = 0; n < 10; n++) {
            final String key = String.valueOf(i * 10 + n);

            final ODocument document = new ODocument("TestClass");
            document.field("prop", key);
            document.save();
          }

          session.commit();
        } catch (NoGoException e) {
          //skip
        }
      }

      final OIndexManager indexManager = session.getMetadata().getIndexManager();
      final OIndex index = indexManager.getIndex("TestIndex");

      for (int i = 0; i < cycles; i++) {
        for (int n = 0; n < 10; n++) {
          final String key = String.valueOf(i * 10 + n);

          @SuppressWarnings("unchecked")
          final Collection<ORID> rids = (Collection<ORID>) index.get(key);

          if (i % 2 == 0) {
            Assert.assertEquals(1, rids.size());
            final ORID rid = rids.iterator().next();
            final ODocument document = session.load(rid);
            Assert.assertEquals(key, document.field("prop"));
          } else {
            Assert.assertTrue(rids.isEmpty());
          }
        }
      }
    }
  }

  @Test
  public void testKeyPutTwo() {
    final int cycles = 1_000_000;

    try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
      for (int i = 0; i < cycles; i++) {
        if (i % 2 == 0) {
          storage.setTxApprover(new GoTxApprover());
        } else {
          storage.setTxApprover(new NoGoTxApprover());
        }

        session.begin();
        try {
          for (int n = 0; n < 10; n++) {
            final String key = String.valueOf(i * 10 + n);

            final ODocument document = new ODocument("TestClassTwo");
            document.field("prop", key);
            document.save();
          }

          session.commit();
        } catch (NoGoException e) {
          //skip
        }
      }

      final OIndexManager indexManager = session.getMetadata().getIndexManager();
      final OIndex index = indexManager.getIndex("TestIndex2");

      for (int i = 0; i < cycles; i++) {
        for (int n = 0; n < 10; n++) {
          final String key = String.valueOf(i * 10 + n);

          final ORID rid = (ORID) index.get(key);

          if (i % 2 == 0) {
            final ODocument document = session.load(rid);
            Assert.assertEquals(key, document.field("prop"));
          } else {
            Assert.assertNull(rid);
          }
        }
      }
    }
  }

  @Test
  public void testKeyPutNull() {
    try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
      for (int i = 0; i < 2; i++) {
        if (i % 2 == 0) {
          storage.setTxApprover(new GoTxApprover());
        } else {
          storage.setTxApprover(new NoGoTxApprover());
        }

        session.begin();
        try {
          for (int n = 0; n < 10; n++) {
            final ODocument document = new ODocument("TestClass");
            document.field("prop", (String) null);
            document.save();
          }

          session.commit();
        } catch (NoGoException e) {
          //skip
        }
      }

      final OIndexManager indexManager = session.getMetadata().getIndexManager();
      final OIndex index = indexManager.getIndex("TestIndex");

      @SuppressWarnings("unchecked")
      final Collection<ORID> rids = (Collection<ORID>) index.get(null);

      Assert.assertEquals(10, rids.size());
      for (ORID rid : rids) {
        final ODocument document = session.load(rid);
        Assert.assertNull(document.field("prop"));
      }
    }
  }

  @Test
  public void testKeyRemove() {
    final int entriesCount = 1_000_000;

    try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
      for (int i = 0; i < entriesCount; i++) {
        final ODocument document = new ODocument("TestClass");
        document.field("prop", String.valueOf(i));
        document.save();
      }

      for (int i = 0; i < entriesCount / 10; i++) {
        if (i % 2 == 0) {
          storage.setTxApprover(new GoTxApprover());
        } else {
          storage.setTxApprover(new NoGoTxApprover());
        }

        session.begin();
        try {
          for (int n = 0; n < 10; n++) {
            final String key = String.valueOf(i * 10 + n);

            try (final OResultSet resultSet = session.query("select * from TestClass where prop == ?", key)) {
              while (resultSet.hasNext()) {
                final OResult result = resultSet.next();
                final ODocument document = (ODocument) result.getRecord().orElseThrow(AssertionError::new);
                document.delete();
              }
            }
          }

          session.commit();
        } catch (NoGoException e) {
          //skip
        }
      }

      final OIndexManager indexManager = session.getMetadata().getIndexManager();
      final OIndex index = indexManager.getIndex("TestIndex");

      for (int i = 0; i < entriesCount / 10; i++) {
        for (int n = 0; n < 10; n++) {
          final String key = String.valueOf(i * 10 + n);

          @SuppressWarnings("unchecked")
          final Collection<ORID> rids = (Collection<ORID>) index.get(key);
          if (i % 2 == 1) {
            Assert.assertEquals(1, rids.size());
            final ORID rid = rids.iterator().next();
            final ODocument document = session.load(rid);
            Assert.assertEquals(key, document.field("prop"));
          } else {
            Assert.assertTrue(rids.isEmpty());
          }
        }
      }
    }
  }

  @Test
  public void testKeyRemoveTwo() {
    final int entriesCount = 1_000_000;

    try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
      for (int i = 0; i < entriesCount; i++) {
        final ODocument document = new ODocument("TestClassTwo");
        document.field("prop", String.valueOf(i));
        document.save();
      }

      for (int i = 0; i < entriesCount / 10; i++) {
        if (i % 2 == 0) {
          storage.setTxApprover(new GoTxApprover());
        } else {
          storage.setTxApprover(new NoGoTxApprover());
        }

        session.begin();
        try {
          for (int n = 0; n < 10; n++) {
            final String key = String.valueOf(i * 10 + n);

            try (final OResultSet resultSet = session.query("select * from TestClassTwo where prop == ?", key)) {
              while (resultSet.hasNext()) {
                final OResult result = resultSet.next();
                final ODocument document = (ODocument) result.getRecord().orElseThrow(AssertionError::new);
                document.delete();
              }
            }
          }

          session.commit();
        } catch (NoGoException e) {
          //skip
        }
      }

      final OIndexManager indexManager = session.getMetadata().getIndexManager();
      final OIndex index = indexManager.getIndex("TestIndex2");

      for (int i = 0; i < entriesCount / 10; i++) {
        for (int n = 0; n < 10; n++) {
          final String key = String.valueOf(i * 10 + n);

          final ORID rid = (ORID) index.get(key);
          if (i % 2 == 1) {
            final ODocument document = session.load(rid);
            Assert.assertEquals(key, document.field("prop"));
          } else {
            Assert.assertNull(rid);
          }
        }
      }
    }
  }

  @Test
  public void testKeyRemoveNull() {
    try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {

      final ODocument document = new ODocument("TestClassTwo");
      document.field("prop", (String) null);
      document.save();

      storage.setTxApprover(new NoGoTxApprover());
      session.begin();
      try {
        document.delete();
        session.commit();
      } catch (NoGoException e) {
        //skip
      }

      final OIndexManager indexManager = session.getMetadata().getIndexManager();
      final OIndex index = indexManager.getIndex("TestIndex2");
      Assert.assertEquals(document.getIdentity(), index.get(null));
    }
  }

  @Test
  public void testKeyRemoveNullDuplicates() {
    try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
      final List<ORID> savedRids = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        final ODocument document = new ODocument("TestClass");
        document.field("prop", (String) null);
        document.save();

        savedRids.add(document.getIdentity());
      }

      session.begin();
      storage.setTxApprover(new NoGoTxApprover());
      try {
        for (ORID rid : savedRids) {
          final ODocument document = session.load(rid);
          document.delete();
        }

        session.commit();
        Assert.fail();
      } catch (NoGoException e) {
        //skip
      }

      final OIndexManager indexManager = session.getMetadata().getIndexManager();
      final OIndex index = indexManager.getIndex("TestIndex");

      @SuppressWarnings("unchecked")
      final Collection<ORID> rids = (Collection<ORID>) index.get(null);

      Assert.assertEquals(10, rids.size());
      for (ORID rid : rids) {
        final ODocument document = session.load(rid);
        Assert.assertNull(document.field("prop"));
      }
    }
  }

  @Test
  public void testKeyPutRemoveDuplicates() {
    final int entriesCount = 1_000_000;

    try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
      for (int i = 0; i < entriesCount / 10; i++) {
        for (int n = 0; n < 10; n++) {
          final ODocument document = new ODocument("TestClass");
          document.field("prop", String.valueOf(i));
          document.save();
        }
      }

      for (int i = 0; i < entriesCount / 10; i++) {
        if (i % 2 == 0) {
          storage.setTxApprover(new GoTxApprover());
        } else {
          storage.setTxApprover(new NoGoTxApprover());
        }

        session.begin();
        try {
          final String key = String.valueOf(i);

          try (final OResultSet resultSet = session.query("select * from TestClass where prop == ?", key)) {
            while (resultSet.hasNext()) {
              final OResult result = resultSet.next();
              final ODocument document = (ODocument) result.getRecord().orElseThrow(AssertionError::new);
              document.delete();
            }
          }

          session.commit();
        } catch (NoGoException e) {
          //skip
        }
      }

      final OIndexManager indexManager = session.getMetadata().getIndexManager();
      final OIndex index = indexManager.getIndex("TestIndex");

      for (int i = 0; i < entriesCount / 10; i++) {
        final String key = String.valueOf(i);

        @SuppressWarnings("unchecked")
        final Collection<ORID> rids = (Collection<ORID>) index.get(key);
        if (i % 2 == 1) {
          Assert.assertEquals(10, rids.size());

          for (final ORID rid : rids) {
            final ODocument document = session.load(rid);
            Assert.assertEquals(key, document.field("prop"));
          }
        } else {
          Assert.assertTrue(rids.isEmpty());
        }
      }
    }
  }

  @Test
  public void testUpdateValue() {
    final int entriesCount = 1_000_000;

    try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
      final List<ORID> savedRids = new ArrayList<>();

      final OIndexManager indexManager = session.getMetadata().getIndexManager();
      final OIndex index = indexManager.getIndex("TestIndex3");

      for (int i = 0; i < entriesCount; i++) {
        final ODocument document = new ODocument("TestClass");
        document.save();

        savedRids.add(document.getIdentity());
        index.put(String.valueOf(i), document.getIdentity());
      }

      for (int i = 0; i < entriesCount / 10; i++) {
        if (i % 2 == 0) {
          storage.setTxApprover(new GoTxApprover());
        } else {
          storage.setTxApprover(new NoGoTxApprover());
        }

        session.begin();
        try {
          for (int n = 0; n < 10; n++) {
            final int k = i * 10 + n;
            final String key = String.valueOf(k);
            index.put(key, savedRids.get(savedRids.size() - k - 1));
          }
          session.commit();
        } catch (NoGoException e) {
          //skip
        }
      }

      for (int i = 0; i < entriesCount / 10; i++) {
        for (int n = 0; n < 10; n++) {
          final int k = i * 10 + n;
          final String key = String.valueOf(k);

          final ORID orid = (ORID) index.get(key);

          if (i % 2 == 0) {
            Assert.assertEquals(savedRids.get(savedRids.size() - k - 1), orid);
          } else {
            Assert.assertEquals(savedRids.get(k), orid);
          }
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
