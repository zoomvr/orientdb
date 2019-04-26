package com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v2;

import com.orientechnologies.common.exception.OHighLevelException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.txapprover.OTxApprover;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OCellBTreeMultiValueV2TxIT {
  private OrientDB orientDB;

  private String dbName;

  private OAbstractPaginatedStorage storage;

  @Before
  public void before() throws Exception {
    final String buildDirectory =
        System.getProperty("buildDirectory", "./target") + File.separator + OCellBTreeMultiValueV2TxIT.class.getSimpleName();

    dbName = "localMultiValueCellBTreeTXTest";
    final File dbDirectory = new File(buildDirectory, dbName);
    OFileUtils.deleteRecursively(dbDirectory);

    orientDB = new OrientDB("plocal:" + buildDirectory, OrientDBConfig.defaultConfig());
    orientDB.create(dbName, ODatabaseType.PLOCAL);

    final ODatabaseSession databaseDocumentTx = orientDB.open(dbName, "admin", "admin");
    final OClass cls = databaseDocumentTx.createClass("TestClass");
    cls.createProperty("prop", OType.STRING);
    cls.createIndex("TestIndex", OClass.INDEX_TYPE.NOTUNIQUE, "prop");

    storage = (OAbstractPaginatedStorage) (((ODatabaseInternal) databaseDocumentTx).getStorage());
    final int indexId = storage.loadIndexEngine("TestIndex");
    Assert.assertTrue(storage.checkIndexEngineType(indexId, OCellBTreeMultiValueV2.class));
    databaseDocumentTx.close();
  }

  @After
  public void afterMethod() {
    orientDB.drop(dbName);
    orientDB.close();
  }

  @Test
  public void testPutNullKey() {
    final int itemsCount = 64_000;

    try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
      final OIndexManager indexManager = session.getMetadata().getIndexManager();

      final List<ORID> savedRids = new ArrayList<>();

      for (int i = 0; i < itemsCount; i++) {
        session.begin();
        try {
          final ODocument document = new ODocument("TestClass");
          document.field("prop", (Object) null);
          document.save();

          if (i % 2 == 0) {
            storage.setTxApprover(new GoTxApprover());
            savedRids.add(document.getIdentity());
          } else {
            storage.setTxApprover(new NoGoTxApprover());
          }

          session.commit();
        } catch (NoGoException e) {
          //skip
        }
      }

      final OIndex index = indexManager.getIndex("TestIndex");

      @SuppressWarnings("unchecked")
      final List<ORID> result = (List<ORID>) index.get(null);

      Assert.assertEquals(savedRids.size(), result.size());
      Set<ORID> resultSet = new HashSet<>(result);

      for (ORID savedRid : savedRids) {
        Assert.assertTrue(resultSet.contains(savedRid));
      }
    }
  }

  @Test
  public void testKeyPutSameKey() {
    final int itemsCount = 1_000_000;
    final String key = "test_key";

    try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
      final OIndexManager indexManager = session.getMetadata().getIndexManager();

      final List<ORID> savedRids = new ArrayList<>();

      for (int i = 0; i < itemsCount; i++) {
        session.begin();
        try {
          final ODocument document = new ODocument("TestClass");
          document.field("prop", key);
          document.save();

          if (i % 2 == 0) {
            storage.setTxApprover(new GoTxApprover());
            savedRids.add(document.getIdentity());
          } else {
            storage.setTxApprover(new NoGoTxApprover());
          }

          session.commit();
        } catch (NoGoException e) {
          //skip
        }
      }

      final OIndex index = indexManager.getIndex("TestIndex");

      @SuppressWarnings("unchecked")
      final List<ORID> result = (List<ORID>) index.get(key);

      Assert.assertEquals(savedRids.size(), result.size());
      Set<ORID> resultSet = new HashSet<>(result);

      for (ORID savedRid : savedRids) {
        Assert.assertTrue(resultSet.contains(savedRid));
      }
    }
  }

  @Test
  public void testKeyPutTenSameKeys() {
    final int itemsCount = 1_000_000;

    final Map<String, List<OIdentifiable>> keyRecordMap = new HashMap<>();

    final String[] keys = new String[10];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = "test_key_" + i;

      keyRecordMap.put(keys[i], new ArrayList<>());
    }

    try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
      for (int i = 0; i < itemsCount; i++) {

        session.begin();
        try {

          if (i % 2 == 0) {
            storage.setTxApprover(new GoTxApprover());
          } else {
            storage.setTxApprover(new NoGoTxApprover());
          }

          for (String key : keys) {
            final ODocument document = new ODocument("TestClass");
            document.field("prop", key);
            document.save();

            if (i % 2 == 0) {
              keyRecordMap.get(key).add(document.getIdentity());
            }
          }

          session.commit();
        } catch (NoGoException e) {
          //skip
        }
      }

      final OIndexManager indexManager = session.getMetadata().getIndexManager();
      final OIndex index = indexManager.getIndex("TestIndex");
      for (String key : keys) {
        @SuppressWarnings("unchecked")
        final Collection<ORID> result = (Collection<ORID>) index.get(key);

        final List<OIdentifiable> savedRids = keyRecordMap.get(key);
        Assert.assertEquals(savedRids.size(), result.size());

        final Set<ORID> resultSet = new HashSet<>(result);

        for (OIdentifiable savedRid : savedRids) {
          Assert.assertTrue(resultSet.contains(savedRid.getIdentity()));
        }
      }
    }
  }

  @Test
  public void testKeyPut() {
    final int keysCount = 1_000_000;

    String lastKey = null;

    try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
      final OIndexManager indexManager = session.getMetadata().getIndexManager();
      final OIndex index = indexManager.getIndex("TestIndex");

      for (int i = 0; i < keysCount / 10; i++) {
        session.begin();

        if (i % 2 == 0) {
          storage.setTxApprover(new GoTxApprover());
        } else {
          storage.setTxApprover(new NoGoTxApprover());
        }
        try {

          for (int n = 0; n < 10; n++) {
            final String key = Integer.toString(i * 10 + n);

            final ODocument document = new ODocument("TestClass");
            document.field("prop", key);
            document.save();

            if (i % 2 == 0) {
              if (lastKey == null) {
                lastKey = key;
              } else if (key.compareTo(lastKey) > 0) {
                lastKey = key;
              }
            }

            if ((i * 10 + n) % 100_000 == 0) {
              System.out.printf("%d items loaded out of %d%n", i * 10 + n, keysCount);
            }
          }
          session.commit();
        } catch (NoGoException e) {
          //skip
        }
      }

      for (int i = 0; i < keysCount / 10; i++) {
        for (int n = 0; n < 10; n++) {
          final String key = String.valueOf(i * 10 + n);

          @SuppressWarnings("unchecked")
          final List<ORID> result = (List<ORID>) index.get(key);
          if (i % 2 == 0) {
            Assert.assertEquals(1, result.size());
            final ODocument document = session.load(result.get(0));
            Assert.assertEquals(key, document.field("prop"));

            if ((i * 10 + n) % 100_000 == 0) {
              System.out.printf("%d items tested out of %d%n", i, keysCount);
            }
          } else {
            Assert.assertTrue(result.isEmpty());
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
