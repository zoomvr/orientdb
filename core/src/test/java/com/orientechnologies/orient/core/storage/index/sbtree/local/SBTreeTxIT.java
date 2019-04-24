package com.orientechnologies.orient.core.storage.index.sbtree.local;

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
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.txapprover.OTxApprover;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Collection;

public class SBTreeTxIT {
  private OrientDB orientDB;

  private String dbName;

  private OAbstractPaginatedStorage storage;

  @Before
  public void before() throws Exception {
    final String buildDirectory =
        System.getProperty("buildDirectory", "./target") + File.separator + SBTreeTxIT.class.getSimpleName();

    dbName = "localSBTreeTXTest";
    final File dbDirectory = new File(buildDirectory, dbName);
    OFileUtils.deleteRecursively(dbDirectory);

    orientDB = new OrientDB("plocal:" + buildDirectory, OrientDBConfig.defaultConfig());
    orientDB.create(dbName, ODatabaseType.PLOCAL);

    final ODatabaseSession databaseDocumentTx = orientDB.open(dbName, "admin", "admin");
    final OClass cls = databaseDocumentTx.createClass("TestClass");
    cls.createProperty("prop", OType.STRING);
    cls.createIndex("TestIndex", OClass.INDEX_TYPE.NOTUNIQUE.toString(), null, null, "SBTREE", new String[] { "prop" });

    storage = (OAbstractPaginatedStorage) (((ODatabaseInternal) databaseDocumentTx).getStorage());
    final int indexId = storage.loadIndexEngine("TestIndex");
    storage.checkIndexEngineType(indexId, OSBTree.class);
    databaseDocumentTx.close();
  }

  @After
  public void afterMethod() {
    orientDB.drop(dbName);
    orientDB.close();
  }

  @Test
  public void testKeyPut() {
    final int cycles = 100_000;

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
