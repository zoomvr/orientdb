package com.orientechnologies.orient.core.storage.cluster.v0;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.storage.cluster.LocalPaginatedClusterAbstract;
import com.orientechnologies.orient.core.storage.cluster.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import org.junit.BeforeClass;

import java.io.File;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 26.03.13
 */

public class LocalPaginatedClusterV0TestIT extends LocalPaginatedClusterAbstract {
  @BeforeClass
  public static void beforeClass() {
    buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null || buildDirectory.isEmpty())
      buildDirectory = ".";

    buildDirectory += "/localPaginatedClusterTest";

    OFileUtils.deleteRecursively(new File(buildDirectory));

    dbName = LocalPaginatedClusterV0TestIT.class.getSimpleName();

    orientDB = new OrientDB("embedded:" + buildDirectory,
        OrientDBConfig.builder().addConfig(OGlobalConfiguration.STORAGE_CLUSTER_VERSION, 0).build());
    orientDB.create(dbName, ODatabaseType.PLOCAL);
    try (ODatabaseSession databaseSession = orientDB.open(dbName, "admin", "admin")) {
      final ODatabaseInternal databaseInternal = (ODatabaseInternal) databaseSession;
      final int clusterId = databaseInternal.addCluster("paginatedClusterTest");
      storage = (OAbstractPaginatedStorage) databaseInternal.getStorage();
      paginatedCluster = (OPaginatedCluster) storage.getClusterById(clusterId);
    }
  }

  public static void afterClass() {
    orientDB.close();
  }

}
