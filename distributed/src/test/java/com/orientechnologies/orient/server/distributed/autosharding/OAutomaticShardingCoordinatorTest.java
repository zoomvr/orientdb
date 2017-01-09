/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *  
 */
package com.orientechnologies.orient.server.distributed.autosharding;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.OModifiableDistributedConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

/**
 * Unit tests for the automatic sharding coordinator component.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class OAutomaticShardingCoordinatorTest {
  @Test
  public void testRebalance() {
    OModifiableDistributedConfiguration cfg = new OModifiableDistributedConfiguration(
        new ODocument().fromJSON("{'classes':{'*':{'copies':3 },'Person':{'copies':2 }}, 'clusters':{'*':{'servers':['<NEW_NODE>']}}}", "noMap"));

    final OAutomaticShardingCoordinator component = new OAutomaticShardingCoordinator();

    final Set<String> clusters = new HashSet<String>();
    clusters.add("person");
    clusters.add("person_1");
    clusters.add("person_2");
    clusters.add("person_3");
    clusters.add("person_4");
    clusters.add("person_5");
    clusters.add("person_6");
    clusters.add("person_7");

    final Set<String> servers = new HashSet<String>();
    servers.add("europe");
    servers.add("usa");
    servers.add("asia");

    final int lastCfg = cfg.getVersion();

    component.rebalanceClass(cfg, "Person", clusters, servers);

    checkAssignmentIsCorrect(cfg, clusters, servers, lastCfg, 2);
  }

  @Test
  public void testCfgCopies() {
    ODistributedConfiguration cfg = new ODistributedConfiguration(
        new ODocument().fromJSON("{'classes':{'*':{'copies':3 },'Person':{'copies':2 }}}", "noMap"));

    Assert.assertEquals(2, cfg.getClassCopies("Person"));
    Assert.assertEquals(3, cfg.getClassCopies("Dunno")); // DEFAULT * EXPECTED

    cfg = new ODistributedConfiguration(new ODocument().fromJSON("{'classes':{'Person':{'copies':2 }}}", "noMap"));

    Assert.assertEquals(2, cfg.getClassCopies("Person"));
    Assert.assertEquals(0, cfg.getClassCopies("Dunno")); // NO DEFAULT *

    cfg = new ODistributedConfiguration(new ODocument().fromJSON("{}", "noMap"));

    Assert.assertEquals(0, cfg.getClassCopies("Person")); // NO DEFINITION
    Assert.assertEquals(0, cfg.getClassCopies("Dunno")); // NO DEFAULT *
  }

  @Test
  public void testCfgClasses() {
    ODistributedConfiguration cfg = new ODistributedConfiguration(
        new ODocument().fromJSON("{'classes':{'*':{'copies':3 },'Person':{'copies':2 }}}", "noMap"));

    Assert.assertEquals(2, cfg.getClasses().size());
    Assert.assertTrue(cfg.getClasses().contains("Person"));
    Assert.assertTrue(cfg.getClasses().contains("*"));
    Assert.assertFalse(cfg.getClasses().contains("Dunno"));

    cfg = new ODistributedConfiguration(new ODocument().fromJSON("{'classes':{}}", "noMap"));

    Assert.assertEquals(0, cfg.getClasses().size());
    Assert.assertFalse(cfg.getClasses().contains("Person"));
    Assert.assertFalse(cfg.getClasses().contains("*"));
    Assert.assertFalse(cfg.getClasses().contains("Dunno"));
  }

  protected void checkAssignmentIsCorrect(OModifiableDistributedConfiguration cfg, Set<String> clusters, Set<String> servers,
      int lastCfg, int copies) {
    Assert.assertTrue(cfg.getVersion() > lastCfg);

    final Set<String> shardedServers = cfg.getServers(clusters);
    Assert.assertEquals(servers.size(), shardedServers.size());

    final Map<String, Set<String>> mapServerClusters = new HashMap<String, Set<String>>();

    for (String cl : clusters) {
      final List<String> clServers = cfg.getServers(cl, null);
      Assert.assertEquals(copies, clServers.size());

      for (String s : clServers) {
        Set<String> mapClusters = mapServerClusters.get(s);
        if (mapClusters == null) {
          mapClusters = new HashSet<String>();
          mapServerClusters.put(s, mapClusters);
        }
        mapClusters.add(cl);
      }
    }

    Assert.assertEquals(servers.size(), mapServerClusters.size());

    for (Map.Entry<String, Set<String>> entry : mapServerClusters.entrySet()) {
      Assert.assertEquals(copies, entry.getValue().size());
    }
  }
}
