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

import com.orientechnologies.orient.server.distributed.OModifiableDistributedConfiguration;

import java.util.*;

/**
 * Coordinates the shards in a class.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class OAutomaticShardingCoordinator {
  /**
   * Re-balances a class.
   *
   * @param cfg       Modifiable distributed configuration
   * @param className name of the class
   * @param clusters  clusters part of the class
   * @param servers   available servers
   */
  public void rebalanceClass(final OModifiableDistributedConfiguration cfg, final String className, final Set<String> clusters,
      final Set<String> servers) {
    // CREATE THE MAP SERVER/CLUSTERS
    final Map<String, Set<String>> mapServerClusters = new HashMap<String, Set<String>>();
    for (String cl : clusters) {
      final List<String> clServers = cfg.getServers(cl, null);

      for (String s : clServers) {
        Set<String> mapClusters = mapServerClusters.get(s);
        if (mapClusters == null) {
          mapClusters = new HashSet<String>();
          mapServerClusters.put(s, mapClusters);
        }
        mapClusters.add(cl);
      }
    }

    // 8 CLUSTERS:
    //-------------
    // 1 server: a[0,1,2,3,4,5,6,7]
    // 2 servers: a[0,1,2,3],b[4,5,6,7]
    // 3 servers: a[0,1,2],b[3,4,5],c[6,7]
    // 4 servers: a[0,1],b[2,3],c[4,5],d[6,7]

    final Set<String> missingServers = new HashSet<String>();
    for (String s : mapServerClusters.keySet())
      if (!servers.contains(s))
        missingServers.add(s);

    final Set<String> newServers = new HashSet<String>();
    for (String s : servers)
      if (!mapServerClusters.containsKey(s))
        newServers.add(s);

    if (!missingServers.isEmpty()) {
      for (String s : missingServers) {
        final Set<String> missingCluster = mapServerClusters.get(s);
      }
    }
  }
}
