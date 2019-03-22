package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OHighLevelException;
import com.orientechnologies.orient.core.storage.cluster.OPaginatedCluster;

public class ONotEmptyClusterCanNotBeDeletedException extends ODurableComponentException implements OHighLevelException {
  public ONotEmptyClusterCanNotBeDeletedException(final ONotEmptyClusterCanNotBeDeletedException exception) {
    super(exception);
  }

  public ONotEmptyClusterCanNotBeDeletedException(final String message, final OPaginatedCluster component) {
    super(message, component);
  }
}
