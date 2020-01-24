package com.orientechnologies.orient.server.network;

import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;

public class OServerNetworkListenerManaged extends OServerNetworkListener {

  public OServerNetworkListenerManaged(OServer iServer, OServerSocketFactory iSocketFactory, String iHostName,
      String iHostPortRange, String iProtocolName, Class<? extends ONetworkProtocol> iProtocol,
      OServerParameterConfiguration[] iParameters, OServerCommandConfiguration[] iCommands) {
    super(iServer, iSocketFactory, iHostName, iHostPortRange, iProtocolName, iProtocol, iParameters, iCommands);
  }
}
