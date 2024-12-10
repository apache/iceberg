/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.rest;

import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.rules.ExternalResource;

/**
 * This class is to make the {@link RESTCatalogServer} usable for JUnit4 in a similar way to {@link
 * RESTServerExtension}.
 */
public class RESTServerRule extends ExternalResource {
  public static final String FREE_PORT = "0";

  private volatile RESTCatalogServer localServer;
  private RESTCatalog client;
  private final Map<String, String> config;

  public RESTServerRule() {
    config = Maps.newHashMap();
  }

  public RESTServerRule(Map<String, String> config) {
    Map<String, String> conf = Maps.newHashMap(config);
    if (conf.containsKey(RESTCatalogServer.REST_PORT)
        && conf.get(RESTCatalogServer.REST_PORT).equals(FREE_PORT)) {
      conf.put(RESTCatalogServer.REST_PORT, String.valueOf(RCKUtils.findFreePort()));
    }
    this.config = conf;
  }

  public Map<String, String> config() {
    return config;
  }

  public RESTCatalog client() {
    if (null == client) {
      try {
        maybeInitClientAndServer();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    return client;
  }

  public String uri() {
    return client().properties().get(CatalogProperties.URI);
  }

  private void maybeInitClientAndServer() throws Exception {
    if (null == localServer) {
      synchronized (this) {
        if (null == localServer) {
          this.localServer = new RESTCatalogServer(config);
          this.localServer.start(false);
          this.client = RCKUtils.initCatalogClient(config);
        }
      }
    }
  }

  @Override
  protected void before() throws Throwable {
    maybeShutdownClientAndServer();
    maybeInitClientAndServer();
  }

  @Override
  protected void after() {
    maybeShutdownClientAndServer();
  }

  private void maybeShutdownClientAndServer() {
    try {
      if (localServer != null) {
        localServer.stop();
        localServer = null;
      }
      if (client != null) {
        client.close();
        client = null;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
