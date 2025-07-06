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

import java.net.BindException;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class RESTServerExtension implements BeforeAllCallback, AfterAllCallback {
  // if the caller explicitly wants the server to start on port 0, it means the caller wants to
  // launch on a free port
  public static final String FREE_PORT = "0";

  private RESTCatalogServer localServer;
  private RESTCatalog client;
  private final Map<String, String> config;
  private final boolean findFreePort;

  public RESTServerExtension() {
    config = Maps.newHashMap();
    findFreePort = false;
  }

  public RESTServerExtension(Map<String, String> config) {
    Map<String, String> conf = Maps.newHashMap(config);
    findFreePort =
        conf.containsKey(RESTCatalogServer.REST_PORT)
            && conf.get(RESTCatalogServer.REST_PORT).equals(FREE_PORT);
    this.config = conf;
  }

  public Map<String, String> config() {
    return config;
  }

  public RESTCatalog client() {
    return client;
  }

  @Override
  public void beforeAll(ExtensionContext extensionContext) throws Exception {
    if (Boolean.parseBoolean(
        extensionContext.getConfigurationParameter(RCKUtils.RCK_LOCAL).orElse("true"))) {
      int maxAttempts = 10;
      for (int i = 0; i < maxAttempts; i++) {
        try {
          if (findFreePort) {
            config.put(RESTCatalogServer.REST_PORT, String.valueOf(RCKUtils.findFreePort()));
          }
          this.localServer = new RESTCatalogServer(config);
          this.localServer.start(false);
          break;
        } catch (BindException e) {
          if (!findFreePort || i == maxAttempts - 1) {
            throw new RuntimeException("Failed to start REST server", e);
          }
        } catch (Exception e) {
          throw new RuntimeException("Failed to start REST server", e);
        }
      }

      this.client = RCKUtils.initCatalogClient(config);
    }
  }

  @Override
  public void afterAll(ExtensionContext extensionContext) throws Exception {
    if (localServer != null) {
      localServer.stop();
    }
    if (client != null) {
      client.close();
    }
  }
}
