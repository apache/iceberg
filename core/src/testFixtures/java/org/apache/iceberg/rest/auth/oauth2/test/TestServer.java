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
package org.apache.iceberg.rest.auth.oauth2.test;

import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;

/**
 * A singleton test server for OAuth2 testing. It can be shared by many tests, even running in
 * parallel, provided that each test uses a different root path.
 */
public final class TestServer {

  private static final class Holder {

    private static final ClientAndServer INSTANCE;

    static {
      INSTANCE = ClientAndServer.startClientAndServer();
      Runtime.getRuntime().addShutdownHook(new Thread(INSTANCE::close));
    }
  }

  public static ClientAndServer instance() {
    return Holder.INSTANCE;
  }

  private TestServer() {}

  /** Clears all expectations and logs for the given path. */
  @SuppressWarnings("resource")
  public static void clearExpectations(String path) {
    instance().clear(HttpRequest.request().withPath(path));
  }
}
