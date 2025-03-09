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
package org.apache.iceberg.azure.adlsv2;

import static org.mockserver.integration.ClientAndServer.startClientAndServer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.mockserver.integration.ClientAndServer;

public class BaseVendedCredentialsTest {
  protected static final int PORT = 3232;
  protected static final String BASE_URI = String.format("http://127.0.0.1:%d", PORT);
  protected static ClientAndServer mockServer;

  @BeforeAll
  public static void beforeAll() {
    mockServer = startClientAndServer(PORT);
  }

  @AfterAll
  public static void stopServer() {
    mockServer.stop();
  }

  @BeforeEach
  public void before() {
    mockServer.reset();
  }
}
