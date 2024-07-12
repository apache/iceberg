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
package org.apache.iceberg.lock;

import static org.apache.iceberg.lock.ServerSideHttpLockManager.REQUEST_AUTH;
import static org.apache.iceberg.lock.ServerSideHttpLockManager.REQUEST_URL;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class HttpLockManagerTest {
  private TestHttpServer testHttpServer = null;

  @BeforeEach
  public void before() throws IOException {
    testHttpServer = new TestHttpServer();
  }

  @AfterEach
  public void after() throws IOException {
    testHttpServer.close();
  }

  @Test
  public void httpLockTest() {
    ServerSideHttpLockManager httpLockManager =
        new ServerSideHttpLockManager("http://localhost:28081/test-rest-lock");
    String ownerId = UUID.randomUUID().toString();
    String entityId = UUID.randomUUID().toString();
    assertThat(httpLockManager.acquire(entityId, ownerId)).isTrue();
    assertThat(httpLockManager.release(entityId, ownerId)).isTrue();
  }

  @Test
  public void testWithAuth() {
    ServerSideHttpLockManager httpLockManager = new ServerSideHttpLockManager();

    httpLockManager.initialize(
        ImmutableMap.of(
            REQUEST_AUTH,
            TestAuthImpl.class.getName(),
            REQUEST_URL,
            "http://localhost:28081/test-rest-lock"));

    String ownerId = UUID.randomUUID().toString();
    String entityId = UUID.randomUUID().toString();
    assertThat(httpLockManager.acquire(entityId, ownerId)).isTrue();
    assertThat(httpLockManager.release(entityId, ownerId)).isTrue();
  }

  @Test
  public void testWithAuthAndRequestFailed() {
    ServerSideHttpLockManager httpLockManager = new ServerSideHttpLockManager();

    httpLockManager.initialize(
        ImmutableMap.of(
            REQUEST_AUTH,
            TestAuthImpl.class.getName(),
            REQUEST_URL,
            "http://localhost:28081/test-rest-lock",
            "timeStamp",
            "123456"));

    String ownerId = UUID.randomUUID().toString();
    String entityId = UUID.randomUUID().toString();
    assertThat(httpLockManager.acquire(entityId, ownerId)).isFalse();
    assertThat(httpLockManager.release(entityId, ownerId)).isFalse();
  }
}
