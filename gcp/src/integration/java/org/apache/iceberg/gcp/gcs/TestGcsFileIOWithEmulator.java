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
package org.apache.iceberg.gcp.gcs;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports.Binding;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.time.Duration;
import java.util.Map;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class TestGcsFileIOWithEmulator extends TestGcsFileIO {

  private static final String BUCKET = "test-bucket";
  private static final String PROJECT_ID = "test-project";
  private static final int GCS_EMULATOR_PORT = 4443;

  @Container
  private static final GenericContainer<?> GCS_EMULATOR =
      new GenericContainer<>("fsouza/fake-gcs-server:latest")
          .withExposedPorts(GCS_EMULATOR_PORT)
          .withCreateContainerCmdModifier(
              cmd ->
                  cmd.withHostConfig(
                      new HostConfig()
                          .withPortBindings(
                              new PortBinding(
                                  Binding.bindPort(GCS_EMULATOR_PORT),
                                  new ExposedPort(GCS_EMULATOR_PORT)))))
          .withCommand(
              "-scheme",
              "http",
              "-external-url",
              String.format("http://localhost:%d", GCS_EMULATOR_PORT))
          .waitingFor(
              new HttpWaitStrategy()
                  .forPort(GCS_EMULATOR_PORT)
                  .forPath("/storage/v1/b")
                  .forStatusCode(200)
                  .withStartupTimeout(Duration.ofMinutes(2)));

  private static Storage emulatorStorage;

  @BeforeAll
  public static void beforeClass() {
    String endpoint = String.format("http://localhost:%d", GCS_EMULATOR_PORT);
    StorageOptions options =
        StorageOptions.newBuilder()
            .setProjectId(PROJECT_ID)
            .setHost(endpoint)
            .setCredentials(NoCredentials.getInstance())
            .build();
    emulatorStorage = options.getService();
    emulatorStorage.create(BucketInfo.of(BUCKET));
  }

  @AfterAll
  public static void afterClass() {
    if (emulatorStorage != null) {
      emulatorStorage.delete(BUCKET);
    }
  }

  @Override
  protected void init() {
    this.storage = emulatorStorage;
    this.bucket = BUCKET;
  }

  @Override
  protected Map<String, String> properties() {
    return ImmutableMap.of(
        GCPProperties.GCS_NO_AUTH,
        "true",
        GCPProperties.GCS_SERVICE_HOST,
        String.format("http://localhost:%d", GCS_EMULATOR_PORT));
  }
}
