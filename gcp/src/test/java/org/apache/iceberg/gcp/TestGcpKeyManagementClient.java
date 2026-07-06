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
package org.apache.iceberg.gcp;

import static org.apache.iceberg.gcp.GCPProperties.GCS_NO_AUTH;
import static org.apache.iceberg.gcp.GCPProperties.KMS_ENDPOINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;

class TestGcpKeyManagementClient {
  @Test
  void kmsEndpoint() {
    try (GcpKeyManagementClient client = new GcpKeyManagementClient()) {
      String endpoint = "example.com:443";
      client.initialize(Map.of(GCS_NO_AUTH, "true", KMS_ENDPOINT, endpoint));

      assertThat(client.kmsClient().getSettings().getEndpoint()).isEqualTo(endpoint);
    }
  }

  @Test
  void invalidKmsEndpoint() {
    try (GcpKeyManagementClient client = new GcpKeyManagementClient()) {
      client.initialize(Map.of(GCS_NO_AUTH, "true", KMS_ENDPOINT, "example.com"));

      assertThatThrownBy(client::kmsClient)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("invalid endpoint, expecting \"<host>:<port>\"");
    }
  }
}
