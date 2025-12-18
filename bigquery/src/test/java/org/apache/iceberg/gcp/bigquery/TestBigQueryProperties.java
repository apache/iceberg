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
package org.apache.iceberg.gcp.bigquery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class TestBigQueryProperties {

  @Test
  public void testInitializeWithValidProperties() {
    Map<String, String> properties =
        Map.of(
            BigQueryProperties.PROJECT_ID, "test-project",
            BigQueryProperties.GCP_LOCATION, "us-central1");
    BigQueryProperties bigQueryProperties = new BigQueryProperties(properties);

    assertThat(bigQueryProperties.projectId()).isEqualTo("test-project");
    assertThat(bigQueryProperties.location()).isEqualTo("us-central1");
  }

  @Test
  public void testInitializeWithDefaultLocation() {
    Map<String, String> properties = Map.of(BigQueryProperties.PROJECT_ID, "test-project");
    BigQueryProperties bigQueryProperties = new BigQueryProperties(properties);

    assertThat(bigQueryProperties.projectId()).isEqualTo("test-project");
    assertThat(bigQueryProperties.location()).isEqualTo("us");
  }

  @Test
  public void testInitializeWithNullProperties() {
    assertThatThrownBy(() -> new BigQueryProperties(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("Properties cannot be null");
  }

  @Test
  public void testInitializeWithoutProjectId() {
    Map<String, String> properties = Map.of(BigQueryProperties.GCP_LOCATION, "us-central1");
    assertThatThrownBy(() -> new BigQueryProperties(properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("gcp.bigquery.project-id");
  }

  @Test
  public void testExpandScopesMixedFormats() {
    Map<String, String> properties = Map.of(BigQueryProperties.PROJECT_ID, "test-project");
    BigQueryProperties bigQueryProperties = new BigQueryProperties(properties);

    assertThat(
            bigQueryProperties.expandScopes(
                Arrays.asList(
                    "bigquery",
                    "http://www.googleapis.com/auth/devstorage.read_write",
                    "https://www.googleapis.com/auth/cloud-platform")))
        .containsExactly(
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/devstorage.read_write",
            "https://www.googleapis.com/auth/cloud-platform");
  }

  @Test
  public void testParseCommaSeparatedList() {
    Map<String, String> properties = Map.of(BigQueryProperties.PROJECT_ID, "test-project");
    BigQueryProperties bigQueryProperties = new BigQueryProperties(properties);

    assertThat(bigQueryProperties.parseCommaSeparatedList("a, b, c", null))
        .containsExactly("a", "b", "c");
    assertThat(bigQueryProperties.parseCommaSeparatedList(null, Arrays.asList("default")))
        .containsExactly("default");
    assertThat(bigQueryProperties.parseCommaSeparatedList("", Arrays.asList("default")))
        .containsExactly("default");
  }
}
