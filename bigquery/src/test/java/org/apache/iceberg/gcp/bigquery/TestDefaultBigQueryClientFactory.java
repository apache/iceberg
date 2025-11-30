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

import static org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog.GCP_LOCATION;
import static org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog.PROJECT_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.cloud.bigquery.BigQueryOptions;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class TestDefaultBigQueryClientFactory {

  @Test
  public void testInitializeWithValidProperties() {
    DefaultBigQueryClientFactory factory = new DefaultBigQueryClientFactory();
    Map<String, String> properties =
        Map.of(
            PROJECT_ID, "test-project",
            GCP_LOCATION, "location");
    factory.initialize(properties);
    BigQueryOptions options = factory.bigQueryOptions();
    assertThat(options).isNotNull();
    assertThat(options.getProjectId()).isEqualTo("test-project");
    assertThat(options.getLocation()).isEqualTo("location");
  }

  @Test
  public void testInitializeWithDefaultLocation() {
    DefaultBigQueryClientFactory factory = new DefaultBigQueryClientFactory();
    Map<String, String> properties = Map.of(PROJECT_ID, "test-project");
    factory.initialize(properties);
    BigQueryOptions options = factory.bigQueryOptions();
    assertThat(options).isNotNull();
    assertThat(options.getProjectId()).isEqualTo("test-project");
    assertThat(options.getLocation()).isEqualTo("us");
  }

  @Test
  public void testInitializeWithNullProperties() {
    DefaultBigQueryClientFactory factory = new DefaultBigQueryClientFactory();
    assertThatThrownBy(() -> factory.initialize(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("Properties cannot be null");
  }

  @Test
  public void testInitializeWithoutProjectId() {
    DefaultBigQueryClientFactory factory = new DefaultBigQueryClientFactory();
    Map<String, String> properties = Map.of(GCP_LOCATION, "location");
    assertThatThrownBy(() -> factory.initialize(properties))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("Cannot initialize DefaultBigQueryClientFactory without project ID");
  }

  @Test
  public void testInitializeWithEmptyProperties() {
    DefaultBigQueryClientFactory factory = new DefaultBigQueryClientFactory();
    Map<String, String> properties = Map.of();
    assertThatThrownBy(() -> factory.initialize(properties))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("Cannot initialize DefaultBigQueryClientFactory without project ID");
  }

  @Test
  public void testInitializeCanBeCalledMultipleTimes() {
    DefaultBigQueryClientFactory factory = new DefaultBigQueryClientFactory();
    Map<String, String> properties1 = Map.of(PROJECT_ID, "test-project-1");
    factory.initialize(properties1);
    BigQueryOptions options1 = factory.bigQueryOptions();
    assertThat(options1.getProjectId()).isEqualTo("test-project-1");

    Map<String, String> properties2 = Map.of(PROJECT_ID, "test-project-2");
    factory.initialize(properties2);
    BigQueryOptions options2 = factory.bigQueryOptions();
    assertThat(options2.getProjectId()).isEqualTo("test-project-2");
  }
}
