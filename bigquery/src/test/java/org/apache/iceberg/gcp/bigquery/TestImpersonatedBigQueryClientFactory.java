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
import static org.apache.iceberg.gcp.bigquery.ImpersonatedBigQueryClientFactory.IMPERSONATE_DELEGATES;
import static org.apache.iceberg.gcp.bigquery.ImpersonatedBigQueryClientFactory.IMPERSONATE_LIFETIME_SECONDS;
import static org.apache.iceberg.gcp.bigquery.ImpersonatedBigQueryClientFactory.IMPERSONATE_SCOPES;
import static org.apache.iceberg.gcp.bigquery.ImpersonatedBigQueryClientFactory.IMPERSONATE_SERVICE_ACCOUNT;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;

public class TestImpersonatedBigQueryClientFactory {

  @Test
  public void testInitializeWithMinimalProperties() {
    ImpersonatedBigQueryClientFactory factory = new ImpersonatedBigQueryClientFactory();
    Map<String, String> properties =
        Map.of(
            PROJECT_ID, "test-project",
            IMPERSONATE_SERVICE_ACCOUNT, "test-sa@test-project.iam.gserviceaccount.com");
    assertThatCode(() -> factory.initialize(properties)).doesNotThrowAnyException();
  }

  @Test
  public void testInitializeWithAllProperties() {
    ImpersonatedBigQueryClientFactory factory = new ImpersonatedBigQueryClientFactory();
    Map<String, String> properties =
        Map.of(
            PROJECT_ID,
            "test-project",
            GCP_LOCATION,
            "us-central1",
            IMPERSONATE_SERVICE_ACCOUNT,
            "test-sa@test-project.iam.gserviceaccount.com",
            IMPERSONATE_LIFETIME_SECONDS,
            "3600",
            IMPERSONATE_SCOPES,
            "bigquery, devstorage.read_only",
            IMPERSONATE_DELEGATES,
            "test-delegate-sa1@test-project.iam.gserviceaccount.com, test-delegate-sa1@test-project.iam.gserviceaccount.com");
    assertThatCode(() -> factory.initialize(properties)).doesNotThrowAnyException();
  }

  @Test
  public void testInitializeWithNullProperties() {
    ImpersonatedBigQueryClientFactory factory = new ImpersonatedBigQueryClientFactory();
    assertThatThrownBy(() -> factory.initialize(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("Properties cannot be null");
  }

  @Test
  public void testInitializeWithoutServiceAccount() {
    ImpersonatedBigQueryClientFactory factory = new ImpersonatedBigQueryClientFactory();
    Map<String, String> properties = Map.of(PROJECT_ID, "test-project");
    assertThatThrownBy(() -> factory.initialize(properties))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining(
            "Cannot initialize ImpersonatedBigQueryClientFactory without target service account");
  }

  @Test
  public void testInitializeWithoutProjectId() {
    ImpersonatedBigQueryClientFactory factory = new ImpersonatedBigQueryClientFactory();
    Map<String, String> properties =
        Map.of(IMPERSONATE_SERVICE_ACCOUNT, "test-sa@test-project.iam.gserviceaccount.com");
    assertThatThrownBy(() -> factory.initialize(properties))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining(
            "Cannot initialize ImpersonatedBigQueryClientFactory without project ID");
  }

  @Test
  public void testInitializeWithDelegationChain() {
    ImpersonatedBigQueryClientFactory factory = new ImpersonatedBigQueryClientFactory();
    Map<String, String> properties =
        Map.of(
            PROJECT_ID,
            "test-project",
            IMPERSONATE_SERVICE_ACCOUNT,
            "test-sa@test-project.iam.gserviceaccount.com",
            IMPERSONATE_DELEGATES,
            "test-delegate-sa1@test-project.iam.gserviceaccount.com, test-delegate-sa2@test-project.iam.gserviceaccount.com");
    assertThatCode(() -> factory.initialize(properties)).doesNotThrowAnyException();
  }
}
