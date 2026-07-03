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

import static org.apache.iceberg.gcp.bigquery.BigQueryProperties.PROJECT_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.model.DatasetReference;
import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestBigQueryMetastoreCatalog {

  @TempDir private File tempFolder;
  private static final String GCP_PROJECT = "my-project";
  private static final String GCP_REGION = "us-central1";
  private static final Namespace NAMESPACE = Namespace.of("db");

  private BigQueryMetastoreCatalog newCatalog(BigQueryMetastoreClient client) {
    BigQueryMetastoreCatalog catalog = new BigQueryMetastoreCatalog();
    catalog.setConf(new Configuration());
    String warehouseLocation = tempFolder.toPath().resolve("hive-warehouse").toString();

    catalog.initialize(
        "CATALOG_ID",
        ImmutableMap.of(
            PROJECT_ID,
            GCP_PROJECT,
            CatalogProperties.WAREHOUSE_LOCATION,
            warehouseLocation,
            CatalogProperties.FILE_IO_IMPL,
            "org.apache.iceberg.hadoop.HadoopFileIO"),
        GCP_PROJECT,
        GCP_REGION,
        client);
    return catalog;
  }

  @Test
  public void removePropertiesChecksNamespaceExistenceOnlyOnce() {
    FakeBigQueryMetastoreClient client = spy(new FakeBigQueryMetastoreClient());
    BigQueryMetastoreCatalog catalog = newCatalog(client);
    catalog.createNamespace(NAMESPACE, ImmutableMap.of("key", "value"));

    boolean removed = catalog.removeProperties(NAMESPACE, ImmutableSet.of("key"));

    assertThat(removed).isTrue();
    // client.removeParameters() already checks namespace existence internally, so
    // removeProperties() must not perform a separate existence check of its own.
    verify(client, times(1)).load(any(DatasetReference.class));
  }

  @Test
  public void removePropertiesPropagatesNoSuchNamespaceException() {
    BigQueryMetastoreClient client = mock(BigQueryMetastoreClient.class);
    when(client.removeParameters(any(), any()))
        .thenThrow(new NoSuchNamespaceException("Namespace does not exist: %s", NAMESPACE));
    BigQueryMetastoreCatalog catalog = newCatalog(client);

    assertThatThrownBy(() -> catalog.removeProperties(NAMESPACE, ImmutableSet.of("key")))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining("Namespace does not exist");
  }

  @Test
  public void removePropertiesReturnsFalseForEmptyPropertiesWithoutCallingClient() {
    BigQueryMetastoreClient client = mock(BigQueryMetastoreClient.class);
    BigQueryMetastoreCatalog catalog = newCatalog(client);

    // Namespace does not exist, but an empty property set is checked first, so no
    // NoSuchNamespaceException is thrown and the client is never contacted.
    boolean removed = catalog.removeProperties(NAMESPACE, ImmutableSet.of());

    assertThat(removed).isFalse();
    verify(client, times(0)).load(any(DatasetReference.class));
    verify(client, times(0)).removeParameters(any(), any());
  }
}
