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
package org.apache.iceberg.spark;

import static org.apache.iceberg.spark.SparkCatalogProperties.REST_CATALOG_PURGE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for the REST catalog purge delegation feature in {@link SparkCatalog}.
 *
 * <p>Verifies that {@link SparkCatalogProperties#REST_CATALOG_PURGE} controls whether Spark
 * delegates DROP TABLE PURGE to the REST catalog or performs client-side file deletion.
 */
public class TestRestDropPurgeTable extends TestBase {

  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  private static final Identifier SPARK_ID = Identifier.of(new String[] {"ns"}, "test_table");

  @TempDir private File tableDir;

  private RESTCatalog restCatalogMock;

  private SparkCatalog createCatalog(boolean catalogPurge) {
    Table table =
        new HadoopTables(new Configuration())
            .create(SCHEMA, PartitionSpec.unpartitioned(), tableDir.getAbsolutePath());

    restCatalogMock = mock(RESTCatalog.class);
    when(restCatalogMock.loadTable(any())).thenReturn(table);
    when(restCatalogMock.dropTable(any(), anyBoolean())).thenReturn(true);

    SparkCatalog catalog =
        new SparkCatalog() {
          @Override
          protected Catalog buildIcebergCatalog(String name, CaseInsensitiveStringMap options) {
            return restCatalogMock;
          }
        };
    catalog.initialize(
        "test_catalog",
        new CaseInsensitiveStringMap(
            ImmutableMap.of(REST_CATALOG_PURGE, Boolean.toString(catalogPurge))));
    return catalog;
  }

  @Test
  void purgeTableDelegatesToCatalogWhenEnabled() {
    SparkCatalog catalog = createCatalog(true);
    catalog.purgeTable(SPARK_ID);
    verify(restCatalogMock).dropTable(any(), eq(true));
  }

  @Test
  void purgeNotDelegatedToCatalogWhenDisabled() {
    SparkCatalog catalog = createCatalog(false);
    catalog.purgeTable(SPARK_ID);
    verify(restCatalogMock).dropTable(any(), eq(false));
  }

  @Test
  void initializationFailsWhenPurgeEnabledWithNonRestCatalog() {
    SparkCatalog catalog =
        new SparkCatalog() {
          @Override
          protected Catalog buildIcebergCatalog(String name, CaseInsensitiveStringMap options) {
            return new InMemoryCatalog();
          }
        };
    assertThatThrownBy(
            () ->
                catalog.initialize(
                    "test_catalog",
                    new CaseInsensitiveStringMap(ImmutableMap.of(REST_CATALOG_PURGE, "true"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(REST_CATALOG_PURGE);
  }

  @Test
  void purgeTableDelegatesToCatalogWhenEnabledViaSessionCatalog() {
    Table table =
        new HadoopTables(new Configuration())
            .create(SCHEMA, PartitionSpec.unpartitioned(), tableDir.getAbsolutePath());

    RESTCatalog sessionRestCatalogMock = mock(RESTCatalog.class);
    when(sessionRestCatalogMock.loadTable(any())).thenReturn(table);
    when(sessionRestCatalogMock.dropTable(any(), anyBoolean())).thenReturn(true);

    SparkSessionCatalog<SparkCatalog> sessionCatalog =
        new SparkSessionCatalog<SparkCatalog>() {
          @Override
          protected TableCatalog buildSparkCatalog(String name, CaseInsensitiveStringMap options) {
            SparkCatalog sparkCatalog =
                new SparkCatalog() {
                  @Override
                  protected Catalog buildIcebergCatalog(
                      String catalogName, CaseInsensitiveStringMap catalogOptions) {
                    return sessionRestCatalogMock;
                  }
                };
            sparkCatalog.initialize(name, options);
            return sparkCatalog;
          }
        };

    sessionCatalog.initialize(
        "spark_catalog", new CaseInsensitiveStringMap(ImmutableMap.of(REST_CATALOG_PURGE, "true")));

    sessionCatalog.purgeTable(SPARK_ID);

    verify(sessionRestCatalogMock).dropTable(any(), eq(true));
  }
}
