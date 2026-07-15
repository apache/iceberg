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

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.Collections;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableSummary;
import org.apache.spark.sql.connector.catalog.ViewCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestSparkSessionCatalog extends TestBase {
  private final String envHmsUriKey = "spark.hadoop." + METASTOREURIS.varname;
  private final String catalogHmsUriKey = "spark.sql.catalog.spark_catalog.uri";
  private final String hmsUri = hiveConf.get(METASTOREURIS.varname);

  @BeforeAll
  public static void setUpCatalog() {
    spark
        .conf()
        .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
    spark.conf().set("spark.sql.catalog.spark_catalog.type", "hive");
  }

  @BeforeEach
  public void setupHmsUri() {
    spark.sessionState().catalogManager().reset();
    spark.conf().set(envHmsUriKey, hmsUri);
    spark.conf().set(catalogHmsUriKey, hmsUri);
  }

  @Test
  public void testValidateHmsUri() {
    // HMS uris match
    assertThat(spark.sessionState().catalogManager().v2SessionCatalog().defaultNamespace())
        .containsExactly("default");

    // HMS uris doesn't match
    spark.sessionState().catalogManager().reset();
    String catalogHmsUri = "RandomString";
    spark.conf().set(envHmsUriKey, hmsUri);
    spark.conf().set(catalogHmsUriKey, catalogHmsUri);

    assertThatThrownBy(() -> spark.sessionState().catalogManager().v2SessionCatalog())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            String.format(
                "Inconsistent Hive metastore URIs: %s (Spark session) != %s (spark_catalog)",
                hmsUri, catalogHmsUri));

    // no env HMS uri, only catalog HMS uri
    spark.sessionState().catalogManager().reset();
    spark.conf().set(catalogHmsUriKey, hmsUri);
    spark.conf().unset(envHmsUriKey);
    assertThat(spark.sessionState().catalogManager().v2SessionCatalog().defaultNamespace())
        .containsExactly("default");

    // no catalog HMS uri, only env HMS uri
    spark.sessionState().catalogManager().reset();
    spark.conf().set(envHmsUriKey, hmsUri);
    spark.conf().unset(catalogHmsUriKey);
    assertThat(spark.sessionState().catalogManager().v2SessionCatalog().defaultNamespace())
        .containsExactly("default");
  }

  @Test
  public void testLoadFunction() {
    String functionClass = "org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper";

    // load permanent UDF in Hive via FunctionCatalog
    spark.sql(String.format("CREATE FUNCTION perm_upper AS '%s'", functionClass));
    assertThat(scalarSql("SELECT perm_upper('xyz')"))
        .as("Load permanent UDF in Hive")
        .isEqualTo("XYZ");

    // load temporary UDF in Hive via FunctionCatalog
    spark.sql(String.format("CREATE TEMPORARY FUNCTION temp_upper AS '%s'", functionClass));
    assertThat(scalarSql("SELECT temp_upper('xyz')"))
        .as("Load temporary UDF in Hive")
        .isEqualTo("XYZ");

    // TODO: fix loading Iceberg built-in functions in SessionCatalog
  }

  @Test
  public void listViewsReturnsSessionCatalogViews() throws NoSuchNamespaceException {
    Identifier viewIdent = Identifier.of(new String[] {"default"}, "session_catalog_list_views");
    Identifier[] views = new Identifier[] {viewIdent};
    TableCatalog sessionCatalog =
        mock(
            TableCatalog.class,
            withSettings()
                .extraInterfaces(
                    FunctionCatalog.class, SupportsNamespaces.class, ViewCatalog.class));
    when(((ViewCatalog) sessionCatalog).listViews(new String[] {"default"})).thenReturn(views);

    SparkSessionCatalog<?> catalog = new NoViewCatalog<>();
    catalog.initialize("spark_catalog", new CaseInsensitiveStringMap(Collections.emptyMap()));
    catalog.setDelegateCatalog(sessionCatalog);

    assertThat(catalog.listViews("default")).containsExactly(viewIdent);
  }

  @Test
  public void listRelationSummariesUnionsAndDeduplicatesViews()
      throws NoSuchNamespaceException, NoSuchTableException {
    String[] namespace = new String[] {"default"};
    Identifier tableIdent = Identifier.of(namespace, "table");
    Identifier sharedViewIdent = Identifier.of(namespace, "shared_view");
    Identifier sessionViewIdent = Identifier.of(namespace, "session_view");
    Identifier icebergViewIdent = Identifier.of(namespace, "iceberg_view");

    TableCatalog sessionCatalog =
        mock(
            TableCatalog.class,
            withSettings()
                .extraInterfaces(
                    FunctionCatalog.class, SupportsNamespaces.class, ViewCatalog.class));
    when(sessionCatalog.listTableSummaries(namespace))
        .thenReturn(
            new TableSummary[] {
              TableSummary.of(tableIdent, TableSummary.EXTERNAL_TABLE_TYPE),
              TableSummary.of(sharedViewIdent, TableSummary.FOREIGN_TABLE_TYPE),
              TableSummary.of(sessionViewIdent, TableSummary.VIEW_TABLE_TYPE)
            });
    when(((ViewCatalog) sessionCatalog).listViews(namespace))
        .thenReturn(new Identifier[] {sharedViewIdent, sessionViewIdent});

    TableCatalog icebergCatalog =
        mock(TableCatalog.class, withSettings().extraInterfaces(ViewCatalog.class));
    when(((ViewCatalog) icebergCatalog).listViews(namespace))
        .thenReturn(new Identifier[] {sharedViewIdent, icebergViewIdent});

    SparkSessionCatalog<?> catalog = new CatalogWithIcebergViews<>(icebergCatalog);
    catalog.initialize("spark_catalog", new CaseInsensitiveStringMap(Collections.emptyMap()));
    catalog.setDelegateCatalog(sessionCatalog);

    assertThat(catalog.listViews(namespace))
        .containsExactly(sharedViewIdent, icebergViewIdent, sessionViewIdent);
    assertThat(catalog.listTableSummaries(namespace))
        .extracting(TableSummary::identifier)
        .containsExactly(tableIdent);
    assertThat(catalog.listRelationSummaries(namespace))
        .extracting(TableSummary::identifier)
        .containsExactlyInAnyOrder(tableIdent, sharedViewIdent, sessionViewIdent, icebergViewIdent);
    assertThat(catalog.listRelationSummaries(namespace))
        .filteredOn(summary -> summary.identifier().equals(sharedViewIdent))
        .singleElement()
        .extracting(TableSummary::tableType)
        .isEqualTo(TableSummary.VIEW_TABLE_TYPE);
  }

  private static class NoViewCatalog<
          T extends TableCatalog & FunctionCatalog & SupportsNamespaces & ViewCatalog>
      extends SparkSessionCatalog<T> {
    @Override
    protected TableCatalog buildSparkCatalog(String name, CaseInsensitiveStringMap options) {
      return mock(TableCatalog.class);
    }
  }

  private static class CatalogWithIcebergViews<
          T extends TableCatalog & FunctionCatalog & SupportsNamespaces & ViewCatalog>
      extends SparkSessionCatalog<T> {
    private final TableCatalog icebergCatalog;

    private CatalogWithIcebergViews(TableCatalog icebergCatalog) {
      this.icebergCatalog = icebergCatalog;
    }

    @Override
    protected TableCatalog buildSparkCatalog(String name, CaseInsensitiveStringMap options) {
      return icebergCatalog;
    }
  }
}
