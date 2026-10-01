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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.Collections;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.SupportsDeleteV2;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableSummary;
import org.apache.spark.sql.connector.catalog.View;
import org.apache.spark.sql.connector.catalog.ViewCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.types.StructType;
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
  public void listingUsesSessionNamespaceWhenIcebergNamespaceIsMissing()
      throws NoSuchNamespaceException, NoSuchTableException {
    String[] namespace = new String[] {"session_only"};
    Identifier tableIdent = Identifier.of(namespace, "table");
    Identifier viewIdent = Identifier.of(namespace, "view");

    TableCatalog sessionCatalog = sessionCatalogWithViews();
    ViewCatalog sessionViewCatalog = (ViewCatalog) sessionCatalog;
    when(sessionViewCatalog.listViews(namespace)).thenReturn(new Identifier[] {viewIdent});
    when(sessionCatalog.listTableSummaries(namespace))
        .thenReturn(
            new TableSummary[] {TableSummary.of(tableIdent, TableSummary.EXTERNAL_TABLE_TYPE)});

    TableCatalog icebergCatalog = icebergCatalogWithViews();
    ViewCatalog icebergViewCatalog = (ViewCatalog) icebergCatalog;
    when(icebergViewCatalog.listViews(namespace))
        .thenThrow(new NoSuchNamespaceException(namespace));
    when(icebergCatalog.listTableSummaries(namespace))
        .thenThrow(new NoSuchNamespaceException(namespace));

    SparkSessionCatalog<?> catalog = catalogWithViews(icebergCatalog, sessionCatalog);

    assertThat(catalog.listViews(namespace)).containsExactly(viewIdent);
    assertThat(catalog.listTableSummaries(namespace))
        .extracting(TableSummary::identifier)
        .containsExactly(tableIdent);
    assertThat(catalog.listRelationSummaries(namespace))
        .extracting(TableSummary::identifier)
        .containsExactly(tableIdent, viewIdent);
  }

  @Test
  public void listViewsValidatesSessionNamespaceFirst() throws NoSuchNamespaceException {
    String[] namespace = new String[] {"missing"};
    TableCatalog sessionCatalog = sessionCatalogWithViews();
    ViewCatalog sessionViewCatalog = (ViewCatalog) sessionCatalog;
    when(sessionViewCatalog.listViews(namespace))
        .thenThrow(new NoSuchNamespaceException(namespace));

    TableCatalog icebergCatalog = icebergCatalogWithViews();
    ViewCatalog icebergViewCatalog = (ViewCatalog) icebergCatalog;
    SparkSessionCatalog<?> catalog = catalogWithViews(icebergCatalog, sessionCatalog);

    assertThatThrownBy(() -> catalog.listViews(namespace))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(namespace[0])
        .hasCauseInstanceOf(NoSuchNamespaceException.class);
    verify(icebergViewCatalog, never()).listViews(namespace);
  }

  @Test
  public void rollbackStagedTableDelegatesTruncatableDriverMetrics() {
    SupportsDeleteV2 table = mock(SupportsDeleteV2.class);
    CustomTaskMetric metric = mock(CustomTaskMetric.class);
    when(table.reportDriverMetrics()).thenReturn(new CustomTaskMetric[] {metric});

    RollbackStagedTable stagedTable =
        new RollbackStagedTable(
            mock(TableCatalog.class), Identifier.of(new String[] {"default"}, "table"), table);

    assertThat(stagedTable.reportDriverMetrics()).containsExactly(metric);
  }

  @Test
  public void rollbackStagedTableDelegatesStagedDriverMetrics() {
    StagedTable table = mock(StagedTable.class);
    CustomTaskMetric metric = mock(CustomTaskMetric.class);
    when(table.reportDriverMetrics()).thenReturn(new CustomTaskMetric[] {metric});

    RollbackStagedTable stagedTable =
        new RollbackStagedTable(
            mock(TableCatalog.class), Identifier.of(new String[] {"default"}, "table"), table);

    assertThat(stagedTable.reportDriverMetrics()).containsExactly(metric);
  }

  @Test
  public void listRelationSummariesUnionsAndDeduplicatesViews()
      throws NoSuchNamespaceException, NoSuchTableException, NoSuchViewException {
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
    ViewCatalog sessionViewCatalog = (ViewCatalog) sessionCatalog;
    when(sessionCatalog.listTableSummaries(namespace))
        .thenReturn(
            new TableSummary[] {
              TableSummary.of(tableIdent, TableSummary.EXTERNAL_TABLE_TYPE),
              TableSummary.of(sharedViewIdent, TableSummary.VIEW_TABLE_TYPE),
              TableSummary.of(sessionViewIdent, TableSummary.VIEW_TABLE_TYPE)
            });
    when(sessionViewCatalog.listViews(namespace))
        .thenReturn(new Identifier[] {sharedViewIdent, sessionViewIdent});

    TableCatalog icebergCatalog =
        mock(TableCatalog.class, withSettings().extraInterfaces(ViewCatalog.class));
    ViewCatalog icebergViewCatalog = (ViewCatalog) icebergCatalog;
    when(icebergCatalog.listTableSummaries(namespace)).thenReturn(new TableSummary[0]);
    when(icebergViewCatalog.listViews(namespace))
        .thenReturn(new Identifier[] {sharedViewIdent, icebergViewIdent});

    SparkSessionCatalog<?> catalog = catalogWithViews(icebergCatalog, sessionCatalog);

    assertThat(catalog.listViews(namespace))
        .containsExactly(sharedViewIdent, sessionViewIdent, icebergViewIdent);
    assertThat(catalog.listTableSummaries(namespace))
        .extracting(TableSummary::identifier)
        .containsExactly(tableIdent);
    TableSummary[] relationSummaries = catalog.listRelationSummaries(namespace);
    assertThat(relationSummaries)
        .extracting(TableSummary::identifier)
        .containsExactlyInAnyOrder(tableIdent, sharedViewIdent, sessionViewIdent, icebergViewIdent);
    assertThat(relationSummaries)
        .filteredOn(summary -> summary.identifier().equals(sharedViewIdent))
        .singleElement()
        .extracting(TableSummary::tableType)
        .isEqualTo(TableSummary.VIEW_TABLE_TYPE);
    verify(icebergCatalog, times(2)).listTableSummaries(namespace);
    verify(icebergCatalog, never()).tableExists(any());
    verify(sessionViewCatalog, never()).loadView(any());
    verify(icebergViewCatalog, never()).loadView(any());
  }

  @Test
  public void relationSummariesIgnoreFalsePositiveIcebergTableListing()
      throws NoSuchNamespaceException, NoSuchTableException {
    String[] namespace = new String[] {"default"};
    Identifier viewIdent = Identifier.of(namespace, "view");
    Identifier unrelatedTableIdent = Identifier.of(namespace, "unrelated_table");

    TableCatalog sessionCatalog = sessionCatalogWithViews();
    when(((ViewCatalog) sessionCatalog).listViews(namespace))
        .thenReturn(new Identifier[] {viewIdent});
    when(sessionCatalog.listTableSummaries(namespace))
        .thenReturn(new TableSummary[] {TableSummary.of(viewIdent, TableSummary.VIEW_TABLE_TYPE)});

    TableCatalog icebergCatalog = icebergCatalogWithViews();
    when(((ViewCatalog) icebergCatalog).listViews(namespace)).thenReturn(new Identifier[0]);
    when(icebergCatalog.listTableSummaries(namespace))
        .thenReturn(
            new TableSummary[] {
              TableSummary.of(viewIdent, TableSummary.EXTERNAL_TABLE_TYPE),
              TableSummary.of(unrelatedTableIdent, TableSummary.EXTERNAL_TABLE_TYPE)
            });
    when(icebergCatalog.tableExists(viewIdent)).thenReturn(false);

    SparkSessionCatalog<?> catalog = catalogWithViews(icebergCatalog, sessionCatalog);

    assertThat(catalog.listTableSummaries(namespace)).isEmpty();
    assertThat(catalog.listRelationSummaries(namespace))
        .singleElement()
        .extracting(TableSummary::tableType)
        .isEqualTo(TableSummary.VIEW_TABLE_TYPE);
    verify(icebergCatalog, times(2)).listTableSummaries(namespace);
    verify(icebergCatalog, times(2)).tableExists(viewIdent);
    verify(icebergCatalog, never()).tableExists(unrelatedTableIdent);
  }

  @Test
  public void relationSummariesPreserveSessionTableForCrossCatalogCollision()
      throws NoSuchNamespaceException, NoSuchTableException {
    String[] namespace = new String[] {"default"};
    Identifier ident = Identifier.of(namespace, "session_table_collision");
    Table table = mock(Table.class);

    TableCatalog sessionCatalog = sessionCatalogWithViews();
    when(((ViewCatalog) sessionCatalog).listViews(namespace)).thenReturn(new Identifier[0]);
    when(sessionCatalog.listTableSummaries(namespace))
        .thenReturn(new TableSummary[] {TableSummary.of(ident, TableSummary.EXTERNAL_TABLE_TYPE)});
    when(sessionCatalog.loadTable(ident)).thenReturn(table);

    TableCatalog icebergCatalog = icebergCatalogWithViews();
    when(icebergCatalog.listTableSummaries(namespace)).thenReturn(new TableSummary[0]);
    when(((ViewCatalog) icebergCatalog).listViews(namespace)).thenReturn(new Identifier[] {ident});
    when(icebergCatalog.loadTable(ident)).thenThrow(new NoSuchTableException(ident));

    SparkSessionCatalog<?> catalog = catalogWithViews(icebergCatalog, sessionCatalog);

    assertThat(catalog.listTableSummaries(namespace))
        .singleElement()
        .extracting(TableSummary::tableType)
        .isEqualTo(TableSummary.EXTERNAL_TABLE_TYPE);
    assertThat(catalog.listRelationSummaries(namespace))
        .singleElement()
        .extracting(TableSummary::tableType)
        .isEqualTo(TableSummary.EXTERNAL_TABLE_TYPE);
    assertThat(catalog.loadRelation(ident)).isSameAs(table);
    verify(icebergCatalog, never()).listTableSummaries(namespace);
    verify(icebergCatalog, never()).tableExists(any());
  }

  @Test
  public void relationSummariesPreserveIcebergTableForCrossCatalogCollision()
      throws NoSuchNamespaceException, NoSuchTableException {
    String[] namespace = new String[] {"default"};
    Identifier ident = Identifier.of(namespace, "iceberg_table_collision");
    Table table = mock(Table.class);

    TableCatalog sessionCatalog = sessionCatalogWithViews();
    when(((ViewCatalog) sessionCatalog).listViews(namespace)).thenReturn(new Identifier[] {ident});
    when(sessionCatalog.listTableSummaries(namespace))
        .thenReturn(new TableSummary[] {TableSummary.of(ident, TableSummary.VIEW_TABLE_TYPE)});

    TableCatalog icebergCatalog = icebergCatalogWithViews();
    when(((ViewCatalog) icebergCatalog).listViews(namespace)).thenReturn(new Identifier[0]);
    when(icebergCatalog.listTableSummaries(namespace))
        .thenReturn(new TableSummary[] {TableSummary.of(ident, TableSummary.EXTERNAL_TABLE_TYPE)});
    when(icebergCatalog.tableExists(ident)).thenReturn(true);
    when(icebergCatalog.loadTable(ident)).thenReturn(table);

    SparkSessionCatalog<?> catalog = catalogWithViews(icebergCatalog, sessionCatalog);

    assertThat(catalog.listTableSummaries(namespace))
        .singleElement()
        .extracting(TableSummary::tableType)
        .isEqualTo(TableSummary.EXTERNAL_TABLE_TYPE);
    assertThat(catalog.listRelationSummaries(namespace))
        .singleElement()
        .extracting(TableSummary::tableType)
        .isEqualTo(TableSummary.EXTERNAL_TABLE_TYPE);
    assertThat(catalog.loadRelation(ident)).isSameAs(table);
    verify(icebergCatalog, times(2)).listTableSummaries(namespace);
    verify(icebergCatalog, times(2)).tableExists(ident);
  }

  @Test
  public void tableCreationRejectsViewCollision() {
    Identifier ident = Identifier.of(new String[] {"default"}, "view");
    TableCatalog sessionCatalog = sessionCatalogWithViews();
    TableCatalog icebergCatalog = icebergCatalogWithViews();
    when(((ViewCatalog) icebergCatalog).viewExists(ident)).thenReturn(true);

    SparkSessionCatalog<?> catalog = catalogWithViews(icebergCatalog, sessionCatalog);
    StructType schema = new StructType();
    Transform[] partitions = new Transform[0];

    assertThatThrownBy(() -> catalog.createTable(ident, schema, partitions, Collections.emptyMap()))
        .isInstanceOf(TableAlreadyExistsException.class)
        .hasMessageContaining(ident.name());
    assertThatThrownBy(() -> catalog.stageCreate(ident, schema, partitions, Collections.emptyMap()))
        .isInstanceOf(TableAlreadyExistsException.class)
        .hasMessageContaining(ident.name());
    assertThatThrownBy(
            () ->
                catalog.stageCreateOrReplace(
                    ident, schema, partitions, Collections.singletonMap("provider", "parquet")))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessage(
            "Cannot create or replace table %s: a view with the same name already exists", ident);
  }

  @Test
  public void tableRenameRejectsViewCollision() throws Exception {
    Identifier from = Identifier.of(new String[] {"default"}, "table");
    Identifier to = Identifier.of(new String[] {"default"}, "view");
    TableCatalog sessionCatalog = sessionCatalogWithViews();
    TableCatalog icebergCatalog = icebergCatalogWithViews();
    when(((ViewCatalog) icebergCatalog).viewExists(to)).thenReturn(true);

    SparkSessionCatalog<?> catalog = catalogWithViews(icebergCatalog, sessionCatalog);

    assertThatThrownBy(() -> catalog.renameTable(from, to))
        .isInstanceOf(TableAlreadyExistsException.class)
        .hasMessageContaining(to.name());
    verify(icebergCatalog, never()).renameTable(from, to);
    verify(sessionCatalog, never()).renameTable(from, to);
  }

  @Test
  public void viewCreationRejectsTableCollision() throws Exception {
    Identifier ident = Identifier.of(new String[] {"default"}, "table");
    View view = mock(View.class);
    TableCatalog sessionCatalog = sessionCatalogWithViews();
    when(sessionCatalog.tableExists(ident)).thenReturn(true);
    TableCatalog icebergCatalog = icebergCatalogWithViews();
    ViewCatalog icebergViewCatalog = (ViewCatalog) icebergCatalog;

    SparkSessionCatalog<?> catalog = catalogWithViews(icebergCatalog, sessionCatalog);

    assertThatThrownBy(() -> catalog.createView(ident, view))
        .isInstanceOf(ViewAlreadyExistsException.class)
        .hasMessageContaining(ident.name());
    assertThatThrownBy(() -> catalog.createOrReplaceView(ident, view))
        .isInstanceOf(ViewAlreadyExistsException.class)
        .hasMessageContaining(ident.name());
    verify(icebergViewCatalog, never()).createView(ident, view);
    verify(icebergViewCatalog, never()).createOrReplaceView(ident, view);
  }

  @Test
  public void viewRenameRejectsTableCollision() throws Exception {
    Identifier from = Identifier.of(new String[] {"default"}, "view");
    Identifier to = Identifier.of(new String[] {"default"}, "table");
    TableCatalog sessionCatalog = sessionCatalogWithViews();
    when(sessionCatalog.tableExists(to)).thenReturn(true);
    TableCatalog icebergCatalog = icebergCatalogWithViews();

    SparkSessionCatalog<?> catalog = catalogWithViews(icebergCatalog, sessionCatalog);

    assertThatThrownBy(() -> catalog.renameView(from, to))
        .isInstanceOf(ViewAlreadyExistsException.class)
        .hasMessageContaining(to.name());
    verify((ViewCatalog) icebergCatalog, never()).renameView(from, to);
    verify((ViewCatalog) sessionCatalog, never()).renameView(from, to);
  }

  @Test
  public void createOrReplaceViewUsesExistingSessionCatalogView()
      throws ViewAlreadyExistsException, NoSuchNamespaceException {
    Identifier viewIdent = Identifier.of(new String[] {"default"}, "session_view");
    View replacement = mock(View.class);
    View replaced = mock(View.class);

    TableCatalog sessionCatalog =
        mock(
            TableCatalog.class,
            withSettings()
                .extraInterfaces(
                    FunctionCatalog.class, SupportsNamespaces.class, ViewCatalog.class));
    ViewCatalog sessionViewCatalog = (ViewCatalog) sessionCatalog;
    when(sessionViewCatalog.viewExists(viewIdent)).thenReturn(true);
    when(sessionViewCatalog.createOrReplaceView(viewIdent, replacement)).thenReturn(replaced);

    TableCatalog icebergCatalog =
        mock(TableCatalog.class, withSettings().extraInterfaces(ViewCatalog.class));
    ViewCatalog icebergViewCatalog = (ViewCatalog) icebergCatalog;
    when(icebergViewCatalog.viewExists(viewIdent)).thenReturn(false);

    SparkSessionCatalog<?> catalog = new CatalogWithIcebergViews<>(icebergCatalog);
    catalog.initialize("spark_catalog", new CaseInsensitiveStringMap(Collections.emptyMap()));
    catalog.setDelegateCatalog(sessionCatalog);

    assertThat(catalog.createOrReplaceView(viewIdent, replacement)).isSameAs(replaced);
    verify(sessionViewCatalog).createOrReplaceView(viewIdent, replacement);
    verify(icebergViewCatalog, never()).createOrReplaceView(viewIdent, replacement);
  }

  private static TableCatalog sessionCatalogWithViews() {
    return mock(
        TableCatalog.class,
        withSettings()
            .extraInterfaces(FunctionCatalog.class, SupportsNamespaces.class, ViewCatalog.class));
  }

  private static TableCatalog icebergCatalogWithViews() {
    return mock(TableCatalog.class, withSettings().extraInterfaces(ViewCatalog.class));
  }

  private static SparkSessionCatalog<?> catalogWithViews(
      TableCatalog icebergCatalog, TableCatalog sessionCatalog) {
    SparkSessionCatalog<?> catalog = new CatalogWithIcebergViews<>(icebergCatalog);
    catalog.initialize("spark_catalog", new CaseInsensitiveStringMap(Collections.emptyMap()));
    catalog.setDelegateCatalog(sessionCatalog);
    return catalog;
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
