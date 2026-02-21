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
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.ContextAwareTableCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkTestHelperBase;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests that the referenced-by view chain is correctly constructed and passed to
 * ContextAwareTableCatalog.loadTable() during view resolution.
 *
 * <p>This verifies:
 *
 * <ul>
 *   <li>Single view to table: context contains one view identifier
 *   <li>Nested view to view to table: context contains the full chain (outer first, inner last)
 *   <li>Direct table access: no context is passed
 * </ul>
 */
public class TestReferencedByViewChain extends SparkTestHelperBase {

  private static final String CATALOG_NAME = "ref_test_catalog";
  private static final Namespace NAMESPACE = Namespace.of("default");
  private static final String TABLE_NAME = "test_table";

  private static SparkSession spark;

  /**
   * An InMemoryCatalog that also implements ContextAwareTableCatalog and records all context-aware
   * loadTable calls for later assertion.
   */
  public static class ContextTrackingCatalog extends InMemoryCatalog
      implements ContextAwareTableCatalog {

    /** Records of (tableIdentifier, viewContext) captured from loadTable calls. */
    public static final List<CapturedContext> CAPTURED = new CopyOnWriteArrayList<>();

    public static class CapturedContext {
      public final TableIdentifier tableIdentifier;
      public final Map<String, Object> context;

      CapturedContext(TableIdentifier tableIdentifier, Map<String, Object> context) {
        this.tableIdentifier = tableIdentifier;
        this.context = context;
      }
    }

    public static void clearCaptured() {
      CAPTURED.clear();
    }

    @Override
    public Table loadTable(TableIdentifier identifier, Map<String, Object> loadingContext)
        throws org.apache.iceberg.exceptions.NoSuchTableException {
      CAPTURED.add(new CapturedContext(identifier, loadingContext));
      return super.loadTable(identifier);
    }
  }

  @BeforeAll
  public static void startSpark() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.driver.host", InetAddress.getLoopbackAddress().getHostAddress())
            .config("spark.sql.extensions", IcebergSparkSessionExtensions.class.getName())
            .config("spark.sql.catalog." + CATALOG_NAME, SparkCatalog.class.getName())
            .config(
                "spark.sql.catalog." + CATALOG_NAME + "." + CatalogProperties.CATALOG_IMPL,
                ContextTrackingCatalog.class.getName())
            .config("spark.sql.catalog." + CATALOG_NAME + ".default-namespace", "default")
            .config("spark.sql.catalog." + CATALOG_NAME + ".cache-enabled", "false")
            .config("spark.sql.defaultCatalog", CATALOG_NAME)
            .getOrCreate();

    spark.sql(String.format("CREATE NAMESPACE IF NOT EXISTS %s", NAMESPACE));
  }

  @AfterAll
  public static void stopSpark() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @BeforeEach
  public void before() {
    ContextTrackingCatalog.clearCaptured();

    spark.sql(String.format("USE %s.%s", CATALOG_NAME, NAMESPACE));
    spark.sql(String.format("CREATE TABLE IF NOT EXISTS %s (id INT, data STRING)", TABLE_NAME));

    try {
      List<SimpleRecord> records =
          IntStream.rangeClosed(1, 5)
              .mapToObj(i -> new SimpleRecord(i, String.valueOf(i)))
              .collect(Collectors.toList());
      Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
      df.writeTo(TABLE_NAME).append();
    } catch (org.apache.spark.sql.catalyst.analysis.NoSuchTableException e) {
      throw new RuntimeException(e);
    }

    ContextTrackingCatalog.clearCaptured();
  }

  @AfterEach
  public void after() {
    spark.sql(String.format("USE %s.%s", CATALOG_NAME, NAMESPACE));
    spark.sql("DROP VIEW IF EXISTS inner_view");
    spark.sql("DROP VIEW IF EXISTS outer_view");
    spark.sql("DROP VIEW IF EXISTS simple_view");
    spark.sql(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
    ContextTrackingCatalog.clearCaptured();
  }

  @Test
  public void testDirectTableAccessHasNoContext() {
    ContextTrackingCatalog.clearCaptured();

    List<Row> rows = spark.sql(String.format("SELECT * FROM %s", TABLE_NAME)).collectAsList();
    assertThat(rows).hasSize(5);

    // Direct table access should NOT go through the context-aware path
    // (context map is empty, so SparkCatalog.load() takes the non-context branch)
    assertThat(ContextTrackingCatalog.CAPTURED).isEmpty();
  }

  @Test
  public void testSingleViewPassesViewIdentifierInContext() {
    String viewSql = String.format("SELECT id FROM %s", TABLE_NAME);
    ViewCatalog viewCatalog = viewCatalog();
    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, "simple_view"))
        .withQuery("spark", viewSql)
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(CATALOG_NAME)
        .withSchema(SparkSchemaUtil.convert(spark.sql(viewSql).schema()))
        .create();

    ContextTrackingCatalog.clearCaptured();

    List<Row> result = spark.sql("SELECT * FROM simple_view").collectAsList();
    assertThat(result).hasSize(5);

    // Verify context was passed with the view chain
    assertThat(ContextTrackingCatalog.CAPTURED).hasSize(1);

    ContextTrackingCatalog.CapturedContext captured = ContextTrackingCatalog.CAPTURED.get(0);
    assertThat(captured.tableIdentifier).isEqualTo(TableIdentifier.of(NAMESPACE, TABLE_NAME));
    assertThat(captured.context).containsKey(ContextAwareTableCatalog.VIEW_IDENTIFIER_KEY);

    @SuppressWarnings("unchecked")
    List<TableIdentifier> viewChain =
        (List<TableIdentifier>) captured.context.get(ContextAwareTableCatalog.VIEW_IDENTIFIER_KEY);
    assertThat(viewChain).hasSize(1);
    assertThat(viewChain.get(0)).isEqualTo(TableIdentifier.of(NAMESPACE, "simple_view"));
  }

  @Test
  public void testNestedViewAccumulatesChain() {
    String innerSql = String.format("SELECT id, data FROM %s WHERE id <= 3", TABLE_NAME);
    ViewCatalog viewCatalog = viewCatalog();
    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, "inner_view"))
        .withQuery("spark", innerSql)
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(CATALOG_NAME)
        .withSchema(SparkSchemaUtil.convert(spark.sql(innerSql).schema()))
        .create();

    String outerSql = "SELECT id FROM inner_view WHERE id > 1";
    viewCatalog
        .buildView(TableIdentifier.of(NAMESPACE, "outer_view"))
        .withQuery("spark", outerSql)
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(CATALOG_NAME)
        .withSchema(SparkSchemaUtil.convert(spark.sql(outerSql).schema()))
        .create();

    ContextTrackingCatalog.clearCaptured();

    // Query through the outer view (outer_view -> inner_view -> table)
    List<Object[]> result = rowsToJava(spark.sql("SELECT * FROM outer_view").collectAsList());
    assertThat(result).hasSize(2).containsExactlyInAnyOrder(new Object[] {2}, new Object[] {3});

    // Find the captured call for the actual table (test_table) with the full chain.
    // During resolution, inner_view may also be attempted as a table first (and fail),
    // and Spark's analyzer may run rules iteratively, so filter to just the successful
    // table load with the full chain.
    List<ContextTrackingCatalog.CapturedContext> tableCaptures =
        ContextTrackingCatalog.CAPTURED.stream()
            .filter(c -> c.tableIdentifier.equals(TableIdentifier.of(NAMESPACE, TABLE_NAME)))
            .filter(c -> c.context.containsKey(ContextAwareTableCatalog.VIEW_IDENTIFIER_KEY))
            .collect(Collectors.toList());

    assertThat(tableCaptures).isNotEmpty();

    // Check the first matching capture has the full chain
    ContextTrackingCatalog.CapturedContext captured = tableCaptures.get(0);

    @SuppressWarnings("unchecked")
    List<TableIdentifier> viewChain =
        (List<TableIdentifier>) captured.context.get(ContextAwareTableCatalog.VIEW_IDENTIFIER_KEY);

    // Chain should be [outer_view, inner_view] - outermost first, innermost last
    assertThat(viewChain).hasSize(2);
    assertThat(viewChain.get(0)).isEqualTo(TableIdentifier.of(NAMESPACE, "outer_view"));
    assertThat(viewChain.get(1)).isEqualTo(TableIdentifier.of(NAMESPACE, "inner_view"));
  }

  private ViewCatalog viewCatalog() {
    org.apache.iceberg.catalog.Catalog icebergCatalog =
        Spark3Util.loadIcebergCatalog(spark, CATALOG_NAME);
    assertThat(icebergCatalog).isInstanceOf(ViewCatalog.class);
    return (ViewCatalog) icebergCatalog;
  }
}
