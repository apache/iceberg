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
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.ContextAwareCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkTestHelperBase;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.iceberg.view.View;
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
 * ContextAwareCatalog.loadTable() during view resolution.
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
   * An InMemoryCatalog that also implements ContextAwareCatalog and records all context-aware
   * loadTable calls for later assertion.
   */
  public static class ContextTrackingCatalog extends InMemoryCatalog
      implements ContextAwareCatalog {

    /** Records of (tableIdentifier, loadingContext) captured from loadTable calls. */
    public static final List<CapturedContext> CAPTURED = new CopyOnWriteArrayList<>();

    /** Records of (viewIdentifier, loadingContext) captured from loadView calls. */
    public static final List<CapturedContext> CAPTURED_VIEWS = new CopyOnWriteArrayList<>();

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
      CAPTURED_VIEWS.clear();
    }

    @Override
    public Table loadTable(TableIdentifier identifier, Map<String, Object> loadingContext)
        throws NoSuchTableException {
      CAPTURED.add(new CapturedContext(identifier, loadingContext));
      return super.loadTable(identifier);
    }

    @Override
    public View loadView(TableIdentifier identifier, Map<String, Object> loadingContext) {
      CAPTURED_VIEWS.add(new CapturedContext(identifier, loadingContext));
      return super.loadView(identifier);
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
            .config("spark.sql.iceberg.referenced-by-enabled", "true")
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
    spark.sql("DROP VIEW IF EXISTS simple_view");
    spark.sql("DROP VIEW IF EXISTS view_a");
    spark.sql("DROP VIEW IF EXISTS view_b");
    spark.sql("DROP VIEW IF EXISTS view_c");
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
    createView("simple_view", String.format("SELECT id FROM %s", TABLE_NAME));
    ContextTrackingCatalog.clearCaptured();

    List<Row> result = spark.sql("SELECT * FROM simple_view").collectAsList();
    assertThat(result).hasSize(5);

    assertThat(ContextTrackingCatalog.CAPTURED).hasSize(1);
    assertCapturedTableChain(ContextTrackingCatalog.CAPTURED, TABLE_NAME, "simple_view");
  }

  @Test
  public void testNestedViewChainAccumulatesContext() {
    // view_c -> view_b -> view_a -> table
    // When querying view_c:
    //   - loading view_a should receive context with [view_c, view_b]
    //   - loading table should receive context with [view_c, view_b, view_a]
    createView("view_a", String.format("SELECT id, data FROM %s", TABLE_NAME));
    createView("view_b", "SELECT id FROM view_a WHERE id <= 3");
    createView("view_c", "SELECT id FROM view_b WHERE id > 1");
    ContextTrackingCatalog.clearCaptured();

    List<Object[]> result = rowsToJava(spark.sql("SELECT * FROM view_c").collectAsList());
    assertThat(result).hasSize(2).containsExactlyInAnyOrder(new Object[] {2}, new Object[] {3});

    // Verify the table load has the full chain [view_c, view_b, view_a]
    assertCapturedTableChain(
        ContextTrackingCatalog.CAPTURED, TABLE_NAME, "view_c", "view_b", "view_a");

    // Verify view_a was loaded with context containing [view_c, view_b]
    assertCapturedTableChain(ContextTrackingCatalog.CAPTURED_VIEWS, "view_a", "view_c", "view_b");
  }

  private void createView(String viewName, String sql) {
    ViewCatalog catalog = viewCatalog();
    catalog
        .buildView(TableIdentifier.of(NAMESPACE, viewName))
        .withQuery("spark", sql)
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(CATALOG_NAME)
        .withSchema(SparkSchemaUtil.convert(spark.sql(sql).schema()))
        .create();
  }

  /**
   * Asserts that a captured context list contains an entry for the given target with the expected
   * view chain.
   */
  @SuppressWarnings("unchecked")
  private void assertCapturedTableChain(
      List<ContextTrackingCatalog.CapturedContext> captures,
      String targetName,
      String... expectedViewNames) {
    List<ContextTrackingCatalog.CapturedContext> matching =
        captures.stream()
            .filter(c -> c.tableIdentifier.equals(TableIdentifier.of(NAMESPACE, targetName)))
            .filter(c -> c.context.containsKey(ContextAwareCatalog.VIEW_IDENTIFIER_KEY))
            .collect(Collectors.toList());

    assertThat(matching).isNotEmpty();

    List<TableIdentifier> viewChain =
        (List<TableIdentifier>)
            matching.get(0).context.get(ContextAwareCatalog.VIEW_IDENTIFIER_KEY);
    assertThat(viewChain).hasSize(expectedViewNames.length);
    for (int i = 0; i < expectedViewNames.length; i++) {
      assertThat(viewChain.get(i)).isEqualTo(TableIdentifier.of(NAMESPACE, expectedViewNames[i]));
    }
  }

  private ViewCatalog viewCatalog() {
    Catalog icebergCatalog = Spark3Util.loadIcebergCatalog(spark, CATALOG_NAME);
    assertThat(icebergCatalog).isInstanceOf(ViewCatalog.class);
    return (ViewCatalog) icebergCatalog;
  }
}
