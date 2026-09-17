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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.LoadContext;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
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

public class TestReferencedByViewChain extends SparkTestHelperBase {

  private static final String CATALOG_NAME = "ref_test_catalog";
  private static final Namespace NAMESPACE = Namespace.of("default");
  private static final String TABLE_NAME = "test_table";

  private static SparkSession spark;

  public static class ContextTrackingCatalog extends InMemoryCatalog {
    public static final List<CapturedContext> CAPTURED = new CopyOnWriteArrayList<>();
    public static final List<CapturedContext> CAPTURED_VIEWS = new CopyOnWriteArrayList<>();

    public static class CapturedContext {
      public final TableIdentifier tableIdentifier;
      public final List<TableIdentifier> referencedBy;

      CapturedContext(TableIdentifier tableIdentifier, List<TableIdentifier> referencedBy) {
        this.tableIdentifier = tableIdentifier;
        this.referencedBy = referencedBy;
      }
    }

    public static void clearCaptured() {
      CAPTURED.clear();
      CAPTURED_VIEWS.clear();
    }

    @Override
    public Table loadTable(TableIdentifier identifier, LoadContext context) {
      CAPTURED.add(new CapturedContext(identifier, referencedBy(context)));
      return super.loadTable(identifier);
    }

    @Override
    public View loadView(TableIdentifier identifier, LoadContext context) {
      CAPTURED_VIEWS.add(new CapturedContext(identifier, referencedBy(context)));
      return super.loadView(identifier);
    }

    private static List<TableIdentifier> referencedBy(LoadContext context) {
      return context != null ? context.referencedBy() : Collections.emptyList();
    }
  }

  @BeforeAll
  public static void startSpark() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.driver.host", InetAddress.getLoopbackAddress().getHostAddress())
            .config("spark.ui.enabled", "false")
            .config(
                "spark.metrics.conf.*.sink.servlet.class",
                "org.apache.iceberg.spark.DummyMetricsServlet")
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
    spark.sql("DROP VIEW IF EXISTS simple_view");
    spark.sql("DROP VIEW IF EXISTS view_a");
    spark.sql("DROP VIEW IF EXISTS view_b");
    spark.sql("DROP VIEW IF EXISTS view_c");
    spark.sql(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
    ContextTrackingCatalog.clearCaptured();
  }

  @Test
  public void directTableAccessHasEmptyContext() {
    ContextTrackingCatalog.clearCaptured();

    List<Row> rows = spark.sql(String.format("SELECT * FROM %s", TABLE_NAME)).collectAsList();
    assertThat(rows).hasSize(5);

    assertThat(ContextTrackingCatalog.CAPTURED)
        .isNotEmpty()
        .allSatisfy(captured -> assertThat(captured.referencedBy).isEmpty());
  }

  @Test
  public void singleViewPassesViewIdentifierInContext() {
    createView("simple_view", String.format("SELECT id FROM %s", TABLE_NAME));
    ContextTrackingCatalog.clearCaptured();

    List<Row> result = spark.sql("SELECT * FROM simple_view").collectAsList();
    assertThat(result).hasSize(5);

    assertThat(ContextTrackingCatalog.CAPTURED)
        .filteredOn(captured -> !captured.referencedBy.isEmpty())
        .hasSize(1);
    assertCapturedTableChain(ContextTrackingCatalog.CAPTURED, TABLE_NAME, "simple_view");
  }

  @Test
  public void nestedViewChainAccumulatesContext() {
    createView("view_a", String.format("SELECT id, data FROM %s", TABLE_NAME));
    createView("view_b", "SELECT id FROM view_a WHERE id <= 3");
    createView("view_c", "SELECT id FROM view_b WHERE id > 1");
    ContextTrackingCatalog.clearCaptured();

    List<Object[]> result = rowsToJava(spark.sql("SELECT * FROM view_c").collectAsList());
    assertThat(result).hasSize(2).containsExactlyInAnyOrder(new Object[] {2}, new Object[] {3});

    assertCapturedTableChain(
        ContextTrackingCatalog.CAPTURED, TABLE_NAME, "view_c", "view_b", "view_a");
    assertCapturedTableChain(ContextTrackingCatalog.CAPTURED_VIEWS, "view_a", "view_c", "view_b");
  }

  private void createView(String viewName, String sql) {
    viewCatalog()
        .buildView(TableIdentifier.of(NAMESPACE, viewName))
        .withQuery("spark", sql)
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(CATALOG_NAME)
        .withSchema(SparkSchemaUtil.convert(spark.sql(sql).schema()))
        .create();
  }

  private void assertCapturedTableChain(
      List<ContextTrackingCatalog.CapturedContext> captures,
      String targetName,
      String... expectedViewNames) {
    List<ContextTrackingCatalog.CapturedContext> matching =
        captures.stream()
            .filter(
                captured ->
                    captured.tableIdentifier.equals(TableIdentifier.of(NAMESPACE, targetName)))
            .filter(captured -> captured.referencedBy != null && !captured.referencedBy.isEmpty())
            .collect(Collectors.toList());

    assertThat(matching).isNotEmpty();

    List<TableIdentifier> viewChain = matching.get(0).referencedBy;
    assertThat(viewChain).hasSize(expectedViewNames.length);
    for (int index = 0; index < expectedViewNames.length; index += 1) {
      assertThat(viewChain.get(index))
          .isEqualTo(TableIdentifier.of(NAMESPACE, expectedViewNames[index]));
    }
  }

  private ViewCatalog viewCatalog() {
    Catalog icebergCatalog = Spark3Util.loadIcebergCatalog(spark, CATALOG_NAME);
    assertThat(icebergCatalog).isInstanceOf(ViewCatalog.class);
    return (ViewCatalog) icebergCatalog;
  }
}
