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

import static org.apache.iceberg.TestHelpers.waitUntilAfter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetAddress;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.LoadContext;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.SparkTestHelperBase;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.spark.source.HasIcebergCatalog;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.View;
import org.apache.spark.SparkThrowable;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestReferencedByViewChain extends SparkTestHelperBase {

  private static final String CATALOG_NAME = "ref_test_catalog";
  private static final String OTHER_CATALOG_NAME = "other_ref_test_catalog";
  private static final Namespace NAMESPACE = Namespace.of("default");
  private static final Namespace VIEW_DEFAULT_NAMESPACE = Namespace.of("view_defaults");
  private static final String TABLE_NAME = "test_table";
  private static final String OTHER_TABLE_NAME = "other_table";
  private static final String VIEW_DEFAULT_TABLE_NAME = "view_default_table";
  private static final String VIEW_DEFAULT_VIEW_NAME = "view_default_view";
  private static final String SESSION_CATALOG_NAME = "spark_catalog";
  private static final String PARQUET_TABLE_NAME = "parquet_table";
  private static final String PARQUET_VIEW_NAME = "parquet_view";
  private static final String SCHEMA_TABLE_NAME = "schema_table";

  @TempDir private static Path warehouse;

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
    public Table loadTable(TableIdentifier identifier) {
      CAPTURED.add(new CapturedContext(identifier, Collections.emptyList()));
      return super.loadTable(identifier);
    }

    @Override
    public Table loadTable(TableIdentifier identifier, LoadContext context) {
      CAPTURED.add(new CapturedContext(identifier, referencedBy(context)));
      return super.loadTable(identifier);
    }

    @Override
    public View loadView(TableIdentifier identifier) {
      CAPTURED_VIEWS.add(new CapturedContext(identifier, Collections.emptyList()));
      return super.loadView(identifier);
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
  static void startSpark() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.driver.host", InetAddress.getLoopbackAddress().getHostAddress())
            .config(TestBase.DISABLE_UI)
            .config("spark.sql.extensions", IcebergSparkSessionExtensions.class.getName())
            .config("spark.sql.catalog." + CATALOG_NAME, SparkCatalog.class.getName())
            .config(
                "spark.sql.catalog." + CATALOG_NAME + "." + CatalogProperties.CATALOG_IMPL,
                ContextTrackingCatalog.class.getName())
            .config("spark.sql.catalog." + CATALOG_NAME + ".default-namespace", "default")
            .config("spark.sql.catalog." + CATALOG_NAME + ".cache-enabled", "true")
            .config("spark.sql.catalog." + OTHER_CATALOG_NAME, SparkCatalog.class.getName())
            .config(
                "spark.sql.catalog." + OTHER_CATALOG_NAME + "." + CatalogProperties.CATALOG_IMPL,
                ContextTrackingCatalog.class.getName())
            .config("spark.sql.catalog." + OTHER_CATALOG_NAME + ".default-namespace", "default")
            .config("spark.sql.catalog." + OTHER_CATALOG_NAME + ".cache-enabled", "false")
            .config(
                "spark.sql.catalog." + SESSION_CATALOG_NAME, SparkSessionCatalog.class.getName())
            .config(
                "spark.sql.catalog." + SESSION_CATALOG_NAME + "." + CatalogProperties.CATALOG_IMPL,
                InMemoryCatalog.class.getName())
            .config("spark.sql.catalog." + SESSION_CATALOG_NAME + ".cache-enabled", "false")
            .config("spark.sql.warehouse.dir", warehouse.toString())
            .config("spark.sql.defaultCatalog", CATALOG_NAME)
            // Cover Hive-enabled sessions, whose analyzer is built separately from the default one
            .config("spark.hadoop.hive.metastore.uris", "")
            .config(
                "spark.hadoop.javax.jdo.option.ConnectionURL",
                "jdbc:derby:memory:" + UUID.randomUUID() + ";create=true")
            .enableHiveSupport()
            .getOrCreate();

    spark.sql(String.format("CREATE NAMESPACE IF NOT EXISTS %s.%s", CATALOG_NAME, NAMESPACE));
    spark.sql(
        String.format(
            "CREATE NAMESPACE IF NOT EXISTS %s.%s", CATALOG_NAME, VIEW_DEFAULT_NAMESPACE));
    spark.sql(String.format("CREATE NAMESPACE IF NOT EXISTS %s.%s", OTHER_CATALOG_NAME, NAMESPACE));
    ((SupportsNamespaces) Spark3Util.loadIcebergCatalog(spark, SESSION_CATALOG_NAME))
        .createNamespace(NAMESPACE);
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @BeforeEach
  void before() {
    spark.sql(String.format("USE %s.%s", CATALOG_NAME, NAMESPACE));
    spark.sql(String.format("CREATE TABLE IF NOT EXISTS %s (id INT, data STRING)", TABLE_NAME));
    appendRecords(TABLE_NAME);
    spark.sql(String.format("REFRESH TABLE %s", TABLE_NAME));

    ContextTrackingCatalog.clearCaptured();
  }

  @AfterEach
  void after() {
    spark.conf().unset(SparkSQLProperties.REFERENCED_BY_ENABLED);
    spark.sql(String.format("USE %s.%s", CATALOG_NAME, NAMESPACE));
    spark.sql("DROP VIEW IF EXISTS simple_view");
    spark.sql("DROP VIEW IF EXISTS view_a");
    spark.sql("DROP VIEW IF EXISTS view_b");
    spark.sql("DROP VIEW IF EXISTS view_c");
    spark.sql("DROP VIEW IF EXISTS time_travel_view");
    spark.sql("DROP VIEW IF EXISTS cross_view");
    spark.sql("DROP VIEW IF EXISTS shadow_v");
    spark.sql(String.format("DROP VIEW IF EXISTS %s", VIEW_DEFAULT_VIEW_NAME));
    spark.sql(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
    spark.sql(String.format("DROP TABLE IF EXISTS %s", SCHEMA_TABLE_NAME));
    spark.sql(
        String.format(
            "DROP TABLE IF EXISTS %s.%s", VIEW_DEFAULT_NAMESPACE, VIEW_DEFAULT_TABLE_NAME));
    spark.sql(
        String.format(
            "DROP TABLE IF EXISTS %s.%s.%s", OTHER_CATALOG_NAME, NAMESPACE, OTHER_TABLE_NAME));
    viewCatalog(SESSION_CATALOG_NAME).dropView(TableIdentifier.of(NAMESPACE, PARQUET_VIEW_NAME));
    spark.sql(
        String.format(
            "DROP TABLE IF EXISTS %s.%s.%s", SESSION_CATALOG_NAME, NAMESPACE, PARQUET_TABLE_NAME));
    ContextTrackingCatalog.clearCaptured();
  }

  @Test
  void referencedByIsDisabledByDefault() {
    createView("simple_view", String.format("SELECT id FROM %s", TABLE_NAME));
    ContextTrackingCatalog.clearCaptured();

    List<Row> result = spark.sql("SELECT * FROM simple_view").collectAsList();
    assertThat(result).hasSize(5);

    assertThat(ContextTrackingCatalog.CAPTURED)
        .allSatisfy(captured -> assertThat(captured.referencedBy).isEmpty());
    assertThat(ContextTrackingCatalog.CAPTURED_VIEWS)
        .allSatisfy(captured -> assertThat(captured.referencedBy).isEmpty());
  }

  @Test
  void singleViewPassesViewIdentifierInContext() {
    setReferencedByEnabled(true);
    createView("simple_view", String.format("SELECT id FROM %s", TABLE_NAME));
    spark.sql(String.format("REFRESH TABLE %s", TABLE_NAME));
    ContextTrackingCatalog.clearCaptured();

    List<Row> result = spark.sql("SELECT * FROM simple_view").collectAsList();
    assertThat(result).hasSize(5);

    assertThat(capturedLoadsOf(TableIdentifier.of(NAMESPACE, TABLE_NAME)))
        .isNotEmpty()
        .allSatisfy(
            captured ->
                assertThat(captured.referencedBy)
                    .containsExactly(TableIdentifier.of(NAMESPACE, "simple_view")));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void viewBodyResolvesTableQualifiedColumns(boolean referencedByEnabled) {
    setReferencedByEnabled(referencedByEnabled);
    createView("view_a", String.format("SELECT %s.id FROM %s", TABLE_NAME, TABLE_NAME));
    createView(
        "view_b",
        String.format(
            "SELECT %s.%s.%s.id FROM %s.%s.%s",
            CATALOG_NAME, NAMESPACE, TABLE_NAME, CATALOG_NAME, NAMESPACE, TABLE_NAME));

    assertThat(spark.sql("SELECT * FROM view_a").collectAsList()).hasSize(5);
    assertThat(spark.sql("SELECT * FROM view_b").collectAsList()).hasSize(5);
  }

  @Test
  void referencedByUsesResolvedViewIdentifier() {
    setReferencedByEnabled(true);
    spark.sql(
        String.format(
            "CREATE TABLE %s.%s (id INT, data STRING)",
            VIEW_DEFAULT_NAMESPACE, VIEW_DEFAULT_TABLE_NAME));
    appendRecords(String.format("%s.%s", VIEW_DEFAULT_NAMESPACE, VIEW_DEFAULT_TABLE_NAME));
    spark.sql(
        String.format("REFRESH TABLE %s.%s", VIEW_DEFAULT_NAMESPACE, VIEW_DEFAULT_TABLE_NAME));

    String viewSql = String.format("SELECT id FROM %s", VIEW_DEFAULT_TABLE_NAME);
    String schemaSql =
        String.format("SELECT id FROM %s.%s", VIEW_DEFAULT_NAMESPACE, VIEW_DEFAULT_TABLE_NAME);
    viewCatalog(CATALOG_NAME)
        .buildView(TableIdentifier.of(NAMESPACE, VIEW_DEFAULT_VIEW_NAME))
        .withQuery("spark", viewSql)
        .withDefaultNamespace(VIEW_DEFAULT_NAMESPACE)
        .withDefaultCatalog(CATALOG_NAME)
        .withSchema(SparkSchemaUtil.convert(spark.sql(schemaSql).schema()))
        .create();
    ContextTrackingCatalog.clearCaptured();

    List<Row> result =
        spark.sql(String.format("SELECT * FROM %s", VIEW_DEFAULT_VIEW_NAME)).collectAsList();
    assertThat(result).hasSize(5);

    assertCapturedTableChain(
        ContextTrackingCatalog.CAPTURED,
        VIEW_DEFAULT_NAMESPACE,
        VIEW_DEFAULT_TABLE_NAME,
        VIEW_DEFAULT_VIEW_NAME);
  }

  @Test
  void nestedViewChainAccumulatesContext() {
    setReferencedByEnabled(true);
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

  @Test
  void timeTravelViewPassesViewIdentifierInContext() throws Exception {
    setReferencedByEnabled(true);
    long snapshotId = Spark3Util.loadIcebergTable(spark, TABLE_NAME).currentSnapshot().snapshotId();
    createView(
        "time_travel_view",
        String.format("SELECT id FROM %s VERSION AS OF %d", TABLE_NAME, snapshotId));
    ContextTrackingCatalog.clearCaptured();

    List<Row> result = spark.sql("SELECT * FROM time_travel_view").collectAsList();
    assertThat(result).hasSize(5);

    assertCapturedTableChain(ContextTrackingCatalog.CAPTURED, TABLE_NAME, "time_travel_view");
  }

  @Test
  void crossCatalogViewAccessDoesNotFailWhenReferencedByIsDisabled() {
    createCrossCatalogView();
    ContextTrackingCatalog.clearCaptured();

    List<Row> result = spark.sql("SELECT * FROM cross_view").collectAsList();
    assertThat(result).hasSize(5);

    assertThat(capturedLoadsOf(TableIdentifier.of(NAMESPACE, OTHER_TABLE_NAME)))
        .isNotEmpty()
        .allSatisfy(captured -> assertThat(captured.referencedBy).isEmpty());
  }

  @Test
  void crossCatalogViewAccessFailsWhenReferencedByIsEnabled() {
    setReferencedByEnabled(true);
    createCrossCatalogView();

    assertThatThrownBy(() -> spark.sql("SELECT * FROM cross_view").collectAsList())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cross-catalog view references are not supported with referenced-by enabled. "
                + "Views from catalogs [%s] cannot reference entities in catalog [%s]",
            CATALOG_NAME, OTHER_CATALOG_NAME);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void versionTimeTravelViewReadsPinnedSnapshot(boolean referencedByEnabled) throws Exception {
    setReferencedByEnabled(referencedByEnabled);
    long snapshotId = Spark3Util.loadIcebergTable(spark, TABLE_NAME).currentSnapshot().snapshotId();
    createView(
        "time_travel_view",
        String.format("SELECT id FROM %s VERSION AS OF %d", TABLE_NAME, snapshotId));
    appendRecords(TABLE_NAME);

    List<Row> result = spark.sql("SELECT * FROM time_travel_view").collectAsList();
    assertThat(result).hasSize(5);
  }

  @Test
  void timestampStringTimeTravelView() throws Exception {
    setReferencedByEnabled(true);
    long asOfMillis = waitUntilAfter(currentSnapshotMillis());
    String asOf = new Timestamp(asOfMillis).toString();
    createView(
        "time_travel_view",
        String.format("SELECT id FROM %s TIMESTAMP AS OF '%s'", TABLE_NAME, asOf));
    waitUntilAfter(asOfMillis);
    appendRecords(TABLE_NAME);
    ContextTrackingCatalog.clearCaptured();

    List<Row> result = spark.sql("SELECT * FROM time_travel_view").collectAsList();
    assertThat(result).hasSize(5);

    assertCapturedTableChain(ContextTrackingCatalog.CAPTURED, TABLE_NAME, "time_travel_view");
  }

  @Test
  void timestampSecondsTimeTravelView() throws Exception {
    setReferencedByEnabled(true);
    long asOfSeconds = TimeUnit.MILLISECONDS.toSeconds(currentSnapshotMillis()) + 1;
    createView(
        "time_travel_view",
        String.format("SELECT id FROM %s TIMESTAMP AS OF %d", TABLE_NAME, asOfSeconds));
    waitUntilAfter(TimeUnit.SECONDS.toMillis(asOfSeconds));
    appendRecords(TABLE_NAME);
    ContextTrackingCatalog.clearCaptured();

    List<Row> result = spark.sql("SELECT * FROM time_travel_view").collectAsList();
    assertThat(result).hasSize(5);

    assertCapturedTableChain(ContextTrackingCatalog.CAPTURED, TABLE_NAME, "time_travel_view");
  }

  @Test
  void timestampExpressionTimeTravelView() {
    setReferencedByEnabled(true);
    createView(
        "time_travel_view",
        String.format("SELECT id FROM %s TIMESTAMP AS OF current_timestamp()", TABLE_NAME));
    ContextTrackingCatalog.clearCaptured();

    List<Row> result = spark.sql("SELECT * FROM time_travel_view").collectAsList();
    assertThat(result).hasSize(5);

    assertCapturedTableChain(ContextTrackingCatalog.CAPTURED, TABLE_NAME, "time_travel_view");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void directReadOfNonIcebergTableInSessionCatalog(boolean referencedByEnabled) {
    setReferencedByEnabled(referencedByEnabled);
    String parquetTable = createSessionCatalogParquetTable();

    assertThat(spark.sql(String.format("SELECT * FROM %s", parquetTable)).count()).isEqualTo(3);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void viewReadsNonIcebergTableInSessionCatalog(boolean referencedByEnabled) {
    setReferencedByEnabled(referencedByEnabled);
    String parquetTable = createSessionCatalogParquetTable();
    createView(
        SESSION_CATALOG_NAME, PARQUET_VIEW_NAME, String.format("SELECT id FROM %s", parquetTable));

    assertThat(
            spark
                .sql(
                    String.format(
                        "SELECT * FROM %s.%s.%s",
                        SESSION_CATALOG_NAME, NAMESPACE, PARQUET_VIEW_NAME))
                .count())
        .isEqualTo(3);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void directTableReadDoesNotLoadView(boolean referencedByEnabled) {
    setReferencedByEnabled(referencedByEnabled);
    spark.sql(String.format("SELECT * FROM %s", TABLE_NAME)).collectAsList();

    assertThat(ContextTrackingCatalog.CAPTURED_VIEWS).isEmpty();
  }

  @Test
  void directTableReadLoadsTableAsOftenAsWhenReferencedByIsDisabled() {
    String otherTable = String.format("%s.%s.%s", OTHER_CATALOG_NAME, NAMESPACE, OTHER_TABLE_NAME);
    spark.sql(String.format("CREATE TABLE %s (id INT, data STRING)", otherTable));
    appendRecords(otherTable);
    TableIdentifier ident = TableIdentifier.of(NAMESPACE, OTHER_TABLE_NAME);

    ContextTrackingCatalog.clearCaptured();
    spark.sql(String.format("SELECT * FROM %s", otherTable)).collectAsList();
    int expectedLoads = capturedLoadsOf(ident).size();

    setReferencedByEnabled(true);
    ContextTrackingCatalog.clearCaptured();
    spark.sql(String.format("SELECT * FROM %s", otherTable)).collectAsList();

    assertThat(expectedLoads).isPositive();
    assertThat(capturedLoadsOf(ident)).hasSize(expectedLoads);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void tempViewShadowsIcebergView(boolean referencedByEnabled) {
    setReferencedByEnabled(referencedByEnabled);
    createView("shadow_v", String.format("SELECT id FROM %s", TABLE_NAME));
    spark.sql("CREATE TEMPORARY VIEW shadow_v AS SELECT 'x' AS temp_col");
    try {
      List<String> columns =
          spark.sql("DESCRIBE TABLE shadow_v").collectAsList().stream()
              .map(row -> row.getString(0))
              .collect(Collectors.toList());

      assertThat(columns).contains("temp_col").doesNotContain("id");
    } finally {
      spark.catalog().dropTempView("shadow_v");
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void cachedViewIsReusedByLaterQuery(boolean referencedByEnabled) {
    setReferencedByEnabled(referencedByEnabled);
    createView("simple_view", String.format("SELECT id FROM %s", TABLE_NAME));
    Dataset<Row> cached = spark.sql("SELECT * FROM simple_view");
    cached.cache();
    try {
      cached.count();
      Dataset<Row> query = spark.sql("SELECT * FROM simple_view");

      assertThat(query.queryExecution().withCachedData().toString()).contains("InMemoryRelation");
    } finally {
      cached.unpersist();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void streamingReadOfViewFails(boolean referencedByEnabled) {
    setReferencedByEnabled(referencedByEnabled);
    createView("simple_view", String.format("SELECT id FROM %s", TABLE_NAME));

    assertThatThrownBy(() -> spark.readStream().table("simple_view"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "is a permanent view, which is not supported by streaming reading API");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void deleteFromViewFails(boolean referencedByEnabled) {
    setReferencedByEnabled(referencedByEnabled);
    createView("simple_view", String.format("SELECT id FROM %s", TABLE_NAME));

    assertFailsWithCondition(
        () -> spark.sql("DELETE FROM simple_view WHERE id = 1"), "TABLE_OR_VIEW_NOT_FOUND");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void createOrReplaceViewWithCycleFails(boolean referencedByEnabled) {
    setReferencedByEnabled(referencedByEnabled);
    createView("view_a", String.format("SELECT id FROM %s", TABLE_NAME));
    createView("view_b", "SELECT id FROM view_a");

    assertFailsWithCondition(
        () -> spark.sql("CREATE OR REPLACE VIEW view_a AS SELECT id FROM view_b"),
        "RECURSIVE_VIEW");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void viewCycleFails(boolean referencedByEnabled) {
    setReferencedByEnabled(referencedByEnabled);
    createViewWithoutAnalysis("view_a", "SELECT id FROM view_b");
    createViewWithoutAnalysis("view_b", "SELECT id FROM view_a");

    assertFailsWithCondition(
        () -> spark.sql("SELECT * FROM view_a").collectAsList(), "VIEW_EXCEED_MAX_NESTED_DEPTH");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void alterViewPropertiesUncachesView(boolean referencedByEnabled) {
    setReferencedByEnabled(referencedByEnabled);
    createView("simple_view", String.format("SELECT id FROM %s", TABLE_NAME));
    spark.catalog().cacheTable("simple_view");

    spark.sql("ALTER VIEW simple_view SET TBLPROPERTIES ('key' = 'value')");

    assertThat(spark.catalog().isCached("simple_view")).isFalse();
  }

  @Test
  void viewCommandsMatchReferencedByDisabled() {
    createView("simple_view", String.format("SELECT id FROM %s", TABLE_NAME));

    for (String command : List.of("DESCRIBE TABLE simple_view", "SHOW CREATE TABLE simple_view")) {
      setReferencedByEnabled(false);
      List<Row> expected = spark.sql(command).collectAsList();

      setReferencedByEnabled(true);
      assertThat(spark.sql(command).collectAsList()).as(command).isEqualTo(expected);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void invalidViewSqlFails(boolean referencedByEnabled) {
    setReferencedByEnabled(referencedByEnabled);
    createViewWithoutAnalysis("simple_view", "invalid SQL");

    assertFailsWithCondition(
        () -> spark.sql("SELECT * FROM simple_view").collectAsList(), "PARSE_SYNTAX_ERROR");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void viewReadingMissingTableFails(boolean referencedByEnabled) {
    setReferencedByEnabled(referencedByEnabled);
    createViewWithoutAnalysis("simple_view", "SELECT id FROM non_existing");

    assertFailsWithCondition(
        () -> spark.sql("SELECT * FROM simple_view").collectAsList(), "TABLE_OR_VIEW_NOT_FOUND");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void viewColumnsBindByName(boolean referencedByEnabled) {
    setReferencedByEnabled(referencedByEnabled);
    createSchemaTable();
    spark.sql(String.format("CREATE VIEW simple_view AS SELECT * FROM %s", SCHEMA_TABLE_NAME));
    spark.sql(String.format("ALTER TABLE %s ALTER COLUMN b FIRST", SCHEMA_TABLE_NAME));

    assertThat(rowsToJava(spark.sql("SELECT * FROM simple_view").collectAsList()))
        .containsExactlyInAnyOrder(new Object[] {1, 10}, new Object[] {2, 20});
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void schemaEvolutionViewFailsAfterSchemaChange(boolean referencedByEnabled) {
    setReferencedByEnabled(referencedByEnabled);
    createSchemaTable();
    spark.sql(
        String.format(
            "CREATE VIEW simple_view WITH SCHEMA EVOLUTION AS SELECT * FROM %s",
            SCHEMA_TABLE_NAME));
    spark.sql(String.format("ALTER TABLE %s ADD COLUMN c INT", SCHEMA_TABLE_NAME));

    assertFailsWithCondition(
        () -> spark.sql("SELECT * FROM simple_view").collectAsList(), "FAILED_UPDATE_VIEW_SCHEMA");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void typeEvolutionViewFailsAfterTypeChange(boolean referencedByEnabled) {
    setReferencedByEnabled(referencedByEnabled);
    createSchemaTable();
    spark.sql(
        String.format(
            "CREATE VIEW simple_view WITH SCHEMA TYPE EVOLUTION AS SELECT * FROM %s",
            SCHEMA_TABLE_NAME));
    spark.sql(String.format("ALTER TABLE %s ALTER COLUMN a TYPE BIGINT", SCHEMA_TABLE_NAME));

    assertFailsWithCondition(
        () -> spark.sql("SELECT * FROM simple_view").collectAsList(), "FAILED_UPDATE_VIEW_SCHEMA");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void viewTextParsesWithStoredSqlConfigs(boolean referencedByEnabled) {
    setReferencedByEnabled(referencedByEnabled);
    createSchemaTable();
    spark.conf().set("spark.sql.ansi.doubleQuotedIdentifiers", "true");
    try {
      spark.sql(
          String.format("CREATE VIEW simple_view AS SELECT \"a\" FROM %s", SCHEMA_TABLE_NAME));
    } finally {
      spark.conf().unset("spark.sql.ansi.doubleQuotedIdentifiers");
    }

    assertThat(rowsToJava(spark.sql("SELECT * FROM simple_view").collectAsList()))
        .containsExactlyInAnyOrder(new Object[] {1}, new Object[] {2});
  }

  private void createSchemaTable() {
    spark.sql(String.format("CREATE TABLE %s (a INT, b INT)", SCHEMA_TABLE_NAME));
    spark.sql(String.format("INSERT INTO %s VALUES (1, 10), (2, 20)", SCHEMA_TABLE_NAME));
  }

  private void setReferencedByEnabled(boolean enabled) {
    spark.conf().set(SparkSQLProperties.REFERENCED_BY_ENABLED, String.valueOf(enabled));
  }

  private void assertFailsWithCondition(ThrowingCallable action, String condition) {
    assertThatThrownBy(action)
        .isInstanceOf(SparkThrowable.class)
        .hasMessageContaining("[" + condition + "]");
  }

  private String createSessionCatalogParquetTable() {
    String parquetTable =
        String.format("%s.%s.%s", SESSION_CATALOG_NAME, NAMESPACE, PARQUET_TABLE_NAME);
    spark.sql(String.format("CREATE TABLE %s (id INT) USING parquet", parquetTable));
    spark.sql(String.format("INSERT INTO %s VALUES (1), (2), (3)", parquetTable));
    return parquetTable;
  }

  private long currentSnapshotMillis() throws Exception {
    return Spark3Util.loadIcebergTable(spark, TABLE_NAME).currentSnapshot().timestampMillis();
  }

  private List<ContextTrackingCatalog.CapturedContext> capturedLoadsOf(TableIdentifier ident) {
    return capturedLoadsOf(ContextTrackingCatalog.CAPTURED, ident);
  }

  private List<ContextTrackingCatalog.CapturedContext> capturedLoadsOf(
      List<ContextTrackingCatalog.CapturedContext> captures, TableIdentifier ident) {
    return captures.stream()
        .filter(captured -> captured.tableIdentifier.equals(ident))
        .collect(Collectors.toList());
  }

  private void createView(String viewName, String sql) {
    createView(CATALOG_NAME, viewName, sql);
  }

  private void createView(String catalogName, String viewName, String sql) {
    createView(catalogName, viewName, sql, SparkSchemaUtil.convert(spark.sql(sql).schema()));
  }

  private void createViewWithoutAnalysis(String viewName, String sql) {
    createView(
        CATALOG_NAME,
        viewName,
        sql,
        new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get())));
  }

  private void createView(String catalogName, String viewName, String sql, Schema schema) {
    viewCatalog(catalogName)
        .buildView(TableIdentifier.of(NAMESPACE, viewName))
        .withQuery("spark", sql)
        .withDefaultNamespace(NAMESPACE)
        .withDefaultCatalog(catalogName)
        .withSchema(schema)
        .create();
  }

  private void createCrossCatalogView() {
    String otherTable = String.format("%s.%s.%s", OTHER_CATALOG_NAME, NAMESPACE, OTHER_TABLE_NAME);
    spark.sql(String.format("CREATE TABLE %s (id INT, data STRING)", otherTable));
    appendRecords(otherTable);
    spark.sql(String.format("REFRESH TABLE %s", otherTable));
    createView("cross_view", String.format("SELECT id FROM %s", otherTable));
    spark.sql(String.format("REFRESH TABLE %s", otherTable));
  }

  private void appendRecords(String tableName) {
    try {
      List<SimpleRecord> records =
          IntStream.rangeClosed(1, 5)
              .mapToObj(i -> new SimpleRecord(i, String.valueOf(i)))
              .collect(Collectors.toList());
      Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
      df.writeTo(tableName).append();
    } catch (NoSuchTableException e) {
      throw new RuntimeException(e);
    }
  }

  private void assertCapturedTableChain(
      List<ContextTrackingCatalog.CapturedContext> captures,
      String targetName,
      String... expectedViewNames) {
    assertCapturedTableChain(captures, NAMESPACE, targetName, expectedViewNames);
  }

  private void assertCapturedTableChain(
      List<ContextTrackingCatalog.CapturedContext> captures,
      Namespace targetNamespace,
      String targetName,
      String... expectedViewNames) {
    List<ContextTrackingCatalog.CapturedContext> matching =
        capturedLoadsOf(captures, TableIdentifier.of(targetNamespace, targetName)).stream()
            .filter(captured -> !captured.referencedBy.isEmpty())
            .collect(Collectors.toList());

    assertThat(matching).isNotEmpty();
    assertThat(matching.get(0).referencedBy)
        .containsExactly(
            Arrays.stream(expectedViewNames)
                .map(name -> TableIdentifier.of(NAMESPACE, name))
                .toArray(TableIdentifier[]::new));
  }

  private ViewCatalog viewCatalog(String catalogName) {
    CatalogPlugin catalogPlugin = spark.sessionState().catalogManager().catalog(catalogName);
    assertThat(catalogPlugin).isInstanceOf(HasIcebergCatalog.class);
    ViewCatalog viewCatalog = ((HasIcebergCatalog) catalogPlugin).icebergViewCatalog();
    assertThat(viewCatalog).isNotNull();
    return viewCatalog;
  }
}
