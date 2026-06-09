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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.execution.SortExec;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
class TestSupportsReportOrdering extends TestBaseWithCatalog {

  private static final String OTHER_TABLE = "other_table";
  private static final String BUCKETED_8 = "bucketed_8";
  private static final String BUCKETED_4 = "bucketed_4";
  private static final Map<String, String> ORDERING_ENABLED = orderingConf(true);
  private static final Map<String, String> ORDERING_DISABLED = orderingConf(false);

  private static Map<String, String> orderingConf(boolean preserveOrdering) {
    return ImmutableMap.<String, String>builder()
        .put(SparkSQLProperties.PRESERVE_DATA_ORDERING, String.valueOf(preserveOrdering))
        .put(SparkSQLProperties.PRESERVE_DATA_GROUPING, "true")
        .put(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD().key(), "-1")
        .put(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "false")
        .put(SQLConf.V2_BUCKETING_ENABLED().key(), "true")
        .put(SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED().key(), "true")
        .put(SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION().key(), "false")
        .put(SQLConf.REPLACE_HASH_WITH_SORT_AGG_ENABLED().key(), "true")
        .buildOrThrow();
  }

  private static Map<String, String> orderingEnabledWith(String key, String value) {
    Map<String, String> conf = Maps.newHashMap(ORDERING_ENABLED);
    conf.put(key, value);
    return conf;
  }

  @AfterEach
  void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %s", tableName(OTHER_TABLE));
    sql("DROP TABLE IF EXISTS %s", tableName(BUCKETED_8));
    sql("DROP TABLE IF EXISTS %s", tableName(BUCKETED_4));
  }

  @TestTemplate
  void windowUsesReportedOrdering() {
    createPartitionedTable(tableName);

    assertSortRemovedAndResultUnchanged(
        "SELECT p, id, row_number() OVER (PARTITION BY p ORDER BY id) AS rn FROM %s", tableName);
  }

  @TestTemplate
  void sortMergeJoinUsesReportedOrdering() {
    createPartitionedTable(tableName);
    createPartitionedTable(tableName(OTHER_TABLE));

    assertSortRemovedAndResultUnchanged(
        "SELECT a.p, a.id, b.data FROM %s a JOIN %s b ON a.p = b.p AND a.id = b.id",
        tableName, tableName(OTHER_TABLE));
  }

  @TestTemplate
  void sortAggregateUsesReportedOrdering() {
    createPartitionedTable(tableName);

    assertSortRemovedAndResultUnchanged(
        "SELECT p, id, max(data), count(*) FROM %s GROUP BY p, id", tableName);
  }

  @TestTemplate
  void scanReturnsRowsInSortOrder() {
    createPartitionedTable(tableName);
    List<Object[]> expected = sql("SELECT id, data FROM %s WHERE p = 'A'", tableName);

    List<Object[]> rows =
        sqlWithConf(
            ORDERING_ENABLED, String.format("SELECT id, data FROM %s WHERE p = 'A'", tableName));

    assertThat(rows).containsExactlyInAnyOrderElementsOf(expected);
    assertThat(rows.stream().map(row -> (Integer) row[0]).toList()).isSorted();
  }

  @TestTemplate
  void unprojectedSortKeyIsCorrect() {
    createPartitionedTable(tableName);

    assertSortRemovedAndResultUnchanged("SELECT p, max(data) FROM %s GROUP BY p", tableName);
  }

  @TestTemplate
  void positionDeletesKeepReportedOrdering() throws IOException {
    createPartitionedTable(tableName);
    sql("INSERT INTO %s VALUES (4, 'd', 'A'), (7, 'g', 'A')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    DataFile firstFile = dataFile(table, "A", 1);
    List<Pair<CharSequence, Long>> deletes = Lists.newArrayList();
    for (long pos = 0; pos < firstFile.recordCount(); pos += 1) {
      deletes.add(Pair.of(firstFile.location(), pos));
    }

    DeleteFile posDeletes =
        FileHelpers.writeDeleteFile(
                table,
                Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
                TestHelpers.Row.of("A"),
                deletes,
                TableUtil.formatVersion(table))
            .first();
    table.newRowDelta().addDeletes(posDeletes).commit();

    withSQLConf(ORDERING_ENABLED, () -> assertThat(reportedOrdering(tableIdent)).hasSize(2));
    assertSortRemovedAndResultUnchanged(
        "SELECT p, id, row_number() OVER (PARTITION BY p ORDER BY id) AS rn FROM %s", tableName);
  }

  @TestTemplate
  void unsortedTableKeepsSplitPlanning() {
    sql(
        "CREATE TABLE %s (id INT, data STRING, p STRING) USING iceberg PARTITIONED BY (p)",
        tableName);
    sql("INSERT INTO %s VALUES (1, 'a', 'A'), (10, 'j', 'B')", tableName);
    sql("INSERT INTO %s VALUES (2, 'b', 'A'), (11, 'k', 'B')", tableName);
    Map<String, String> options = ImmutableMap.of(SparkReadOptions.SPLIT_SIZE, "1");

    withSQLConf(
        ORDERING_ENABLED,
        () -> {
          SparkPartitioningAwareScan<?> scan = buildScan(tableIdent, options, null);
          assertThat(scan.outputOrdering()).isEmpty();
          assertThat(scan.taskGroups()).hasSize(4);
        });
  }

  @TestTemplate
  void bucketedSelfJoinUsesReportedOrdering() {
    createBucketedTable(tableName(BUCKETED_8), 8);

    assertSortRemovedAndResultUnchanged(
        "SELECT a.id, b.data FROM %s a JOIN %s b ON a.id = b.id",
        tableName(BUCKETED_8), tableName(BUCKETED_8));
  }

  @TestTemplate
  void bucketedThreeWayJoinUsesReportedOrdering() {
    createBucketedTable(tableName(BUCKETED_8), 8);
    createBucketedTable(tableName(OTHER_TABLE), 8);

    assertSortRemovedAndResultUnchanged(
        "SELECT a.id, b.data, c.data FROM %s a JOIN %s b ON a.id = b.id JOIN %s c ON b.id = c.id",
        tableName(BUCKETED_8), tableName(OTHER_TABLE), tableName(BUCKETED_8));
  }

  @TestTemplate
  void bucketedJoinWithSkewedKeyIsCorrect() {
    createBucketedTable(tableName(BUCKETED_8), 8);
    createBucketedTable(tableName(OTHER_TABLE), 8);
    // every multiple of 8 lands in the same bucket, so that bucket has many more files
    for (int file = 0; file < 4; file += 1) {
      sql("INSERT INTO %s VALUES (%d, 'skew'), (%d, 'skew')", tableName(OTHER_TABLE), 8, 16);
    }

    assertResultUnchanged(
        orderingEnabledWith(
            SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED().key(), "true"),
        "SELECT a.id, b.data FROM %s a JOIN %s b ON a.id = b.id",
        tableName(BUCKETED_8),
        tableName(OTHER_TABLE));
  }

  @TestTemplate
  void compatibleBucketTransformsDisableOrdering() {
    createBucketedTable(tableName(BUCKETED_8), 8);
    createBucketedTable(tableName(BUCKETED_4), 4);
    Map<String, String> conf =
        orderingEnabledWith(SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS().key(), "true");

    withSQLConf(
        conf, () -> assertThat(reportedOrdering(identifier(tableName(BUCKETED_8)))).isEmpty());
    assertResultUnchanged(
        conf,
        "SELECT a.id, b.data FROM %s a JOIN %s b ON a.id = b.id",
        tableName(BUCKETED_8),
        tableName(BUCKETED_4));
  }

  @TestTemplate
  void joinKeysSubsetOfPartitionKeysDisablesOrdering() {
    createPartitionedTable(tableName);
    Map<String, String> conf =
        orderingEnabledWith(
            SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS().key(), "true");

    withSQLConf(conf, () -> assertThat(reportedOrdering(tableIdent)).isEmpty());
  }

  @TestTemplate
  void unpartitionedTableDoesNotReportOrdering() {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);
    setSortOrder(tableIdent, "id");
    sql("INSERT INTO %s VALUES (1, 'a'), (3, 'c')", tableName);
    sql("INSERT INTO %s VALUES (2, 'b'), (4, 'd')", tableName);

    withSQLConf(ORDERING_ENABLED, () -> assertThat(reportedOrdering(tableIdent)).isEmpty());
  }

  @TestTemplate
  void orderingWithoutGroupingIsIgnored() {
    createPartitionedTable(tableName);
    Map<String, String> conf =
        orderingEnabledWith(SparkSQLProperties.PRESERVE_DATA_GROUPING, "false");

    withSQLConf(conf, () -> assertThat(reportedOrdering(tableIdent)).isEmpty());
    assertResultUnchanged(
        conf,
        "SELECT p, id, row_number() OVER (PARTITION BY p ORDER BY id) AS rn FROM %s",
        tableName);
  }

  @TestTemplate
  void uuidSortKeyDoesNotReportOrdering() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "u", Types.UUIDType.get()),
            Types.NestedField.optional(2, "p", Types.StringType.get()));
    validationCatalog.createTable(
        tableIdent, schema, PartitionSpec.builderFor(schema).identity("p").build());
    setSortOrder(tableIdent, "p", "u");
    sql("INSERT INTO %s VALUES ('%s', 'A')", tableName, "00000000-0000-0000-0000-000000000001");
    sql("INSERT INTO %s VALUES ('%s', 'A')", tableName, "ffffffff-0000-0000-0000-000000000001");

    withSQLConf(ORDERING_ENABLED, () -> assertThat(reportedOrdering(tableIdent)).isEmpty());
  }

  @TestTemplate
  void floatingPointSortKeyNotLastDoesNotReportOrdering() {
    sql(
        "CREATE TABLE %s (id INT, score DOUBLE, p STRING) USING iceberg PARTITIONED BY (p)",
        tableName);
    setSortOrder(tableIdent, "p", "score", "id");
    sql("INSERT INTO %s VALUES (1, 0.0, 'A'), (2, 1.5, 'A')", tableName);
    sql("INSERT INTO %s VALUES (3, -0.0, 'A'), (4, 0.5, 'A')", tableName);

    withSQLConf(ORDERING_ENABLED, () -> assertThat(reportedOrdering(tableIdent)).isEmpty());
  }

  @TestTemplate
  void nestedSortKeyDoesNotReportOrdering() {
    sql(
        "CREATE TABLE %s (id INT, s STRUCT<k: INT, v: STRING>, p STRING) "
            + "USING iceberg PARTITIONED BY (p)",
        tableName);
    setSortOrder(tableIdent, "p", "s.k");
    sql("INSERT INTO %s VALUES (1, named_struct('k', 1, 'v', 'a'), 'A')", tableName);
    sql("INSERT INTO %s VALUES (2, named_struct('k', 2, 'v', 'b'), 'A')", tableName);

    withSQLConf(ORDERING_ENABLED, () -> assertThat(reportedOrdering(tableIdent)).isEmpty());
  }

  @TestTemplate
  void olderSortOrderDoesNotReportOrdering() {
    createPartitionedTable(tableName);
    setSortOrder(tableIdent, "p", "data");

    withSQLConf(ORDERING_ENABLED, () -> assertThat(reportedOrdering(tableIdent)).isEmpty());
  }

  @TestTemplate
  void equalityDeletesDoNotReportOrdering() throws IOException {
    createPartitionedTable(tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Schema deleteSchema = table.schema().select("data");
    Record delete = GenericRecord.create(deleteSchema);
    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of("A"),
            ImmutableList.of(delete.copy("data", "c")),
            deleteSchema);
    table.newRowDelta().addDeletes(eqDeletes).commit();

    withSQLConf(ORDERING_ENABLED, () -> assertThat(reportedOrdering(tableIdent)).isEmpty());
    assertResultUnchanged(
        ORDERING_ENABLED,
        "SELECT p, id, row_number() OVER (PARTITION BY p ORDER BY id) AS rn FROM %s",
        tableName);
  }

  @TestTemplate
  void tooManyFilesPerTaskRestoresSplitPlanning() {
    createPartitionedTable(tableName);
    sql("INSERT INTO %s VALUES (7, 'g', 'A')", tableName);
    Map<String, String> options =
        ImmutableMap.of(
            SparkReadOptions.SPLIT_SIZE,
            "1",
            SparkReadOptions.PRESERVE_DATA_ORDERING_MAX_FILES_PER_TASK,
            "2");

    withSQLConf(
        ORDERING_ENABLED,
        () -> {
          SparkPartitioningAwareScan<?> scan = buildScan(tableIdent, options, null);
          assertThat(scan.outputOrdering()).isEmpty();
          // partition A has 3 files and partition B has 2, each planned separately at split size 1
          assertThat(scan.taskGroups()).hasSize(5);
        });
  }

  @TestTemplate
  void filesPerTaskAtLimitReportOrdering() {
    createPartitionedTable(tableName);
    Map<String, String> options =
        ImmutableMap.of(
            SparkReadOptions.SPLIT_SIZE,
            "1",
            SparkReadOptions.PRESERVE_DATA_ORDERING_MAX_FILES_PER_TASK,
            "2");

    withSQLConf(
        ORDERING_ENABLED,
        () -> {
          SparkPartitioningAwareScan<?> scan = buildScan(tableIdent, options, null);
          assertThat(scan.outputOrdering()).hasSize(2);
          assertThat(scan.taskGroups()).hasSize(2);
        });
  }

  @TestTemplate
  void unprojectedSortKeyTrimsReportedOrdering() {
    sql(
        "CREATE TABLE %s (id INT, data STRING, p STRING) USING iceberg PARTITIONED BY (p)",
        tableName);
    setSortOrder(tableIdent, "p", "id", "data");
    sql("INSERT INTO %s VALUES (1, 'a', 'A'), (3, 'c', 'A')", tableName);
    sql("INSERT INTO %s VALUES (2, 'b', 'A'), (4, 'd', 'A')", tableName);
    StructType projection =
        new StructType().add("id", DataTypes.IntegerType).add("p", DataTypes.StringType);

    withSQLConf(
        ORDERING_ENABLED,
        () -> {
          SortOrder[] ordering =
              buildScan(tableIdent, ImmutableMap.of(), projection).outputOrdering();
          assertThat(ordering)
              .extracting(order -> order.expression().describe())
              .containsExactly("p", "id");
        });
  }

  @TestTemplate
  void runtimeFilterKeepsReportedOrdering() {
    createPartitionedTable(tableName);

    withSQLConf(
        ORDERING_ENABLED,
        () -> {
          SparkBatchQueryScan scan =
              (SparkBatchQueryScan) buildScan(tableIdent, ImmutableMap.of(), null);
          SortOrder[] ordering = scan.outputOrdering();
          assertThat(ordering).hasSize(2);

          scan.filter(new Predicate[] {partitionEquals("A")});

          assertThat(scan.outputOrdering()).isEqualTo(ordering);
          assertThat(scan.taskGroups()).hasSize(1);
          assertThat(scan.taskGroups().get(0).tasks()).hasSize(2);
        });
  }

  @TestTemplate
  void runtimeFilterKeepsRejectedOrdering() {
    createPartitionedTable(tableName);
    sql("INSERT INTO %s VALUES (7, 'g', 'A')", tableName);
    Map<String, String> options =
        ImmutableMap.of(SparkReadOptions.PRESERVE_DATA_ORDERING_MAX_FILES_PER_TASK, "2");

    withSQLConf(
        ORDERING_ENABLED,
        () -> {
          SparkBatchQueryScan scan = (SparkBatchQueryScan) buildScan(tableIdent, options, null);
          assertThat(scan.outputOrdering()).isEmpty();

          // partition B alone is within the limit, but the decision must not change
          scan.filter(new Predicate[] {partitionEquals("B")});

          assertThat(scan.outputOrdering()).isEmpty();
          assertThat(scan.isOrderingEnabled()).isFalse();
        });
  }

  @TestTemplate
  void singleFileTasksKeepVectorizedReads() {
    sql(
        "CREATE TABLE %s (id INT, data STRING, p STRING) USING iceberg PARTITIONED BY (p)",
        tableName);
    setSortOrder(tableIdent, "p", "id");
    sql("INSERT INTO %s VALUES (1, 'a', 'A'), (2, 'b', 'A'), (10, 'j', 'B')", tableName);

    withSQLConf(
        ORDERING_ENABLED,
        () -> {
          SparkPartitioningAwareScan<?> scan = buildScan(tableIdent, ImmutableMap.of(), null);
          assertThat(scan.outputOrdering()).hasSize(2);
          assertThat(scan.toBatch().createReaderFactory())
              .isInstanceOf(SparkColumnarReaderFactory.class);
        });
  }

  @TestTemplate
  void multiFileTasksUseRowReads() {
    createPartitionedTable(tableName);

    withSQLConf(
        ORDERING_ENABLED,
        () -> {
          SparkPartitioningAwareScan<?> scan = buildScan(tableIdent, ImmutableMap.of(), null);
          assertThat(scan.outputOrdering()).hasSize(2);
          assertThat(scan.toBatch().createReaderFactory())
              .isInstanceOf(SparkRowReaderFactory.class);
        });
  }

  @TestTemplate
  void copyOnWriteMergeIsCorrect() {
    createBucketedTable(tableName, 4);
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName(OTHER_TABLE));
    sql("INSERT INTO %s VALUES (1, 'new1'), (4, 'new4'), (100, 'new100')", tableName(OTHER_TABLE));
    List<Object[]> expected =
        sql(
            "SELECT coalesce(s.id, t.id), coalesce(s.data, t.data) "
                + "FROM %s t FULL OUTER JOIN %s s ON t.id = s.id",
            tableName, tableName(OTHER_TABLE));

    withSQLConf(
        ORDERING_ENABLED,
        () ->
            sql(
                "MERGE INTO %s t USING %s s ON t.id = s.id "
                    + "WHEN MATCHED THEN UPDATE SET t.data = s.data "
                    + "WHEN NOT MATCHED THEN INSERT *",
                tableName, tableName(OTHER_TABLE)));

    assertThat(sql("SELECT id, data FROM %s", tableName))
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  // rows of partition A are split across two files with interleaved and duplicate IDs
  private void createPartitionedTable(String name) {
    sql("CREATE TABLE %s (id INT, data STRING, p STRING) USING iceberg PARTITIONED BY (p)", name);
    setSortOrder(identifier(name), "p", "id");
    sql("INSERT INTO %s VALUES (1, 'a', 'A'), (3, 'c', 'A'), (5, 'e', 'A'), (10, 'j', 'B')", name);
    sql("INSERT INTO %s VALUES (2, 'b', 'A'), (3, 'x', 'A'), (6, 'f', 'A'), (11, 'k', 'B')", name);
  }

  // each bucket has three files with interleaved IDs
  private void createBucketedTable(String name, int numBuckets) {
    sql(
        "CREATE TABLE %s (id INT, data STRING) USING iceberg PARTITIONED BY (bucket(%d, id))",
        name, numBuckets);
    setSortOrder(identifier(name), "id");
    for (int file = 0; file < 3; file += 1) {
      StringBuilder values = new StringBuilder();
      for (int id = file; id < 60; id += 3) {
        values
            .append(values.length() == 0 ? "" : ", ")
            .append(String.format("(%d, 'd%d')", id, id));
      }

      sql("INSERT INTO %s VALUES %s", name, values);
    }
  }

  private TableIdentifier identifier(String name) {
    return TableIdentifier.of("default", name.substring(name.lastIndexOf('.') + 1));
  }

  private void setSortOrder(TableIdentifier ident, String... columns) {
    ReplaceSortOrder sortOrder = validationCatalog.loadTable(ident).replaceSortOrder();
    for (String column : columns) {
      sortOrder = sortOrder.asc(column);
    }

    sortOrder.commit();
  }

  private SortOrder[] reportedOrdering(TableIdentifier ident) {
    return buildScan(ident, ImmutableMap.of(), null).outputOrdering();
  }

  private SparkPartitioningAwareScan<?> buildScan(
      TableIdentifier ident, Map<String, String> options, StructType projection) {
    SparkTable sparkTable = SparkTable.create(validationCatalog.loadTable(ident), (String) null);
    ScanBuilder builder = sparkTable.newScanBuilder(new CaseInsensitiveStringMap(options));
    if (projection != null) {
      ((SparkScanBuilder) builder).pruneColumns(projection);
    }

    return (SparkPartitioningAwareScan<?>) builder.build();
  }

  private static Predicate partitionEquals(String value) {
    return new Predicate(
        "=",
        new Expression[] {
          Spark3Util.toNamedReference("p"),
          new LiteralValue<>(UTF8String.fromString(value), DataTypes.StringType)
        });
  }

  private List<Object[]> sqlWithConf(Map<String, String> conf, String query) {
    AtomicReference<List<Object[]>> rows = new AtomicReference<>();
    withSQLConf(conf, () -> rows.set(sql(query)));
    return rows.get();
  }

  private DataFile dataFile(Table table, String partition, int id) {
    try (CloseableIterable<FileScanTask> tasks =
        table
            .newScan()
            .includeColumnStats()
            .filter(Expressions.equal("p", partition))
            .planFiles()) {
      for (FileScanTask task : tasks) {
        if (task.file().lowerBounds() != null
            && Conversions.<Integer>fromByteBuffer(
                    Types.IntegerType.get(), task.file().lowerBounds().get(1))
                == id) {
          return task.file();
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    throw new IllegalStateException("No file in partition " + partition + " starts with " + id);
  }

  private void assertSortRemovedAndResultUnchanged(String query, Object... args) {
    String formattedQuery = String.format(query, args);
    AtomicReference<List<Object[]>> actual = new AtomicReference<>();
    AtomicReference<List<Object[]>> expected = new AtomicReference<>();

    withSQLConf(
        ORDERING_ENABLED,
        () -> {
          assertThat(collectPlans(executeAndKeepPlan(formattedQuery), SortExec.class)).isEmpty();
          actual.set(sql(formattedQuery));
        });

    withSQLConf(
        ORDERING_DISABLED,
        () -> {
          assertThat(collectPlans(executeAndKeepPlan(formattedQuery), SortExec.class)).isNotEmpty();
          expected.set(sql(formattedQuery));
        });

    assertThat(actual.get()).containsExactlyInAnyOrderElementsOf(expected.get());
  }

  private void assertResultUnchanged(Map<String, String> conf, String query, Object... args) {
    String formattedQuery = String.format(query, args);
    AtomicReference<List<Object[]>> actual = new AtomicReference<>();
    AtomicReference<List<Object[]>> expected = new AtomicReference<>();

    withSQLConf(conf, () -> actual.set(sql(formattedQuery)));
    withSQLConf(ORDERING_DISABLED, () -> expected.set(sql(formattedQuery)));

    assertThat(actual.get()).containsExactlyInAnyOrderElementsOf(expected.get());
  }
}
