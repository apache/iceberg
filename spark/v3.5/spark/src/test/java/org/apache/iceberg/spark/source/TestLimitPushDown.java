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

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkValueConverter;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import scala.collection.JavaConverters;

@ExtendWith(ParameterizedTestExtension.class)
public class TestLimitPushDown {

  protected String tableName = null;
  protected Table table = null;
  protected List<Record> records = null;
  protected DataFile dataFile = null;

  private static TestHiveMetastore metastore = null;
  protected static SparkSession spark = null;
  protected static HiveCatalog catalog = null;

  // Schema passed to create tables
  public static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));

  @TempDir private Path temp;

  @Parameter private boolean vectorized;

  @Parameters(name = "vectorized = {0}")
  public static Collection<Boolean> parameters() {
    return Arrays.asList(false, true);
  }

  @BeforeEach
  public void writeTestDataFile() throws IOException {
    this.tableName = "test";
    createTable(tableName, SCHEMA);
    this.records = Lists.newArrayList();

    GenericRecord record = GenericRecord.create(table.schema());

    records.add(record.copy("id", 29, "data", "a"));
    records.add(record.copy("id", 43, "data", "b"));
    records.add(record.copy("id", 61, "data", "c"));
    records.add(record.copy("id", 89, "data", "d"));
    records.add(record.copy("id", 100, "data", "e"));
    records.add(record.copy("id", 121, "data", "f"));
    records.add(record.copy("id", 122, "data", "g"));

    this.dataFile =
        writeDataFile(
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            Row.of(0),
            records);

    table.newAppend().appendFile(dataFile).commit();
  }

  @AfterEach
  public void cleanup() throws IOException {
    dropTable("test");
  }

  @BeforeAll
  public static void startMetastoreAndSpark() {
    metastore = new TestHiveMetastore();
    metastore.start();
    HiveConf hiveConf = metastore.hiveConf();

    spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname))
            .enableHiveSupport()
            .getOrCreate();

    catalog =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(), "hive", ImmutableMap.of(), hiveConf);

    try {
      catalog.createNamespace(Namespace.of("default"));
    } catch (AlreadyExistsException ignored) {
      // the default namespace already exists. ignore the create error
    }
  }

  @AfterAll
  public static void stopMetastoreAndSpark() throws Exception {
    catalog = null;
    metastore.stop();
    metastore = null;
    spark.stop();
    spark = null;
  }

  protected void createTable(String name, Schema schema) {
    table = catalog.createTable(TableIdentifier.of("default", name), schema);
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata meta = ops.current();
    ops.commit(meta, meta.upgradeToFormatVersion(2));

    if (vectorized) {
      table
          .updateProperties()
          .set(TableProperties.PARQUET_VECTORIZATION_ENABLED, "true")
          .set(TableProperties.PARQUET_BATCH_SIZE, "4")
          .commit();
    } else {
      table.updateProperties().set(TableProperties.PARQUET_VECTORIZATION_ENABLED, "false").commit();
    }
  }

  protected void dropTable(String name) {
    catalog.dropTable(TableIdentifier.of("default", name));
  }

  private DataFile writeDataFile(OutputFile out, StructLike partition, List<Record> rows)
      throws IOException {
    FileFormat format = defaultFormat(table.properties());
    GenericAppenderFactory factory = new GenericAppenderFactory(table.schema(), table.spec());

    FileAppender<Record> writer = factory.newAppender(out, format);
    try (Closeable toClose = writer) {
      writer.addAll(rows);
    }

    return DataFiles.builder(table.spec())
        .withFormat(format)
        .withPath(out.location())
        .withPartition(partition)
        .withFileSizeInBytes(writer.length())
        .withSplitOffsets(writer.splitOffsets())
        .withMetrics(writer.metrics())
        .build();
  }

  private FileFormat defaultFormat(Map<String, String> properties) {
    String formatString = properties.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
    return FileFormat.fromString(formatString);
  }

  @TestTemplate
  public void testReadWithLimit() {
    Dataset<org.apache.spark.sql.Row> df =
        spark
            .read()
            .format("iceberg")
            .load(TableIdentifier.of("default", tableName).toString())
            .selectExpr("*");

    testLimit(df, 0, new Object[][] {});
    testLimit(df, 2, new Object[][] {{29, "a"}, {43, "b"}});
    testLimit(df, 4, new Object[][] {{29, "a"}, {43, "b"}, {61, "c"}, {89, "d"}});
    testLimit(
        df, 6, new Object[][] {{29, "a"}, {43, "b"}, {61, "c"}, {89, "d"}, {100, "e"}, {121, "f"}});
  }

  private void testLimit(
      Dataset<org.apache.spark.sql.Row> df, int limit, Object[][] expectedValues) {
    Dataset<org.apache.spark.sql.Row> limitedDf = df.limit(limit);
    LogicalPlan optimizedPlan = limitedDf.queryExecution().optimizedPlan();
    int pushedLimit = collectPushDownLimit(optimizedPlan);
    assertThat(pushedLimit).as("Pushed down limit should be " + limit).isEqualTo(limit);

    if (limit == 0) {
      assertThat(limitedDf.isEmpty()).as("Dataset should be empty when limit is 0").isTrue();
      return;
    }

    List<org.apache.spark.sql.Row> collectedRows = limitedDf.collectAsList();
    assertThat(collectedRows.size())
        .as("Number of collected rows should match expected values length")
        .isEqualTo(expectedValues.length);

    for (int i = 0; i < expectedValues.length; i++) {
      Record record = SparkValueConverter.convert(table.schema(), collectedRows.get(i));
      assertThat(record.get(0))
          .as("Table should contain expected rows")
          .isEqualTo(expectedValues[i][0]);
      assertThat(record.get(1))
          .as("Table should contain expected rows")
          .isEqualTo(expectedValues[i][1]);
    }
  }

  private int collectPushDownLimit(LogicalPlan logicalPlan) {
    Optional<Integer> limit =
        JavaConverters.asJavaCollection(logicalPlan.collectLeaves()).stream()
            .flatMap(
                plan -> {
                  if (!(plan instanceof DataSourceV2ScanRelation)) {
                    return Stream.empty();
                  }

                  DataSourceV2ScanRelation scanRelation = (DataSourceV2ScanRelation) plan;
                  if (!(scanRelation.scan() instanceof SparkBatchQueryScan)) {
                    return Stream.empty();
                  }

                  SparkBatchQueryScan batchQueryScan = (SparkBatchQueryScan) scanRelation.scan();
                  return Stream.of(batchQueryScan.pushdownLimit());
                })
            .findFirst();

    return limit.orElse(0);
  }
}
