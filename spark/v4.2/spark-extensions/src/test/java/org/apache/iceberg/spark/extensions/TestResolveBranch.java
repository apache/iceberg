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

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.PathIdentifier;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.ResolveBranch;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests the {@link ResolveBranch} rule for tables loaded by path.
 *
 * <p>The rule pins the target branch on the table during analysis and rewrites the relation
 * identifier so that further refreshes keep reading from the same branch. For a path based table
 * the branch selector is appended to the location as {@code <location>#branch_<name>}.
 *
 * <p>The target branch is taken from the read option, the identifier, or the session WAP branch.
 * Only the WAP branch reaches the rule for a path load: when the branch is set as a read option it
 * is already applied by {@code IcebergSource} while building the path identifier, so the rule finds
 * the table already pinned and leaves the identifier alone.
 */
public class TestResolveBranch extends TestBase {

  private static final String BRANCH = "test";
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), required(2, "data", Types.StringType.get()));
  private static final HadoopTables TABLES = new HadoopTables();

  @TempDir private Path temp;

  @BeforeAll
  public static void startSparkWithExtensions() {
    TestBase.spark.stop();

    TestBase.spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.driver.host", InetAddress.getLoopbackAddress().getHostAddress())
            .config("spark.testing", "true")
            .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
            .config("spark.sql.extensions", IcebergSparkSessionExtensions.class.getName())
            .config("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname))
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.legacy.respectNullabilityInTextDatasetConversion", "true")
            .config(TestBase.DISABLE_UI)
            .enableHiveSupport()
            .getOrCreate();

    TestBase.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    TestBase.catalog =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(), "hive", ImmutableMap.of(), hiveConf);
  }

  @AfterEach
  public void clearWapBranch() {
    spark.conf().unset(SparkSQLProperties.WAP_BRANCH);
  }

  @Test
  public void testReadBranchByPath() throws IOException {
    String location = tableLocation("read-branch");
    Table table = createTable(location);
    createBranch(table, BRANCH);

    // the branch comes from the session WAP conf, so the rule must pin it and rewrite the
    // identifier
    DataSourceV2Relation relation = readRelation(location);

    assertPathIdentifier(relation, location + "#branch_" + BRANCH);
    assertThat(((SparkTable) relation.table()).branch())
        .as("Table should be pinned to the target branch")
        .isEqualTo(BRANCH);
  }

  @Test
  public void testReadBranchByPathWithBranchSelector() throws IOException {
    String location = tableLocation("read-branch-selector");
    Table table = createTable(location);
    createBranch(table, BRANCH);

    // the location carries a metadata selector, so the table is not pinned while loading and the
    // rule must append the branch selector with a comma rather than a second "#"
    String pathWithSelector = location + "#files";
    DataSourceV2Relation relation = readRelation(pathWithSelector);

    assertPathIdentifier(relation, pathWithSelector);
    assertThat(((SparkTable) relation.table()).branch())
        .as("Metadata table reads are not pinned to the branch")
        .isNull();
  }

  @Test
  public void testWriteBranchByPath() throws IOException {
    String location = tableLocation("write-branch");
    Table table = createTable(location);
    createBranch(table, BRANCH);

    List<SimpleRecord> records = Lists.newArrayList(new SimpleRecord(1, "a"));
    spark
        .createDataFrame(records, SimpleRecord.class)
        .select("id", "data")
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.BRANCH, BRANCH)
        .mode("append")
        .save(location);

    Table reloaded = TABLES.load(location);
    assertThat(reloaded.newScan().useRef(BRANCH).planFiles())
        .as("Branch should receive the appended data file")
        .hasSize(2);
    assertThat(reloaded.newScan().planFiles())
        .as("Main branch should not receive data written to the branch")
        .hasSize(1);
  }

  private void assertPathIdentifier(DataSourceV2Relation relation, String expectedLocation) {
    assertThat(relation.identifier().isDefined())
        .as("Relation identifier should be set for path loaded tables")
        .isTrue();

    Identifier identifier = relation.identifier().get();
    assertThat(identifier)
        .as("Path loaded tables must use a path identifier")
        .isInstanceOf(PathIdentifier.class);
    assertThat(((PathIdentifier) identifier).location())
        .as("Location should be rewritten to include the branch selector")
        .isEqualTo(expectedLocation);
  }

  private DataSourceV2Relation readRelation(String path) {
    spark.conf().set(SparkSQLProperties.WAP_BRANCH, BRANCH);

    Dataset<Row> df = spark.read().format("iceberg").load(path);

    LogicalPlan plan = df.queryExecution().analyzed();
    List<LogicalPlan> relations = collectRelations(plan);
    assertThat(relations).as("Expected exactly one relation in the analyzed plan").hasSize(1);
    return (DataSourceV2Relation) relations.get(0);
  }

  private List<LogicalPlan> collectRelations(LogicalPlan plan) {
    List<LogicalPlan> relations = new ArrayList<>();
    plan.foreach(
        new scala.runtime.AbstractFunction1<LogicalPlan, scala.runtime.BoxedUnit>() {
          @Override
          public scala.runtime.BoxedUnit apply(LogicalPlan node) {
            if (node instanceof DataSourceV2Relation) {
              relations.add(node);
            }

            return scala.runtime.BoxedUnit.UNIT;
          }
        });

    return relations;
  }

  private String tableLocation(String name) {
    return temp.resolve(name).toFile().getAbsolutePath();
  }

  private Table createTable(String location) throws IOException {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table =
        TABLES.create(
            SCHEMA,
            spec,
            ImmutableMap.of(TableProperties.WRITE_AUDIT_PUBLISH_ENABLED, "true"),
            location);

    Record record = GenericRecord.create(SCHEMA);
    record.setField("id", 0L);
    record.setField("data", "initial");
    DataFile dataFile = writeDataFile(table, Lists.newArrayList(record));
    table.newAppend().appendFile(dataFile).commit();

    return table;
  }

  private DataFile writeDataFile(Table table, List<Record> records) throws IOException {
    File file = temp.resolve(UUID.randomUUID() + ".parquet").toFile();
    OutputFile outputFile = Files.localOutput(file);

    DataWriter<Record> dataWriter =
        Parquet.writeData(outputFile)
            .forTable(table)
            .createWriterFunc(GenericParquetWriter::create)
            .overwrite()
            .build();

    try (dataWriter) {
      for (Record record : records) {
        dataWriter.write(record);
      }
    }

    return dataWriter.toDataFile();
  }

  private void createBranch(Table table, String branch) {
    table.manageSnapshots().createBranch(branch, table.currentSnapshot().snapshotId()).commit();
  }
}
