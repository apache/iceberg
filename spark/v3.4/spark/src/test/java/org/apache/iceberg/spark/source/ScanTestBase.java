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
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.data.AvroDataTestBase;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.types.TypeUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;

/** An AvroDataScan test that validates data by reading through Spark */
public abstract class ScanTestBase extends AvroDataTestBase {
  private static final Configuration CONF = new Configuration();

  protected static SparkSession spark = null;
  protected static JavaSparkContext sc = null;

  @BeforeAll
  public static void startSpark() {
    ScanTestBase.spark =
        SparkSession.builder()
            .config("spark.driver.host", InetAddress.getLoopbackAddress().getHostAddress())
            .master("local[2]")
            .getOrCreate();
    ScanTestBase.sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
  }

  @AfterAll
  public static void stopSpark() {
    SparkSession currentSpark = ScanTestBase.spark;
    ScanTestBase.spark = null;
    ScanTestBase.sc = null;
    currentSpark.stop();
  }

  @TempDir private Path temp;

  protected void configureTable(Table table) {}

  protected abstract void writeRecords(Table table, List<GenericData.Record> records)
      throws IOException;

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    writeAndValidate(schema, schema);
  }

  @Override
  protected void writeAndValidate(Schema writeSchema, Schema expectedSchema) throws IOException {
    File parent = temp.resolve("scan_test").toFile();
    File location = new File(parent, "test");

    HadoopTables tables = new HadoopTables(CONF);
    // If V3 spec features are used, set the format version to 3
    Map<String, String> tableProperties =
        writeSchema.columns().stream()
                .anyMatch(f -> f.initialDefaultLiteral() != null || f.writeDefaultLiteral() != null)
            ? ImmutableMap.of(TableProperties.FORMAT_VERSION, "3")
            : ImmutableMap.of();
    Table table =
        tables.create(
            writeSchema, PartitionSpec.unpartitioned(), tableProperties, location.toString());

    // Important: use the table's schema for the rest of the test
    // When tables are created, the column ids are reassigned.
    List<GenericData.Record> expected = RandomData.generateList(table.schema(), 100, 1L);

    writeRecords(table, expected);

    // update the table schema to the expected schema
    if (!expectedSchema.sameSchema(table.schema())) {
      Schema expectedSchemaWithTableIds =
          TypeUtil.reassignOrRefreshIds(expectedSchema, table.schema());
      int highestFieldId =
          Math.max(table.schema().highestFieldId(), expectedSchema.highestFieldId());

      // don't use the table API because tests cover incompatible update cases
      TableOperations ops = ((BaseTable) table).operations();
      TableMetadata builder =
          TableMetadata.buildFrom(ops.current())
              .upgradeFormatVersion(3)
              .setCurrentSchema(expectedSchemaWithTableIds, highestFieldId)
              .build();
      ops.commit(ops.current(), builder);
    }

    Dataset<Row> df = spark.read().format("iceberg").load(table.location());

    List<Row> rows = df.collectAsList();
    assertThat(rows).as("Should contain 100 rows").hasSize(100);

    for (int i = 0; i < expected.size(); i += 1) {
      TestHelpers.assertEqualsSafe(table.schema().asStruct(), expected.get(i), rows.get(i));
    }
  }

  @Override
  protected boolean supportsDefaultValues() {
    return true;
  }
}
