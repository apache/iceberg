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

import static org.apache.iceberg.PlanningMode.DISTRIBUTED;
import static org.apache.iceberg.PlanningMode.LOCAL;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestIdentityPartitionData extends TestBase {
  private static final Configuration CONF = new Configuration();
  private static final HadoopTables TABLES = new HadoopTables(CONF);

  @Parameters(name = "format = {0}, vectorized = {1}, properties = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        "parquet",
        false,
        ImmutableMap.of(
            TableProperties.DEFAULT_FILE_FORMAT, "parquet",
            TableProperties.DATA_PLANNING_MODE, LOCAL.modeName(),
            TableProperties.DELETE_PLANNING_MODE, LOCAL.modeName())
      },
      {
        "parquet",
        true,
        ImmutableMap.of(
            TableProperties.DEFAULT_FILE_FORMAT, "parquet",
            TableProperties.DATA_PLANNING_MODE, DISTRIBUTED.modeName(),
            TableProperties.DELETE_PLANNING_MODE, DISTRIBUTED.modeName())
      },
      {
        "avro",
        false,
        ImmutableMap.of(
            TableProperties.DEFAULT_FILE_FORMAT, "avro",
            TableProperties.DATA_PLANNING_MODE, LOCAL.modeName(),
            TableProperties.DELETE_PLANNING_MODE, LOCAL.modeName())
      },
      {
        "orc",
        false,
        ImmutableMap.of(
            TableProperties.DEFAULT_FILE_FORMAT, "orc",
            TableProperties.DATA_PLANNING_MODE, DISTRIBUTED.modeName(),
            TableProperties.DELETE_PLANNING_MODE, DISTRIBUTED.modeName())
      },
      {
        "orc",
        true,
        ImmutableMap.of(
            TableProperties.DEFAULT_FILE_FORMAT, "orc",
            TableProperties.DATA_PLANNING_MODE, LOCAL.modeName(),
            TableProperties.DELETE_PLANNING_MODE, LOCAL.modeName())
      },
    };
  }

  @Parameter(index = 0)
  private String format;

  @Parameter(index = 1)
  private boolean vectorized;

  @Parameter(index = 2)
  private Map<String, String> properties;

  private static final Schema LOG_SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "date", Types.StringType.get()),
          Types.NestedField.optional(3, "level", Types.StringType.get()),
          Types.NestedField.optional(4, "message", Types.StringType.get()));

  private static final List<LogMessage> LOGS =
      ImmutableList.of(
          LogMessage.debug("2020-02-02", "debug event 1"),
          LogMessage.info("2020-02-02", "info event 1"),
          LogMessage.debug("2020-02-02", "debug event 2"),
          LogMessage.info("2020-02-03", "info event 2"),
          LogMessage.debug("2020-02-03", "debug event 3"),
          LogMessage.info("2020-02-03", "info event 3"),
          LogMessage.error("2020-02-03", "error event 1"),
          LogMessage.debug("2020-02-04", "debug event 4"),
          LogMessage.warn("2020-02-04", "warn event 1"),
          LogMessage.debug("2020-02-04", "debug event 5"));

  @TempDir private Path temp;

  private final PartitionSpec spec =
      PartitionSpec.builderFor(LOG_SCHEMA).identity("date").identity("level").build();
  private Table table = null;
  private Dataset<Row> logs = null;

  /**
   * Use the Hive Based table to make Identity Partition Columns with no duplication of the data in
   * the underlying parquet files. This makes sure that if the identity mapping fails, the test will
   * also fail.
   */
  private void setupParquet() throws Exception {
    File location = Files.createTempDirectory(temp, "logs").toFile();
    File hiveLocation = Files.createTempDirectory(temp, "hive").toFile();
    String hiveTable = "hivetable";
    assertThat(location).as("Temp folder should exist").exists();

    this.logs =
        spark.createDataFrame(LOGS, LogMessage.class).select("id", "date", "level", "message");
    spark.sql(String.format("DROP TABLE IF EXISTS %s", hiveTable));
    logs.orderBy("date", "level", "id")
        .write()
        .partitionBy("date", "level")
        .format("parquet")
        .option("path", hiveLocation.toString())
        .saveAsTable(hiveTable);

    this.table =
        TABLES.create(
            SparkSchemaUtil.schemaForTable(spark, hiveTable),
            SparkSchemaUtil.specForTable(spark, hiveTable),
            properties,
            location.toString());

    SparkTableUtil.importSparkTable(
        spark, new TableIdentifier(hiveTable), table, location.toString());
  }

  @BeforeEach
  public void setupTable() throws Exception {
    if (format.equals("parquet")) {
      setupParquet();
    } else {
      File location = Files.createTempDirectory(temp, "logs").toFile();
      assertThat(location).as("Temp folder should exist").exists();

      this.table = TABLES.create(LOG_SCHEMA, spec, properties, location.toString());
      this.logs =
          spark.createDataFrame(LOGS, LogMessage.class).select("id", "date", "level", "message");

      logs.orderBy("date", "level", "id")
          .write()
          .format("iceberg")
          .mode("append")
          .save(location.toString());
    }
  }

  @TestTemplate
  public void testFullProjection() {
    List<Row> expected = logs.orderBy("id").collectAsList();
    List<Row> actual =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.VECTORIZATION_ENABLED, String.valueOf(vectorized))
            .load(table.location())
            .orderBy("id")
            .select("id", "date", "level", "message")
            .collectAsList();
    assertThat(actual).as("Rows should match").isEqualTo(expected);
  }

  @TestTemplate
  public void testProjections() {
    String[][] cases =
        new String[][] {
          // individual fields
          new String[] {"date"},
          new String[] {"level"},
          new String[] {"message"},
          // field pairs
          new String[] {"date", "message"},
          new String[] {"level", "message"},
          new String[] {"date", "level"},
          // out-of-order pairs
          new String[] {"message", "date"},
          new String[] {"message", "level"},
          new String[] {"level", "date"},
          // full projection, different orderings
          new String[] {"date", "level", "message"},
          new String[] {"level", "date", "message"},
          new String[] {"date", "message", "level"},
          new String[] {"level", "message", "date"},
          new String[] {"message", "date", "level"},
          new String[] {"message", "level", "date"}
        };

    for (String[] ordering : cases) {
      List<Row> expected = logs.select("id", ordering).orderBy("id").collectAsList();
      List<Row> actual =
          spark
              .read()
              .format("iceberg")
              .option(SparkReadOptions.VECTORIZATION_ENABLED, String.valueOf(vectorized))
              .load(table.location())
              .select("id", ordering)
              .orderBy("id")
              .collectAsList();
      assertThat(actual)
          .as("Rows should match for ordering: " + Arrays.toString(ordering))
          .isEqualTo(expected);
    }
  }
}
