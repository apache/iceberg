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

import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION;
import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION_LEVEL;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DELETE_AVRO_COMPRESSION;
import static org.apache.iceberg.TableProperties.DELETE_AVRO_COMPRESSION_LEVEL;
import static org.apache.iceberg.TableProperties.DELETE_DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DELETE_DISTRIBUTION_MODE;
import static org.apache.iceberg.TableProperties.DELETE_ORC_COMPRESSION;
import static org.apache.iceberg.TableProperties.DELETE_ORC_COMPRESSION_STRATEGY;
import static org.apache.iceberg.TableProperties.DELETE_PARQUET_COMPRESSION;
import static org.apache.iceberg.TableProperties.DELETE_PARQUET_COMPRESSION_LEVEL;
import static org.apache.iceberg.TableProperties.MERGE_DISTRIBUTION_MODE;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION_STRATEGY;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION_LEVEL;
import static org.apache.iceberg.TableProperties.UPDATE_DISTRIBUTION_MODE;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_HASH;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_NONE;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_RANGE;
import static org.apache.iceberg.spark.SparkSQLProperties.COMPRESSION_CODEC;
import static org.apache.iceberg.spark.SparkSQLProperties.COMPRESSION_LEVEL;
import static org.apache.spark.sql.connector.write.RowLevelOperation.Command.DELETE;
import static org.apache.spark.sql.connector.write.RowLevelOperation.Command.MERGE;
import static org.apache.spark.sql.connector.write.RowLevelOperation.Command.UPDATE;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestSparkWriteConf extends SparkTestBaseWithCatalog {

  @Before
  public void before() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);
  }

  @After
  public void after() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testSparkWriteConfDistributionDefault() {
    Table table = validationCatalog.loadTable(tableIdent);

    SparkWriteConf writeConf = new SparkWriteConf(spark, table, ImmutableMap.of());

    checkMode(DistributionMode.HASH, writeConf);
  }

  @Test
  public void testSparkWriteConfDistributionModeWithWriteOption() {
    Table table = validationCatalog.loadTable(tableIdent);

    Map<String, String> writeOptions =
        ImmutableMap.of(SparkWriteOptions.DISTRIBUTION_MODE, DistributionMode.NONE.modeName());

    SparkWriteConf writeConf = new SparkWriteConf(spark, table, writeOptions);
    checkMode(DistributionMode.NONE, writeConf);
  }

  @Test
  public void testSparkWriteConfDistributionModeWithSessionConfig() {
    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.DISTRIBUTION_MODE, DistributionMode.NONE.modeName()),
        () -> {
          Table table = validationCatalog.loadTable(tableIdent);
          SparkWriteConf writeConf = new SparkWriteConf(spark, table, ImmutableMap.of());
          checkMode(DistributionMode.NONE, writeConf);
        });
  }

  @Test
  public void testSparkWriteConfDistributionModeWithTableProperties() {
    Table table = validationCatalog.loadTable(tableIdent);

    table
        .updateProperties()
        .set(WRITE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE)
        .set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE)
        .set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE)
        .set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_NONE)
        .commit();

    SparkWriteConf writeConf = new SparkWriteConf(spark, table, ImmutableMap.of());
    checkMode(DistributionMode.NONE, writeConf);
  }

  @Test
  public void testSparkWriteConfDistributionModeWithTblPropAndSessionConfig() {
    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.DISTRIBUTION_MODE, DistributionMode.NONE.modeName()),
        () -> {
          Table table = validationCatalog.loadTable(tableIdent);

          table
              .updateProperties()
              .set(WRITE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE)
              .set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE)
              .set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE)
              .set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE)
              .commit();

          SparkWriteConf writeConf = new SparkWriteConf(spark, table, ImmutableMap.of());
          // session config overwrite the table properties
          checkMode(DistributionMode.NONE, writeConf);
        });
  }

  @Test
  public void testSparkWriteConfDistributionModeWithWriteOptionAndSessionConfig() {
    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.DISTRIBUTION_MODE, DistributionMode.RANGE.modeName()),
        () -> {
          Table table = validationCatalog.loadTable(tableIdent);

          Map<String, String> writeOptions =
              ImmutableMap.of(
                  SparkWriteOptions.DISTRIBUTION_MODE, DistributionMode.NONE.modeName());

          SparkWriteConf writeConf = new SparkWriteConf(spark, table, writeOptions);
          // write options overwrite the session config
          checkMode(DistributionMode.NONE, writeConf);
        });
  }

  @Test
  public void testSparkWriteConfDistributionModeWithEverything() {
    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.DISTRIBUTION_MODE, DistributionMode.RANGE.modeName()),
        () -> {
          Table table = validationCatalog.loadTable(tableIdent);

          Map<String, String> writeOptions =
              ImmutableMap.of(
                  SparkWriteOptions.DISTRIBUTION_MODE, DistributionMode.NONE.modeName());

          table
              .updateProperties()
              .set(WRITE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH)
              .set(DELETE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH)
              .set(UPDATE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH)
              .set(MERGE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH)
              .commit();

          SparkWriteConf writeConf = new SparkWriteConf(spark, table, writeOptions);
          // write options take the highest priority
          checkMode(DistributionMode.NONE, writeConf);
        });
  }

  @Test
  public void testSparkConfOverride() {
    List<List<Map<String, String>>> propertiesSuites =
        Lists.newArrayList(
            Lists.newArrayList(
                ImmutableMap.of(COMPRESSION_CODEC, "zstd", COMPRESSION_LEVEL, "3"),
                ImmutableMap.of(
                    DEFAULT_FILE_FORMAT,
                    "parquet",
                    DELETE_DEFAULT_FILE_FORMAT,
                    "parquet",
                    TableProperties.PARQUET_COMPRESSION,
                    "gzip",
                    TableProperties.DELETE_PARQUET_COMPRESSION,
                    "snappy"),
                ImmutableMap.of(
                    DELETE_PARQUET_COMPRESSION,
                    "zstd",
                    PARQUET_COMPRESSION,
                    "zstd",
                    PARQUET_COMPRESSION_LEVEL,
                    "3",
                    DELETE_PARQUET_COMPRESSION_LEVEL,
                    "3")),
            Lists.newArrayList(
                ImmutableMap.of(
                    COMPRESSION_CODEC,
                    "zstd",
                    SparkSQLProperties.COMPRESSION_STRATEGY,
                    "compression"),
                ImmutableMap.of(
                    DEFAULT_FILE_FORMAT,
                    "orc",
                    DELETE_DEFAULT_FILE_FORMAT,
                    "orc",
                    ORC_COMPRESSION,
                    "zlib",
                    DELETE_ORC_COMPRESSION,
                    "snappy"),
                ImmutableMap.of(
                    DELETE_ORC_COMPRESSION,
                    "zstd",
                    ORC_COMPRESSION,
                    "zstd",
                    DELETE_ORC_COMPRESSION_STRATEGY,
                    "compression",
                    ORC_COMPRESSION_STRATEGY,
                    "compression")),
            Lists.newArrayList(
                ImmutableMap.of(COMPRESSION_CODEC, "zstd", COMPRESSION_LEVEL, "9"),
                ImmutableMap.of(
                    DEFAULT_FILE_FORMAT,
                    "avro",
                    DELETE_DEFAULT_FILE_FORMAT,
                    "avro",
                    AVRO_COMPRESSION,
                    "gzip",
                    DELETE_AVRO_COMPRESSION,
                    "snappy"),
                ImmutableMap.of(
                    DELETE_AVRO_COMPRESSION,
                    "zstd",
                    AVRO_COMPRESSION,
                    "zstd",
                    AVRO_COMPRESSION_LEVEL,
                    "9",
                    DELETE_AVRO_COMPRESSION_LEVEL,
                    "9")));
    for (List<Map<String, String>> propertiesSuite : propertiesSuites) {
      testWriteProperties(propertiesSuite);
    }
  }

  @Test
  public void testDataPropsDefaultsAsDeleteProps() {
    List<List<Map<String, String>>> propertiesSuites =
        Lists.newArrayList(
            Lists.newArrayList(
                ImmutableMap.of(),
                ImmutableMap.of(
                    DEFAULT_FILE_FORMAT,
                    "parquet",
                    DELETE_DEFAULT_FILE_FORMAT,
                    "parquet",
                    PARQUET_COMPRESSION,
                    "zstd",
                    PARQUET_COMPRESSION_LEVEL,
                    "5"),
                ImmutableMap.of(
                    DELETE_PARQUET_COMPRESSION,
                    "zstd",
                    PARQUET_COMPRESSION,
                    "zstd",
                    PARQUET_COMPRESSION_LEVEL,
                    "5",
                    DELETE_PARQUET_COMPRESSION_LEVEL,
                    "5")),
            Lists.newArrayList(
                ImmutableMap.of(),
                ImmutableMap.of(
                    DEFAULT_FILE_FORMAT,
                    "orc",
                    DELETE_DEFAULT_FILE_FORMAT,
                    "orc",
                    ORC_COMPRESSION,
                    "snappy",
                    ORC_COMPRESSION_STRATEGY,
                    "speed"),
                ImmutableMap.of(
                    DELETE_ORC_COMPRESSION,
                    "snappy",
                    ORC_COMPRESSION,
                    "snappy",
                    ORC_COMPRESSION_STRATEGY,
                    "speed",
                    DELETE_ORC_COMPRESSION_STRATEGY,
                    "speed")),
            Lists.newArrayList(
                ImmutableMap.of(),
                ImmutableMap.of(
                    DEFAULT_FILE_FORMAT,
                    "avro",
                    DELETE_DEFAULT_FILE_FORMAT,
                    "avro",
                    AVRO_COMPRESSION,
                    "snappy",
                    AVRO_COMPRESSION_LEVEL,
                    "9"),
                ImmutableMap.of(
                    DELETE_AVRO_COMPRESSION,
                    "snappy",
                    AVRO_COMPRESSION,
                    "snappy",
                    AVRO_COMPRESSION_LEVEL,
                    "9",
                    DELETE_AVRO_COMPRESSION_LEVEL,
                    "9")));
    for (List<Map<String, String>> propertiesSuite : propertiesSuites) {
      testWriteProperties(propertiesSuite);
    }
  }

  @Test
  public void testDeleteFileWriteConf() {
    List<List<Map<String, String>>> propertiesSuites =
        Lists.newArrayList(
            Lists.newArrayList(
                ImmutableMap.of(),
                ImmutableMap.of(
                    DEFAULT_FILE_FORMAT,
                    "parquet",
                    DELETE_DEFAULT_FILE_FORMAT,
                    "parquet",
                    TableProperties.PARQUET_COMPRESSION,
                    "zstd",
                    PARQUET_COMPRESSION_LEVEL,
                    "5",
                    DELETE_PARQUET_COMPRESSION_LEVEL,
                    "6"),
                ImmutableMap.of(
                    DELETE_PARQUET_COMPRESSION,
                    "zstd",
                    PARQUET_COMPRESSION,
                    "zstd",
                    PARQUET_COMPRESSION_LEVEL,
                    "5",
                    DELETE_PARQUET_COMPRESSION_LEVEL,
                    "6")),
            Lists.newArrayList(
                ImmutableMap.of(),
                ImmutableMap.of(
                    DEFAULT_FILE_FORMAT,
                    "orc",
                    DELETE_DEFAULT_FILE_FORMAT,
                    "orc",
                    ORC_COMPRESSION,
                    "snappy",
                    ORC_COMPRESSION_STRATEGY,
                    "speed",
                    DELETE_ORC_COMPRESSION,
                    "zstd",
                    DELETE_ORC_COMPRESSION_STRATEGY,
                    "compression"),
                ImmutableMap.of(
                    DELETE_ORC_COMPRESSION,
                    "zstd",
                    ORC_COMPRESSION,
                    "snappy",
                    ORC_COMPRESSION_STRATEGY,
                    "speed",
                    DELETE_ORC_COMPRESSION_STRATEGY,
                    "compression")),
            Lists.newArrayList(
                ImmutableMap.of(),
                ImmutableMap.of(
                    DEFAULT_FILE_FORMAT,
                    "avro",
                    DELETE_DEFAULT_FILE_FORMAT,
                    "avro",
                    AVRO_COMPRESSION,
                    "snappy",
                    AVRO_COMPRESSION_LEVEL,
                    "9",
                    DELETE_AVRO_COMPRESSION,
                    "zstd",
                    DELETE_AVRO_COMPRESSION_LEVEL,
                    "16"),
                ImmutableMap.of(
                    DELETE_AVRO_COMPRESSION,
                    "zstd",
                    AVRO_COMPRESSION,
                    "snappy",
                    AVRO_COMPRESSION_LEVEL,
                    "9",
                    DELETE_AVRO_COMPRESSION_LEVEL,
                    "16")));
    for (List<Map<String, String>> propertiesSuite : propertiesSuites) {
      testWriteProperties(propertiesSuite);
    }
  }

  private void testWriteProperties(List<Map<String, String>> propertiesSuite) {
    withSQLConf(
        propertiesSuite.get(0),
        () -> {
          Table table = validationCatalog.loadTable(tableIdent);
          Map<String, String> tableProperties = propertiesSuite.get(1);
          UpdateProperties updateProperties = table.updateProperties();
          for (Map.Entry<String, String> entry : tableProperties.entrySet()) {
            updateProperties.set(entry.getKey(), entry.getValue());
          }

          updateProperties.commit();

          SparkWriteConf writeConf = new SparkWriteConf(spark, table, ImmutableMap.of());
          Map<String, String> writeProperties = writeConf.writeProperties();
          Map<String, String> expectedProperties = propertiesSuite.get(2);
          Assert.assertEquals(expectedProperties.size(), writeConf.writeProperties().size());
          for (Map.Entry<String, String> entry : writeProperties.entrySet()) {
            Assert.assertEquals(entry.getValue(), expectedProperties.get(entry.getKey()));
          }

          table.refresh();
          updateProperties = table.updateProperties();
          for (Map.Entry<String, String> entry : tableProperties.entrySet()) {
            updateProperties.remove(entry.getKey());
          }

          updateProperties.commit();
        });
  }

  private void checkMode(DistributionMode expectedMode, SparkWriteConf writeConf) {
    Assert.assertEquals(expectedMode, writeConf.distributionMode());
    Assert.assertEquals(expectedMode, writeConf.copyOnWriteDistributionMode(DELETE));
    Assert.assertEquals(expectedMode, writeConf.positionDeltaDistributionMode(DELETE));
    Assert.assertEquals(expectedMode, writeConf.copyOnWriteDistributionMode(UPDATE));
    Assert.assertEquals(expectedMode, writeConf.positionDeltaDistributionMode(UPDATE));
    Assert.assertEquals(expectedMode, writeConf.copyOnWriteDistributionMode(MERGE));
    Assert.assertEquals(expectedMode, writeConf.positionDeltaDistributionMode(MERGE));
  }
}
