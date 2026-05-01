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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSparkReadConf extends TestBaseWithCatalog {

  @BeforeEach
  public void before() {
    super.before();
    sql("CREATE TABLE %s (id BIGINT, data STRING) USING iceberg", tableName);
  }

  @AfterEach
  public void after() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testSplitSizePrecedence() {
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(new SparkReadConf(spark, table).splitSize())
        .isEqualTo(TableProperties.SPLIT_SIZE_DEFAULT);

    table.updateProperties().set(TableProperties.SPLIT_SIZE, "16777216").commit();
    assertThat(new SparkReadConf(spark, table).splitSize()).isEqualTo(16777216L);

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.SPLIT_SIZE, "33554432"),
        () -> {
          assertThat(new SparkReadConf(spark, table).splitSize()).isEqualTo(33554432L);

          CaseInsensitiveStringMap readOptions =
              new CaseInsensitiveStringMap(ImmutableMap.of(SparkReadOptions.SPLIT_SIZE, "8388608"));
          assertThat(new SparkReadConf(spark, table, readOptions).splitSize()).isEqualTo(8388608L);
        });
  }

  @TestTemplate
  public void testSplitLookbackPrecedence() {
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(new SparkReadConf(spark, table).splitLookback())
        .isEqualTo(TableProperties.SPLIT_LOOKBACK_DEFAULT);

    table.updateProperties().set(TableProperties.SPLIT_LOOKBACK, "5").commit();
    assertThat(new SparkReadConf(spark, table).splitLookback()).isEqualTo(5);

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.SPLIT_LOOKBACK, "7"),
        () -> {
          assertThat(new SparkReadConf(spark, table).splitLookback()).isEqualTo(7);

          CaseInsensitiveStringMap readOptions =
              new CaseInsensitiveStringMap(ImmutableMap.of(SparkReadOptions.LOOKBACK, "9"));
          assertThat(new SparkReadConf(spark, table, readOptions).splitLookback()).isEqualTo(9);
        });
  }

  @TestTemplate
  public void testSplitOpenFileCostPrecedence() {
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(new SparkReadConf(spark, table).splitOpenFileCost())
        .isEqualTo(TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT);

    table.updateProperties().set(TableProperties.SPLIT_OPEN_FILE_COST, "1048576").commit();
    assertThat(new SparkReadConf(spark, table).splitOpenFileCost()).isEqualTo(1048576L);

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.SPLIT_OPEN_FILE_COST, "2097152"),
        () -> {
          assertThat(new SparkReadConf(spark, table).splitOpenFileCost()).isEqualTo(2097152L);

          CaseInsensitiveStringMap readOptions =
              new CaseInsensitiveStringMap(
                  ImmutableMap.of(SparkReadOptions.FILE_OPEN_COST, "4194304"));
          assertThat(new SparkReadConf(spark, table, readOptions).splitOpenFileCost())
              .isEqualTo(4194304L);
        });
  }

  @TestTemplate
  public void testAdaptiveSplitSizeEnabledPrecedence() {
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(new SparkReadConf(spark, table).adaptiveSplitSizeEnabled())
        .isEqualTo(TableProperties.ADAPTIVE_SPLIT_SIZE_ENABLED_DEFAULT);

    table.updateProperties().set(TableProperties.ADAPTIVE_SPLIT_SIZE_ENABLED, "false").commit();
    assertThat(new SparkReadConf(spark, table).adaptiveSplitSizeEnabled()).isFalse();

    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.ADAPTIVE_SPLIT_SIZE_ENABLED, "true"),
        () -> assertThat(new SparkReadConf(spark, table).adaptiveSplitSizeEnabled()).isTrue());
  }

  @TestTemplate
  public void testSplitOptionAccessorsTreatSessionConfAsExplicitOverride() {
    Table table = validationCatalog.loadTable(tableIdent);
    table
        .updateProperties()
        .set(TableProperties.SPLIT_SIZE, "16777216")
        .set(TableProperties.SPLIT_LOOKBACK, "5")
        .set(TableProperties.SPLIT_OPEN_FILE_COST, "1048576")
        .commit();

    SparkReadConf tableOnlyConf = new SparkReadConf(spark, table);
    assertThat(tableOnlyConf.splitSizeOption()).isNull();
    assertThat(tableOnlyConf.splitLookbackOption()).isNull();
    assertThat(tableOnlyConf.splitOpenFileCostOption()).isNull();

    withSQLConf(
        ImmutableMap.of(
            SparkSQLProperties.SPLIT_SIZE, "33554432",
            SparkSQLProperties.SPLIT_LOOKBACK, "7",
            SparkSQLProperties.SPLIT_OPEN_FILE_COST, "2097152"),
        () -> {
          SparkReadConf sessionConf = new SparkReadConf(spark, table);
          assertThat(sessionConf.splitSizeOption()).isEqualTo(33554432L);
          assertThat(sessionConf.splitLookbackOption()).isEqualTo(7);
          assertThat(sessionConf.splitOpenFileCostOption()).isEqualTo(2097152L);

          CaseInsensitiveStringMap readOptions =
              new CaseInsensitiveStringMap(ImmutableMap.of(SparkReadOptions.SPLIT_SIZE, "8388608"));
          assertThat(new SparkReadConf(spark, table, readOptions).splitSizeOption())
              .isEqualTo(8388608L);
        });
  }
}
