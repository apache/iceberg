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
package org.apache.iceberg.spark.actions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class TestSparkShufflingDataRewritePlanner extends TestBase {

  private static final TableIdentifier TABLE_IDENT = TableIdentifier.of("default", "tbl");
  private static final Schema SCHEMA =
      new Schema(
          NestedField.required(1, "id", IntegerType.get()),
          NestedField.required(2, "dep", StringType.get()));
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("dep").build();
  private static final SortOrder SORT_ORDER = SortOrder.builderFor(SCHEMA).asc("id").build();

  @AfterEach
  public void removeTable() {
    catalog.dropTable(TABLE_IDENT);
  }

  @Test
  void testSparkShufflingDataRewritePlannerValidOptions() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    SparkShufflingDataRewritePlanner planner =
        new SparkShufflingDataRewritePlanner(
            table, Expressions.alwaysTrue(), null, false /* caseSensitive */);

    assertThat(planner.validOptions())
        .as("Planner must report all supported options")
        .isEqualTo(
            ImmutableSet.of(
                SparkShufflingDataRewritePlanner.COMPRESSION_FACTOR,
                SparkShufflingDataRewritePlanner.TARGET_FILE_SIZE_BYTES,
                SparkShufflingDataRewritePlanner.MIN_FILE_SIZE_BYTES,
                SparkShufflingDataRewritePlanner.MAX_FILE_SIZE_BYTES,
                SparkShufflingDataRewritePlanner.MIN_INPUT_FILES,
                SparkShufflingDataRewritePlanner.REWRITE_ALL,
                SparkShufflingDataRewritePlanner.MAX_FILE_GROUP_SIZE_BYTES,
                SparkShufflingDataRewritePlanner.DELETE_FILE_THRESHOLD,
                SparkShufflingDataRewritePlanner.DELETE_RATIO_THRESHOLD,
                RewriteDataFiles.REWRITE_JOB_ORDER,
                SparkShufflingDataRewritePlanner.MAX_FILES_TO_REWRITE));
  }

  @Test
  void testInvalidValuesSparkShufflingDataRewritePlannerOptions() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    SparkShufflingDataRewritePlanner planner =
        new SparkShufflingDataRewritePlanner(
            table, Expressions.alwaysTrue(), null, false /* caseSensitive */);

    Map<String, String> invalidCompressionFactorOptions =
        ImmutableMap.of(SparkShufflingDataRewritePlanner.COMPRESSION_FACTOR, "0");
    assertThatThrownBy(() -> planner.init(invalidCompressionFactorOptions))
        .hasMessageContaining("'compression-factor' is set to 0.0 but must be > 0");
  }
}
