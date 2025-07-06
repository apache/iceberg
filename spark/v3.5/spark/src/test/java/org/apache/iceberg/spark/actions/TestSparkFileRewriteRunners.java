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
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class TestSparkFileRewriteRunners extends TestBase {

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
  public void testInvalidConstructorUsagesSortData() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);

    assertThatThrownBy(() -> new SparkSortFileRewriteRunner(spark, table))
        .hasMessageContaining("Cannot sort data without a valid sort order")
        .hasMessageContaining("is unsorted and no sort order is provided");

    assertThatThrownBy(() -> new SparkSortFileRewriteRunner(spark, table, null))
        .hasMessageContaining("Cannot sort data without a valid sort order")
        .hasMessageContaining("the provided sort order is null or empty");

    assertThatThrownBy(() -> new SparkSortFileRewriteRunner(spark, table, SortOrder.unsorted()))
        .hasMessageContaining("Cannot sort data without a valid sort order")
        .hasMessageContaining("the provided sort order is null or empty");
  }

  @Test
  public void testInvalidConstructorUsagesZOrderData() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA, SPEC);

    assertThatThrownBy(() -> new SparkZOrderFileRewriteRunner(spark, table, null))
        .hasMessageContaining("Cannot ZOrder when no columns are specified");

    assertThatThrownBy(() -> new SparkZOrderFileRewriteRunner(spark, table, ImmutableList.of()))
        .hasMessageContaining("Cannot ZOrder when no columns are specified");

    assertThatThrownBy(
            () -> new SparkZOrderFileRewriteRunner(spark, table, ImmutableList.of("dep")))
        .hasMessageContaining("Cannot ZOrder")
        .hasMessageContaining("all columns provided were identity partition columns");

    assertThatThrownBy(
            () -> new SparkZOrderFileRewriteRunner(spark, table, ImmutableList.of("DeP")))
        .hasMessageContaining("Cannot ZOrder")
        .hasMessageContaining("all columns provided were identity partition columns");
  }

  @Test
  public void testSortDataValidOptions() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    SparkSortFileRewriteRunner rewriter = new SparkSortFileRewriteRunner(spark, table, SORT_ORDER);

    assertThat(rewriter.validOptions())
        .as("Rewriter must report all supported options")
        .containsExactlyInAnyOrder(SparkShufflingFileRewriteRunner.SHUFFLE_PARTITIONS_PER_FILE);
  }

  @Test
  public void testZOrderDataValidOptions() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    ImmutableList<String> zOrderCols = ImmutableList.of("id");
    SparkZOrderFileRewriteRunner rewriter =
        new SparkZOrderFileRewriteRunner(spark, table, zOrderCols);

    assertThat(rewriter.validOptions())
        .as("Rewriter must report all supported options")
        .containsExactlyInAnyOrder(
            SparkZOrderFileRewriteRunner.SHUFFLE_PARTITIONS_PER_FILE,
            SparkZOrderFileRewriteRunner.MAX_OUTPUT_SIZE,
            SparkZOrderFileRewriteRunner.VAR_LENGTH_CONTRIBUTION);
  }

  @Test
  public void testInvalidValuesForZOrderDataOptions() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    ImmutableList<String> zOrderCols = ImmutableList.of("id");
    SparkZOrderFileRewriteRunner rewriter =
        new SparkZOrderFileRewriteRunner(spark, table, zOrderCols);

    Map<String, String> invalidMaxOutputOptions =
        ImmutableMap.of(SparkZOrderFileRewriteRunner.MAX_OUTPUT_SIZE, "0");
    assertThatThrownBy(() -> rewriter.init(invalidMaxOutputOptions))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot have the interleaved ZOrder value use less than 1 byte")
        .hasMessageContaining("'max-output-size' was set to 0");

    Map<String, String> invalidVarLengthContributionOptions =
        ImmutableMap.of(SparkZOrderFileRewriteRunner.VAR_LENGTH_CONTRIBUTION, "0");
    assertThatThrownBy(() -> rewriter.init(invalidVarLengthContributionOptions))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot use less than 1 byte for variable length types with ZOrder")
        .hasMessageContaining("'var-length-contribution' was set to 0");
  }
}
