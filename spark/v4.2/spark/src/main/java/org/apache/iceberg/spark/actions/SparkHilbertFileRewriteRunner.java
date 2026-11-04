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

import static org.apache.spark.sql.functions.array;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ZOrderByteUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SparkHilbertFileRewriteRunner extends SparkShufflingFileRewriteRunner {
  private static final Logger LOG = LoggerFactory.getLogger(SparkHilbertFileRewriteRunner.class);

  private static final String H_COLUMN = "ICEHVALUE";
  private static final Schema H_SCHEMA =
      new Schema(Types.NestedField.required(0, H_COLUMN, Types.BinaryType.get()));
  private static final SortOrder H_SORT_ORDER =
      SortOrder.builderFor(H_SCHEMA)
          .sortBy(H_COLUMN, SortDirection.ASC, NullOrder.NULLS_LAST)
          .build();

  /**
   * The number of bits each column contributes to the Hilbert index by default.
   *
   * <p>This is the full width of {@link ZOrderByteUtils#PRIMITIVE_BUFFER_SIZE}. Truncating the
   * shared {@link ZOrderByteUtils} encodings is unsafe: whole-number types (int, date, small longs,
   * ...) are widened into an 8-byte key with their magnitude in the low-order bytes, so fewer
   * high-order bytes would collapse those columns to a single coordinate. Narrower keys come from
   * the quantized path instead, which scales each column to its range in the rewrite group.
   */
  private static final int BITS_PER_COLUMN = ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE * Byte.SIZE;

  private final List<String> hilbertColNames;
  private int quantizationBits;

  SparkHilbertFileRewriteRunner(SparkSession spark, Table table, List<String> hilbertColNames) {
    super(spark, table);
    this.hilbertColNames = validHilbertColNames(spark, table, hilbertColNames);
  }

  @Override
  public String description() {
    return "HILBERT";
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.<String>builder()
        .addAll(super.validOptions())
        .add(CurveQuantization.QUANTIZATION_BITS)
        .build();
  }

  @Override
  public void init(Map<String, String> options) {
    super.init(options);
    this.quantizationBits = CurveQuantization.quantizationBits(options);
    if (quantizationBits > 0) {
      CurveQuantization.validateColumns(table(), hilbertColNames);
    }
  }

  @Override
  List<String> statsColumns(Map<String, String> options) {
    return CurveQuantization.quantizationBits(options) > 0
        ? ImmutableList.copyOf(hilbertColNames)
        : ImmutableList.of();
  }

  @Override
  protected SortOrder sortOrder() {
    return H_SORT_ORDER;
  }

  /**
   * Overrides the sortSchema method to include columns from H_SCHEMA.
   *
   * <p>This method generates a new Schema object which consists of columns from the original table
   * schema and H_SCHEMA.
   */
  @Override
  protected Schema sortSchema() {
    return new Schema(
        new ImmutableList.Builder<Types.NestedField>()
            .addAll(table().schema().columns())
            .addAll(H_SCHEMA.columns())
            .build());
  }

  @Override
  protected Dataset<Row> sortedDF(Dataset<Row> df, Function<Dataset<Row>, Dataset<Row>> sortFunc) {
    return sortedByHilbertValue(df, sortFunc, hilbertValue(df));
  }

  @Override
  protected Dataset<Row> sortedDF(
      Dataset<Row> df,
      Function<Dataset<Row>, Dataset<Row>> sortFunc,
      List<FileScanTask> groupTasks) {
    if (quantizationBits == 0) {
      return sortedDF(df, sortFunc);
    }

    List<CurveQuantization.ColumnRange> ranges =
        CurveQuantization.ranges(groupTasks, table(), hilbertColNames);
    return sortedByHilbertValue(df, sortFunc, quantizedHilbertValue(df, ranges));
  }

  private Dataset<Row> sortedByHilbertValue(
      Dataset<Row> df, Function<Dataset<Row>, Dataset<Row>> sortFunc, Column hilbertValue) {
    Dataset<Row> hValueDF = df.withColumn(H_COLUMN, hilbertValue);
    Dataset<Row> sortedDF = sortFunc.apply(hValueDF);
    return sortedDF.drop(H_COLUMN);
  }

  private Column hilbertValue(Dataset<Row> df) {
    // Reuse the Z-order byte conversions. Every column contributes the full primitive width so the
    // Hilbert transform sees a uniform per-dimension width with no magnitude loss.
    SparkZOrderUDF byteUDF =
        new SparkZOrderUDF(
            hilbertColNames.size(), ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE, Integer.MAX_VALUE);

    Column[] orderedCols =
        hilbertColNames.stream()
            .map(df.schema()::apply)
            .map(col -> byteUDF.sortedLexicographically(df.col(col.name()), col.dataType()))
            .toArray(Column[]::new);

    SparkHilbertUDF hilbertUDF = new SparkHilbertUDF(hilbertColNames.size(), BITS_PER_COLUMN);
    return hilbertUDF.hilbertValue(array(orderedCols));
  }

  private Column quantizedHilbertValue(
      Dataset<Row> df, List<CurveQuantization.ColumnRange> ranges) {
    SparkZOrderUDF byteUDF =
        new SparkZOrderUDF(
            hilbertColNames.size(), ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE, Integer.MAX_VALUE);

    Column[] orderedCols = new Column[hilbertColNames.size()];
    for (int i = 0; i < orderedCols.length; i++) {
      StructField field = df.schema().apply(hilbertColNames.get(i));
      orderedCols[i] =
          byteUDF.quantizedOrderedBytes(
              df.col(field.name()), field.dataType(), ranges.get(i), quantizationBits);
    }

    SparkHilbertUDF hilbertUDF = new SparkHilbertUDF(hilbertColNames.size(), quantizationBits);
    return hilbertUDF.hilbertValue(array(orderedCols));
  }

  private List<String> validHilbertColNames(
      SparkSession spark, Table table, List<String> inputHilbertColNames) {

    Preconditions.checkArgument(
        inputHilbertColNames != null && !inputHilbertColNames.isEmpty(),
        "Cannot HILBERT when no columns are specified");

    Schema schema = table.schema();
    Set<Integer> identityPartitionFieldIds = table.spec().identitySourceIds();
    boolean caseSensitive = SparkUtil.caseSensitive(spark);

    Preconditions.checkArgument(
        caseSensitive
            ? schema.findField(H_COLUMN) == null
            : schema.caseInsensitiveFindField(H_COLUMN) == null,
        "Cannot HILBERT because the table has a column named '%s', which conflicts with Iceberg's internal Hilbert column name",
        H_COLUMN);

    List<String> validHilbertColNames = Lists.newArrayList();

    for (String colName : inputHilbertColNames) {
      Types.NestedField field =
          caseSensitive ? schema.findField(colName) : schema.caseInsensitiveFindField(colName);
      Preconditions.checkArgument(
          field != null,
          "Cannot find column '%s' in table schema (case sensitive = %s): %s",
          colName,
          caseSensitive,
          schema.asStruct());

      if (identityPartitionFieldIds.contains(field.fieldId())) {
        LOG.warn("Ignoring '{}' as such values are constant within a partition", colName);
      } else {
        validHilbertColNames.add(schema.findColumnName(field.fieldId()));
      }
    }

    Preconditions.checkArgument(
        !validHilbertColNames.isEmpty(),
        "Cannot HILBERT, all columns provided were identity partition columns and cannot be used");

    return validHilbertColNames;
  }
}
