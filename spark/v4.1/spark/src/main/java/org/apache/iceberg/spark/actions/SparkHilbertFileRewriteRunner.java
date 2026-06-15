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
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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
   * Controls the number of bits taken from each input column when computing the Hilbert index. Must
   * be a positive multiple of 8, no greater than 64. Default is 64.
   */
  public static final String BITS_PER_COLUMN = "bits-per-column";

  public static final int BITS_PER_COLUMN_DEFAULT = 64;

  private final List<String> hilbertColNames;
  private int bitsPerColumn;

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
    return ImmutableSet.<String>builder().addAll(super.validOptions()).add(BITS_PER_COLUMN).build();
  }

  @Override
  public void init(Map<String, String> options) {
    super.init(options);
    this.bitsPerColumn = bitsPerColumn(options);
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
    Dataset<Row> hValueDF = df.withColumn(H_COLUMN, hilbertValue(df));
    Dataset<Row> sortedDF = sortFunc.apply(hValueDF);
    return sortedDF.drop(H_COLUMN);
  }

  private Column hilbertValue(Dataset<Row> df) {
    int bytesPerColumn = bitsPerColumn / 8;
    // Reuse the Z-order byte conversions; force every column to contribute exactly bytesPerColumn
    // bytes so the Hilbert transform sees a uniform per-dimension width.
    SparkZOrderUDF byteUDF =
        new SparkZOrderUDF(hilbertColNames.size(), bytesPerColumn, Integer.MAX_VALUE);

    Column[] orderedCols =
        hilbertColNames.stream()
            .map(df.schema()::apply)
            .map(col -> byteUDF.sortedLexicographically(df.col(col.name()), col.dataType()))
            .toArray(Column[]::new);

    SparkHilbertUDF hilbertUDF = new SparkHilbertUDF(hilbertColNames.size(), bitsPerColumn);
    return hilbertUDF.hilbertValue(array(orderedCols));
  }

  private int bitsPerColumn(Map<String, String> options) {
    int value = PropertyUtil.propertyAsInt(options, BITS_PER_COLUMN, BITS_PER_COLUMN_DEFAULT);
    Preconditions.checkArgument(
        value > 0 && value % 8 == 0 && value <= 64,
        "Invalid '%s': must be a positive multiple of 8 no greater than 64, was %s",
        BITS_PER_COLUMN,
        value);
    return value;
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
        validHilbertColNames.add(colName);
      }
    }

    Preconditions.checkArgument(
        !validHilbertColNames.isEmpty(),
        "Cannot HILBERT, all columns provided were identity partition columns and cannot be used");

    return validHilbertColNames;
  }
}
