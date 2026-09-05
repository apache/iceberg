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

import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base for rewrite runners that sort rows by a space-filling-curve value computed from a list of
 * columns.
 *
 * <p>Subclasses parameterise the internal value column name, the exact user-facing error messages
 * (kept per-curve for compatibility), and the combine step that turns per-column ordered bytes into
 * a single curve value.
 */
abstract class SparkCurveFileRewriteRunner extends SparkShufflingFileRewriteRunner {
  private static final Logger LOG = LoggerFactory.getLogger(SparkCurveFileRewriteRunner.class);

  private final String curveColumnName;
  private final Schema curveSchema;
  private final SortOrder curveSortOrder;
  private final List<String> curveColNames;

  SparkCurveFileRewriteRunner(
      SparkSession spark,
      Table table,
      List<String> colNames,
      String curveColumnName,
      String noColumnsError,
      String columnConflictError,
      String allIdentityColumnsError) {
    super(spark, table);
    this.curveColumnName = curveColumnName;
    this.curveSchema =
        new Schema(Types.NestedField.required(0, curveColumnName, Types.BinaryType.get()));
    this.curveSortOrder =
        SortOrder.builderFor(curveSchema)
            .sortBy(curveColumnName, SortDirection.ASC, NullOrder.NULLS_LAST)
            .build();
    this.curveColNames =
        validCurveColNames(
            spark, table, colNames, noColumnsError, columnConflictError, allIdentityColumnsError);
  }

  @Override
  protected SortOrder sortOrder() {
    return curveSortOrder;
  }

  /**
   * Returns the schema used while sorting: the table's columns plus the internal curve value
   * column.
   */
  @Override
  protected Schema sortSchema() {
    return new Schema(
        new ImmutableList.Builder<Types.NestedField>()
            .addAll(table().schema().columns())
            .addAll(curveSchema.columns())
            .build());
  }

  @Override
  protected Dataset<Row> sortedDF(Dataset<Row> df, Function<Dataset<Row>, Dataset<Row>> sortFunc) {
    Dataset<Row> valueDF = df.withColumn(curveColumnName, curveValue(df));
    Dataset<Row> sortedDF = sortFunc.apply(valueDF);
    return sortedDF.drop(curveColumnName);
  }

  /** Combines the curve input columns of {@code df} into a single binary curve value. */
  protected abstract Column curveValue(Dataset<Row> df);

  protected List<String> curveColNames() {
    return curveColNames;
  }

  /** Converts the curve input columns to their ordered-bytes representation. */
  protected Column[] orderedColumns(Dataset<Row> df, SparkZOrderUDF byteUDF) {
    return curveColNames.stream()
        .map(df.schema()::apply)
        .map(col -> byteUDF.sortedLexicographically(df.col(col.name()), col.dataType()))
        .toArray(Column[]::new);
  }

  private List<String> validCurveColNames(
      SparkSession spark,
      Table table,
      List<String> inputColNames,
      String noColumnsError,
      String columnConflictError,
      String allIdentityColumnsError) {

    Preconditions.checkArgument(inputColNames != null && !inputColNames.isEmpty(), noColumnsError);

    Schema schema = table.schema();
    Set<Integer> identityPartitionFieldIds = table.spec().identitySourceIds();
    boolean caseSensitive = SparkUtil.caseSensitive(spark);

    Preconditions.checkArgument(
        caseSensitive
            ? schema.findField(curveColumnName) == null
            : schema.caseInsensitiveFindField(curveColumnName) == null,
        columnConflictError,
        curveColumnName);

    List<String> validColNames = Lists.newArrayList();

    for (String colName : inputColNames) {
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
        validColNames.add(colName);
      }
    }

    Preconditions.checkArgument(!validColNames.isEmpty(), allIdentityColumnsError);

    return validColNames;
  }
}
