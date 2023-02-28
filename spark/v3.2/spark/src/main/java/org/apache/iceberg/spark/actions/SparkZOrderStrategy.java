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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteStrategy;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.spark.SparkDistributionAndOrderingUtil;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SortOrderUtil;
import org.apache.iceberg.util.ZOrderByteUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkZOrderStrategy extends SparkSortStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(SparkZOrderStrategy.class);

  private static final String Z_COLUMN = "ICEZVALUE";
  private static final Schema Z_SCHEMA =
      new Schema(NestedField.required(0, Z_COLUMN, Types.BinaryType.get()));
  private static final org.apache.iceberg.SortOrder Z_SORT_ORDER =
      org.apache.iceberg.SortOrder.builderFor(Z_SCHEMA)
          .sortBy(Z_COLUMN, SortDirection.ASC, NullOrder.NULLS_LAST)
          .build();

  /**
   * Controls the amount of bytes interleaved in the ZOrder Algorithm. Default is all bytes being
   * interleaved.
   */
  private static final String MAX_OUTPUT_SIZE_KEY = "max-output-size";

  private static final int DEFAULT_MAX_OUTPUT_SIZE = Integer.MAX_VALUE;

  /**
   * Controls the number of bytes considered from an input column of a type with variable length
   * (String, Binary). Default is to use the same size as primitives {@link
   * ZOrderByteUtils#PRIMITIVE_BUFFER_SIZE}
   */
  private static final String VAR_LENGTH_CONTRIBUTION_KEY = "var-length-contribution";

  private static final int DEFAULT_VAR_LENGTH_CONTRIBUTION = ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE;

  private final List<String> zOrderColNames;

  private int maxOutputSize;
  private int varLengthContribution;

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.<String>builder()
        .addAll(super.validOptions())
        .add(VAR_LENGTH_CONTRIBUTION_KEY)
        .add(MAX_OUTPUT_SIZE_KEY)
        .build();
  }

  @Override
  public RewriteStrategy options(Map<String, String> options) {
    super.options(options);

    varLengthContribution =
        PropertyUtil.propertyAsInt(
            options, VAR_LENGTH_CONTRIBUTION_KEY, DEFAULT_VAR_LENGTH_CONTRIBUTION);
    Preconditions.checkArgument(
        varLengthContribution > 0,
        "Cannot use less than 1 byte for variable length types with zOrder, %s was set to %s",
        VAR_LENGTH_CONTRIBUTION_KEY,
        varLengthContribution);

    maxOutputSize =
        PropertyUtil.propertyAsInt(options, MAX_OUTPUT_SIZE_KEY, DEFAULT_MAX_OUTPUT_SIZE);
    Preconditions.checkArgument(
        maxOutputSize > 0,
        "Cannot have the interleaved ZOrder value use less than 1 byte, %s was set to %s",
        MAX_OUTPUT_SIZE_KEY,
        maxOutputSize);

    return this;
  }

  public SparkZOrderStrategy(Table table, SparkSession spark, List<String> zOrderColNames) {
    super(table, spark);

    Preconditions.checkArgument(
        zOrderColNames != null && !zOrderColNames.isEmpty(),
        "Cannot ZOrder when no columns are specified");

    Stream<String> identityPartitionColumns =
        table.spec().fields().stream()
            .filter(f -> f.transform().isIdentity())
            .map(PartitionField::name);
    List<String> partZOrderCols =
        identityPartitionColumns.filter(zOrderColNames::contains).collect(Collectors.toList());

    if (!partZOrderCols.isEmpty()) {
      LOG.warn(
          "Cannot ZOrder on an Identity partition column as these values are constant within a partition "
              + "and will be removed from the ZOrder expression: {}",
          partZOrderCols);
      zOrderColNames.removeAll(partZOrderCols);
      Preconditions.checkArgument(
          !zOrderColNames.isEmpty(),
          "Cannot perform ZOrdering, all columns provided were identity partition columns and cannot be used.");
    }

    validateColumnsExistence(table, spark, zOrderColNames);

    this.zOrderColNames = zOrderColNames;
  }

  private void validateColumnsExistence(Table table, SparkSession spark, List<String> colNames) {
    boolean caseSensitive = SparkUtil.caseSensitive(spark);
    Schema schema = table.schema();
    colNames.forEach(
        col -> {
          NestedField nestedField =
              caseSensitive ? schema.findField(col) : schema.caseInsensitiveFindField(col);
          if (nestedField == null) {
            throw new IllegalArgumentException(
                String.format(
                    "Cannot find column '%s' in table schema: %s", col, schema.asStruct()));
          }
        });
  }

  @Override
  public String name() {
    return "Z-ORDER";
  }

  @Override
  protected void validateOptions() {
    // Ignore SortStrategy validation
    return;
  }

  @Override
  public Set<DataFile> rewriteFiles(List<FileScanTask> filesToRewrite) {
    SparkZOrderUDF zOrderUDF =
        new SparkZOrderUDF(zOrderColNames.size(), varLengthContribution, maxOutputSize);

    String groupID = UUID.randomUUID().toString();
    boolean requiresRepartition = !filesToRewrite.get(0).spec().equals(table().spec());

    SortOrder[] ordering;
    if (requiresRepartition) {
      ordering =
          SparkDistributionAndOrderingUtil.convert(
              SortOrderUtil.buildSortOrder(table(), sortOrder()));
    } else {
      ordering = SparkDistributionAndOrderingUtil.convert(sortOrder());
    }

    Distribution distribution = Distributions.ordered(ordering);

    try {
      tableCache().add(groupID, table());
      manager().stageTasks(table(), groupID, filesToRewrite);

      // spark session from parent
      SparkSession spark = spark();

      // Reset Shuffle Partitions for our sort
      long numOutputFiles =
          numOutputFiles((long) (inputFileSize(filesToRewrite) * sizeEstimateMultiple()));
      spark.conf().set(SQLConf.SHUFFLE_PARTITIONS().key(), Math.max(1, numOutputFiles));

      Dataset<Row> scanDF =
          spark
              .read()
              .format("iceberg")
              .option(SparkReadOptions.FILE_SCAN_TASK_SET_ID, groupID)
              .load(groupID);

      Column[] originalColumns =
          Arrays.stream(scanDF.schema().names()).map(n -> functions.col(n)).toArray(Column[]::new);

      List<StructField> zOrderColumns =
          zOrderColNames.stream().map(scanDF.schema()::apply).collect(Collectors.toList());

      Column zvalueArray =
          functions.array(
              zOrderColumns.stream()
                  .map(
                      colStruct ->
                          zOrderUDF.sortedLexicographically(
                              functions.col(colStruct.name()), colStruct.dataType()))
                  .toArray(Column[]::new));

      Dataset<Row> zvalueDF = scanDF.withColumn(Z_COLUMN, zOrderUDF.interleaveBytes(zvalueArray));

      SQLConf sqlConf = spark.sessionState().conf();
      LogicalPlan sortPlan = sortPlan(distribution, ordering, zvalueDF.logicalPlan(), sqlConf);
      Dataset<Row> sortedDf = new Dataset<>(spark, sortPlan, zvalueDF.encoder());
      sortedDf
          .select(originalColumns)
          .write()
          .format("iceberg")
          .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, groupID)
          .option(SparkWriteOptions.TARGET_FILE_SIZE_BYTES, writeMaxFileSize())
          .option(SparkWriteOptions.USE_TABLE_DISTRIBUTION_AND_ORDERING, "false")
          .mode("append")
          .save(groupID);

      return rewriteCoordinator().fetchNewDataFiles(table(), groupID);
    } finally {
      tableCache().remove(groupID);
      manager().removeTasks(table(), groupID);
      rewriteCoordinator().clearRewrite(table(), groupID);
    }
  }

  @Override
  protected org.apache.iceberg.SortOrder sortOrder() {
    return Z_SORT_ORDER;
  }
}
