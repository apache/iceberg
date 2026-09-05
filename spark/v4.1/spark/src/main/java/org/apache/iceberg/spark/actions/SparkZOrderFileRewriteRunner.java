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
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.ZOrderByteUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

class SparkZOrderFileRewriteRunner extends SparkCurveFileRewriteRunner {

  private static final String Z_COLUMN = "ICEZVALUE";

  /**
   * Controls the amount of bytes interleaved in the ZOrder algorithm. Default is all bytes being
   * interleaved.
   */
  public static final String MAX_OUTPUT_SIZE = "max-output-size";

  public static final int MAX_OUTPUT_SIZE_DEFAULT = Integer.MAX_VALUE;

  /**
   * Controls the number of bytes considered from an input column of a type with variable length
   * (String, Binary).
   *
   * <p>Default is to use the same size as primitives {@link ZOrderByteUtils#PRIMITIVE_BUFFER_SIZE}.
   */
  public static final String VAR_LENGTH_CONTRIBUTION = "var-length-contribution";

  public static final int VAR_LENGTH_CONTRIBUTION_DEFAULT = ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE;

  private int maxOutputSize;
  private int varLengthContribution;

  SparkZOrderFileRewriteRunner(SparkSession spark, Table table, List<String> zOrderColNames) {
    super(
        spark,
        table,
        zOrderColNames,
        Z_COLUMN,
        "Cannot ZOrder when no columns are specified",
        "Cannot zorder because the table has a column named '%s', which conflicts with Iceberg's internal Z-order column name",
        "Cannot ZOrder, all columns provided were identity partition columns and cannot be used");
  }

  @Override
  public String description() {
    return "Z-ORDER";
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.<String>builder()
        .addAll(super.validOptions())
        .add(MAX_OUTPUT_SIZE)
        .add(VAR_LENGTH_CONTRIBUTION)
        .build();
  }

  @Override
  public void init(Map<String, String> options) {
    super.init(options);
    this.maxOutputSize = maxOutputSize(options);
    this.varLengthContribution = varLengthContribution(options);
  }

  @Override
  protected Column curveValue(Dataset<Row> df) {
    SparkZOrderUDF zOrderUDF =
        new SparkZOrderUDF(curveColNames().size(), varLengthContribution, maxOutputSize);
    return zOrderUDF.interleaveBytes(array(orderedColumns(df, zOrderUDF)));
  }

  private int varLengthContribution(Map<String, String> options) {
    int value =
        PropertyUtil.propertyAsInt(
            options, VAR_LENGTH_CONTRIBUTION, VAR_LENGTH_CONTRIBUTION_DEFAULT);
    Preconditions.checkArgument(
        value > 0,
        "Cannot use less than 1 byte for variable length types with ZOrder, '%s' was set to %s",
        VAR_LENGTH_CONTRIBUTION,
        value);
    return value;
  }

  private int maxOutputSize(Map<String, String> options) {
    int value = PropertyUtil.propertyAsInt(options, MAX_OUTPUT_SIZE, MAX_OUTPUT_SIZE_DEFAULT);
    Preconditions.checkArgument(
        value > 0,
        "Cannot have the interleaved ZOrder value use less than 1 byte, '%s' was set to %s",
        MAX_OUTPUT_SIZE,
        value);
    return value;
  }
}
