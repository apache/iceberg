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
import org.apache.iceberg.Table;
import org.apache.iceberg.util.ZOrderByteUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

class SparkHilbertFileRewriteRunner extends SparkCurveFileRewriteRunner {

  private static final String H_COLUMN = "ICEHVALUE";

  /**
   * The number of bits contributed by each column to the Hilbert index.
   *
   * <p>This is fixed at the full width of {@link ZOrderByteUtils#PRIMITIVE_BUFFER_SIZE}. A smaller,
   * configurable width is unsafe with the shared {@link ZOrderByteUtils} encodings: whole-number
   * types (int, date, small longs, ...) are widened into an 8-byte key with their magnitude in the
   * low-order bytes, so truncating to fewer high-order bytes would discard all magnitude and
   * collapse those columns to a single coordinate. Supporting a narrower width correctly would
   * require tracking each column's significant bit width, which is out of scope here.
   */
  private static final int BITS_PER_COLUMN = ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE * Byte.SIZE;

  SparkHilbertFileRewriteRunner(SparkSession spark, Table table, List<String> hilbertColNames) {
    super(
        spark,
        table,
        hilbertColNames,
        H_COLUMN,
        "Cannot HILBERT when no columns are specified",
        "Cannot HILBERT because the table has a column named '%s', which conflicts with Iceberg's internal Hilbert column name",
        "Cannot HILBERT, all columns provided were identity partition columns and cannot be used");
  }

  @Override
  public String description() {
    return "HILBERT";
  }

  @Override
  protected Column curveValue(Dataset<Row> df) {
    // Reuse the Z-order byte conversions. Every column contributes the full primitive width so the
    // Hilbert transform sees a uniform per-dimension width with no magnitude loss.
    SparkZOrderUDF byteUDF =
        new SparkZOrderUDF(
            curveColNames().size(), ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE, Integer.MAX_VALUE);
    SparkHilbertUDF hilbertUDF = new SparkHilbertUDF(curveColNames().size(), BITS_PER_COLUMN);
    return hilbertUDF.hilbertValue(array(orderedColumns(df, byteUDF)));
  }
}
