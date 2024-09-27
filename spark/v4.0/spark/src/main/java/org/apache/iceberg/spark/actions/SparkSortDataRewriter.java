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

import java.util.function.Function;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

class SparkSortDataRewriter extends SparkShufflingDataRewriter {

  private final SortOrder sortOrder;

  SparkSortDataRewriter(SparkSession spark, Table table) {
    super(spark, table);
    Preconditions.checkArgument(
        table.sortOrder().isSorted(),
        "Cannot sort data without a valid sort order, table '%s' is unsorted and no sort order is provided",
        table.name());
    this.sortOrder = table.sortOrder();
  }

  SparkSortDataRewriter(SparkSession spark, Table table, SortOrder sortOrder) {
    super(spark, table);
    Preconditions.checkArgument(
        sortOrder != null && sortOrder.isSorted(),
        "Cannot sort data without a valid sort order, the provided sort order is null or empty");
    this.sortOrder = sortOrder;
  }

  @Override
  public String description() {
    return "SORT";
  }

  @Override
  protected SortOrder sortOrder() {
    return sortOrder;
  }

  @Override
  protected Dataset<Row> sortedDF(Dataset<Row> df, Function<Dataset<Row>, Dataset<Row>> sortFunc) {
    return sortFunc.apply(df);
  }
}
