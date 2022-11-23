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
package org.apache.iceberg.spark.source;

import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.LocalScan;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

class SparkLocalScan implements LocalScan {

  private final Table table;
  private final StructType aggregateSchema;
  private final InternalRow[] rows;

  SparkLocalScan(Table table, StructType aggregateSchema, InternalRow[] rows) {
    this.table = table;
    this.aggregateSchema = aggregateSchema;
    this.rows = rows;
  }

  @Override
  public InternalRow[] rows() {
    return rows;
  }

  @Override
  public StructType readSchema() {
    return aggregateSchema;
  }

  @Override
  public String description() {
    String aggregates =
        Arrays.stream(aggregateSchema.fields())
            .map(StructField::name)
            .collect(Collectors.joining(", "));
    return String.format("%s [aggregates=%s]", table, aggregates);
  }
}
