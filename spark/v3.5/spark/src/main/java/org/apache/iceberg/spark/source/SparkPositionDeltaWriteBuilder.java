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

import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.spark.SparkWriteConf;
import org.apache.iceberg.types.TypeUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.write.DeltaWrite;
import org.apache.spark.sql.connector.write.DeltaWriteBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperation.Command;
import org.apache.spark.sql.types.StructType;

class SparkPositionDeltaWriteBuilder implements DeltaWriteBuilder {

  private static final Schema EXPECTED_ROW_ID_SCHEMA =
      new Schema(MetadataColumns.FILE_PATH, MetadataColumns.ROW_POSITION);

  private final SparkSession spark;
  private final Table table;
  private final Command command;
  private final SparkBatchQueryScan scan;
  private final IsolationLevel isolationLevel;
  private final SparkWriteConf writeConf;
  private final LogicalWriteInfo info;
  private final boolean checkNullability;
  private final boolean checkOrdering;

  SparkPositionDeltaWriteBuilder(
      SparkSession spark,
      Table table,
      String branch,
      Command command,
      Scan scan,
      IsolationLevel isolationLevel,
      LogicalWriteInfo info) {
    this.spark = spark;
    this.table = table;
    this.command = command;
    this.scan = (SparkBatchQueryScan) scan;
    this.isolationLevel = isolationLevel;
    this.writeConf = new SparkWriteConf(spark, table, branch, info.options());
    this.info = info;
    this.checkNullability = writeConf.checkNullability();
    this.checkOrdering = writeConf.checkOrdering();
  }

  @Override
  public DeltaWrite build() {
    Schema dataSchema = dataSchema();

    validateRowIdSchema();
    validateMetadataSchema();
    SparkUtil.validatePartitionTransforms(table.spec());

    return new SparkPositionDeltaWrite(
        spark, table, command, scan, isolationLevel, writeConf, info, dataSchema);
  }

  private Schema dataSchema() {
    if (info.schema() == null || info.schema().isEmpty()) {
      return null;
    } else {
      Schema dataSchema = SparkSchemaUtil.convert(table.schema(), info.schema());
      validateSchema("data", table.schema(), dataSchema);
      return dataSchema;
    }
  }

  private void validateRowIdSchema() {
    Preconditions.checkArgument(info.rowIdSchema().isPresent(), "Row ID schema must be set");
    StructType rowIdSparkType = info.rowIdSchema().get();
    Schema rowIdSchema = SparkSchemaUtil.convert(EXPECTED_ROW_ID_SCHEMA, rowIdSparkType);
    validateSchema("row ID", EXPECTED_ROW_ID_SCHEMA, rowIdSchema);
  }

  private void validateMetadataSchema() {
    Preconditions.checkArgument(info.metadataSchema().isPresent(), "Metadata schema must be set");
    Schema expectedMetadataSchema =
        new Schema(
            MetadataColumns.SPEC_ID,
            MetadataColumns.metadataColumn(table, MetadataColumns.PARTITION_COLUMN_NAME));
    StructType metadataSparkType = info.metadataSchema().get();
    Schema metadataSchema = SparkSchemaUtil.convert(expectedMetadataSchema, metadataSparkType);
    validateSchema("metadata", expectedMetadataSchema, metadataSchema);
  }

  private void validateSchema(String context, Schema expected, Schema actual) {
    TypeUtil.validateSchema(context, expected, actual, checkNullability, checkOrdering);
  }
}
