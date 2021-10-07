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

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.types.TypeUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

public class SparkRewriteBuilder implements WriteBuilder {
  private final SparkSession spark;
  private final Table table;
  private final LogicalWriteInfo writeInfo;
  private final String fileSetID;
  private final StructType dsSchema;

  public SparkRewriteBuilder(SparkSession spark, Table table, LogicalWriteInfo info) {
    this.spark = spark;
    this.table = table;
    this.writeInfo = info;
    this.fileSetID = info.options().get(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID);
    this.dsSchema = info.schema();
  }

  @Override
  public BatchWrite buildForBatch() {
    Schema writeSchema = SparkSchemaUtil.convert(table.schema(), dsSchema);
    TypeUtil.validateWriteSchema(table.schema(), writeSchema, true, true);
    SparkUtil.validatePartitionTransforms(table.spec());

    String appId = spark.sparkContext().applicationId();

    SparkWrite write = new SparkWrite(spark, table, writeInfo, appId, null, writeSchema, dsSchema);
    return write.asRewrite(fileSetID);
  }
}
