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
import org.apache.iceberg.spark.SparkWriteConf;
import org.apache.iceberg.types.TypeUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;

class SparkRewriteWriteBuilder implements WriteBuilder {

  private final SparkSession spark;
  private final Table table;
  private final Schema schema;
  private final String groupId;
  private final SparkWriteConf writeConf;
  private final LogicalWriteInfo info;
  private final boolean caseSensitive;
  private final boolean checkNullability;
  private final boolean checkOrdering;

  SparkRewriteWriteBuilder(
      SparkSession spark, Table table, Schema schema, String groupId, LogicalWriteInfo info) {
    this.spark = spark;
    this.table = table;
    this.schema = schema;
    this.groupId = groupId;
    this.writeConf = new SparkWriteConf(spark, table, info.options());
    this.info = info;
    this.caseSensitive = writeConf.caseSensitive();
    this.checkNullability = writeConf.checkNullability();
    this.checkOrdering = writeConf.checkOrdering();
  }

  @Override
  public Write build() {
    Schema writeSchema = validateWriteSchema();
    SparkUtil.validatePartitionTransforms(table.spec());
    String appId = spark.sparkContext().applicationId();
    return new SparkWrite(
        spark,
        table,
        null /* main branch */,
        writeConf,
        info,
        appId,
        writeSchema,
        info.schema(),
        writeConf.writeRequirements()) {

      @Override
      public BatchWrite toBatch() {
        return asRewrite(groupId);
      }

      @Override
      public StreamingWrite toStreaming() {
        throw new UnsupportedOperationException("Streaming writes are not supported for rewrites");
      }
    };
  }

  private Schema validateWriteSchema() {
    Schema writeSchema = SparkSchemaUtil.convert(schema, info.schema(), caseSensitive);
    TypeUtil.validateWriteSchema(schema, writeSchema, checkNullability, checkOrdering);
    return writeSchema;
  }
}
