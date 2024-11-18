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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

class SparkStagedScanBuilder implements ScanBuilder, SupportsPushDownRequiredColumns {

  private final SparkSession spark;
  private final Table table;
  private final SparkReadConf readConf;
  private final List<String> metaColumns = Lists.newArrayList();

  private Schema schema;

  SparkStagedScanBuilder(SparkSession spark, Table table, CaseInsensitiveStringMap options) {
    this.spark = spark;
    this.table = table;
    this.readConf = new SparkReadConf(spark, table, options);
    this.schema = table.schema();
  }

  @Override
  public Scan build() {
    return new SparkStagedScan(spark, table, schemaWithMetadataColumns(), readConf);
  }

  @Override
  public void pruneColumns(StructType requestedSchema) {
    StructType requestedProjection = removeMetaColumns(requestedSchema);
    this.schema = SparkSchemaUtil.prune(schema, requestedProjection);

    Stream.of(requestedSchema.fields())
        .map(StructField::name)
        .filter(MetadataColumns::isMetadataColumn)
        .distinct()
        .forEach(metaColumns::add);
  }

  private StructType removeMetaColumns(StructType structType) {
    return new StructType(
        Stream.of(structType.fields())
            .filter(field -> MetadataColumns.nonMetadataColumn(field.name()))
            .toArray(StructField[]::new));
  }

  private Schema schemaWithMetadataColumns() {
    // metadata columns
    List<Types.NestedField> fields =
        metaColumns.stream()
            .distinct()
            .map(name -> MetadataColumns.metadataColumn(table, name))
            .collect(Collectors.toList());
    Schema meta = new Schema(fields);

    // schema of rows returned by readers
    return TypeUtil.join(schema, meta);
  }
}
