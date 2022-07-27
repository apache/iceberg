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

import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

// TODO: remove this class once we compile against Spark 3.2
public class SparkTestTable extends SparkTable {

  private final String[] metadataColumnNames;

  public SparkTestTable(Table icebergTable, String[] metadataColumnNames, boolean refreshEagerly) {
    super(icebergTable, refreshEagerly);
    this.metadataColumnNames = metadataColumnNames;
  }

  @Override
  public StructType schema() {
    StructType schema = super.schema();
    if (metadataColumnNames != null) {
      for (String columnName : metadataColumnNames) {
        Types.NestedField metadataColumn = MetadataColumns.metadataColumn(table(), columnName);
        schema = schema.add(columnName, SparkSchemaUtil.convert(metadataColumn.type()));
      }
    }
    return schema;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    SparkScanBuilder scanBuilder = (SparkScanBuilder) super.newScanBuilder(options);
    if (metadataColumnNames != null) {
      scanBuilder.withMetadataColumns(metadataColumnNames);
    }
    return scanBuilder;
  }
}
