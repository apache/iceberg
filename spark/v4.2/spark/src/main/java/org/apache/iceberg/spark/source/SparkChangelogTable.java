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

import java.util.Set;
import org.apache.iceberg.ChangelogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.connector.catalog.SupportsMetadataColumns;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class SparkChangelogTable
    implements org.apache.spark.sql.connector.catalog.Table, SupportsRead, SupportsMetadataColumns {

  public static final String TABLE_NAME = "changes";

  private static final Set<TableCapability> CAPABILITIES =
      ImmutableSet.of(TableCapability.BATCH_READ);

  private final Table table;
  private final Schema schema;

  private SparkSession lazySpark = null;
  private StructType lazySparkSchema = null;

  public SparkChangelogTable(Table table) {
    this.table = table;
    this.schema = ChangelogUtil.changelogSchema(table.schema());
  }

  @Override
  public String name() {
    return table.name() + "." + TABLE_NAME;
  }

  @Override
  public StructType schema() {
    if (lazySparkSchema == null) {
      this.lazySparkSchema = SparkSchemaUtil.convert(schema);
    }

    return lazySparkSchema;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return CAPABILITIES;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new SparkChangelogScanBuilder(spark(), table, schema, options);
  }

  private SparkSession spark() {
    if (lazySpark == null) {
      this.lazySpark = SparkSession.active();
    }

    return lazySpark;
  }

  @Override
  public MetadataColumn[] metadataColumns() {
    return new MetadataColumn[] {
      SparkMetadataColumns.SPEC_ID,
      SparkMetadataColumns.partition(table),
      SparkMetadataColumns.FILE_PATH,
      SparkMetadataColumns.ROW_POSITION,
      SparkMetadataColumns.IS_DELETED,
    };
  }
}
