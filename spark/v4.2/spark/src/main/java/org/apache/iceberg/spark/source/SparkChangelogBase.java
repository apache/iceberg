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
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.types.StructType;

abstract class SparkChangelogBase {

  static final String NAME = "changes";

  private final Table table;

  private SparkSession lazySpark = null;
  private StructType lazySparkSchema = null;
  private Column[] lazySparkColumns = null;

  SparkChangelogBase(Table table) {
    this.table = table;
  }

  public String name() {
    return table.name() + "." + NAME;
  }

  public Column[] columns() {
    if (lazySparkColumns == null) {
      this.lazySparkColumns =
          Arrays.stream(sparkSchema().fields())
              .map(field -> Column.create(field.name(), field.dataType(), field.nullable()))
              .toArray(Column[]::new);
    }

    return lazySparkColumns;
  }

  abstract Schema changelogSchema();

  Table table() {
    return table;
  }

  SparkSession spark() {
    if (lazySpark == null) {
      this.lazySpark = SparkSession.active();
    }

    return lazySpark;
  }

  StructType sparkSchema() {
    if (lazySparkSchema == null) {
      this.lazySparkSchema = SparkSchemaUtil.convert(changelogSchema());
    }

    return lazySparkSchema;
  }
}
