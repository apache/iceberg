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
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.types.DataTypes;

public class SparkMetadataColumns {

  private SparkMetadataColumns() {}

  public static final SparkMetadataColumn SPEC_ID =
      SparkMetadataColumn.builder()
          .name(MetadataColumns.SPEC_ID.name())
          .dataType(DataTypes.IntegerType)
          .withNullability(true)
          .build();

  public static final SparkMetadataColumn FILE_PATH =
      SparkMetadataColumn.builder()
          .name(MetadataColumns.FILE_PATH.name())
          .dataType(DataTypes.StringType)
          .withNullability(false)
          .build();

  public static final SparkMetadataColumn ROW_POSITION =
      SparkMetadataColumn.builder()
          .name(MetadataColumns.ROW_POSITION.name())
          .dataType(DataTypes.LongType)
          .withNullability(false)
          .build();

  public static final SparkMetadataColumn IS_DELETED =
      SparkMetadataColumn.builder()
          .name(MetadataColumns.IS_DELETED.name())
          .dataType(DataTypes.BooleanType)
          .withNullability(false)
          .build();

  public static final SparkMetadataColumn ROW_ID =
      SparkMetadataColumn.builder()
          .name(MetadataColumns.ROW_ID.name())
          .dataType(DataTypes.LongType)
          .withNullability(true)
          .preserveOnReinsert(true)
          .preserveOnUpdate(true)
          .preserveOnDelete(false)
          .build();

  public static final SparkMetadataColumn LAST_UPDATED_SEQUENCE_NUMBER =
      SparkMetadataColumn.builder()
          .name(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name())
          .dataType(DataTypes.LongType)
          .withNullability(true)
          .preserveOnReinsert(false)
          .preserveOnUpdate(false)
          .preserveOnDelete(false)
          .build();

  public static SparkMetadataColumn partition(Table table) {
    return SparkMetadataColumn.builder()
        .name(MetadataColumns.PARTITION_COLUMN_NAME)
        .dataType(SparkSchemaUtil.convert(Partitioning.partitionType(table)))
        .withNullability(true)
        .build();
  }
}
