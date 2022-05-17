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
package org.apache.iceberg;

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;

public class MetadataColumns {

  private MetadataColumns() {}

  // IDs Integer.MAX_VALUE - (1-100) are used for metadata columns
  public static final NestedField FILE_PATH =
      NestedField.required(
          Integer.MAX_VALUE - 1,
          "_file",
          Types.StringType.get(),
          "Path of the file in which a row is stored");
  public static final NestedField ROW_POSITION =
      NestedField.required(
          Integer.MAX_VALUE - 2,
          "_pos",
          Types.LongType.get(),
          "Ordinal position of a row in the source data file");
  public static final NestedField IS_DELETED =
      NestedField.required(
          Integer.MAX_VALUE - 3,
          "_deleted",
          Types.BooleanType.get(),
          "Whether the row has been deleted");
  public static final NestedField SPEC_ID =
      NestedField.required(
          Integer.MAX_VALUE - 4,
          "_spec_id",
          Types.IntegerType.get(),
          "Spec ID used to track the file containing a row");
  // the partition column type is not static and depends on all specs in the table
  public static final int PARTITION_COLUMN_ID = Integer.MAX_VALUE - 5;
  public static final String PARTITION_COLUMN_NAME = "_partition";
  public static final String PARTITION_COLUMN_DOC = "Partition to which a row belongs to";

  // IDs Integer.MAX_VALUE - (101-200) are used for reserved columns
  public static final NestedField DELETE_FILE_PATH =
      NestedField.required(
          Integer.MAX_VALUE - 101,
          "file_path",
          Types.StringType.get(),
          "Path of a file in which a deleted row is stored");
  public static final NestedField DELETE_FILE_POS =
      NestedField.required(
          Integer.MAX_VALUE - 102,
          "pos",
          Types.LongType.get(),
          "Ordinal position of a deleted row in the data file");
  public static final String DELETE_FILE_ROW_FIELD_NAME = "row";
  public static final int DELETE_FILE_ROW_FIELD_ID = Integer.MAX_VALUE - 103;
  public static final String DELETE_FILE_ROW_DOC = "Deleted row values";
  public static final int POSITION_DELETE_TABLE_PARTITION_FIELD_ID = Integer.MAX_VALUE - 104;
  public static final int POSITION_DELETE_TABLE_SPEC_ID = Integer.MAX_VALUE - 105;
  public static final int POSITION_DELETE_TABLE_FILE_PATH = Integer.MAX_VALUE - 106;

  private static final Map<String, NestedField> META_COLUMNS =
      ImmutableMap.of(
          FILE_PATH.name(), FILE_PATH,
          ROW_POSITION.name(), ROW_POSITION,
          IS_DELETED.name(), IS_DELETED,
          SPEC_ID.name(), SPEC_ID);

  private static final Set<Integer> META_IDS =
      ImmutableSet.of(
          FILE_PATH.fieldId(),
          ROW_POSITION.fieldId(),
          IS_DELETED.fieldId(),
          SPEC_ID.fieldId(),
          PARTITION_COLUMN_ID,
          POSITION_DELETE_TABLE_PARTITION_FIELD_ID,
          POSITION_DELETE_TABLE_SPEC_ID,
          POSITION_DELETE_TABLE_FILE_PATH);

  /**
   * Returns ids of all known constant and metadata columns (to be avoided when projecting from
   * content files)
   */
  public static Set<Integer> metadataFieldIds() {
    return META_IDS;
  }

  public static NestedField metadataColumn(Table table, String name) {
    if (name.equals(PARTITION_COLUMN_NAME)) {
      return Types.NestedField.optional(
          PARTITION_COLUMN_ID,
          PARTITION_COLUMN_NAME,
          Partitioning.partitionType(table),
          PARTITION_COLUMN_DOC);
    } else {
      return META_COLUMNS.get(name);
    }
  }

  public static boolean isMetadataColumn(String name) {
    return name.equals(PARTITION_COLUMN_NAME) || META_COLUMNS.containsKey(name);
  }

  public static boolean nonMetadataColumn(String name) {
    return !isMetadataColumn(name);
  }
}
