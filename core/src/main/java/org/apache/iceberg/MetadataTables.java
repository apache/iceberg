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

import org.apache.iceberg.types.Types;

import static org.apache.iceberg.types.Types.NestedField.required;

class MetadataTables {
  private MetadataTables() {
  }

  static Schema entriesTableSchema(Types.StructType partitionType) {
    return ManifestEntry.wrapFileSchema(filesTableType(partitionType));
  }

  static Schema filesTableSchema(Types.StructType partitionType) {
    return new Schema(filesTableType(partitionType).fields());
  }

  private static Types.StructType filesTableType(Types.StructType partitionType) {
    if (partitionType.fields().size() > 0) {
      return Types.StructType.of(
          DataFile.FILE_PATH,
          DataFile.FILE_FORMAT,
          required(DataFile.PARTITION_ID, DataFile.PARTITION_NAME, partitionType),
          DataFile.RECORD_COUNT,
          DataFile.FILE_SIZE,
          DataFile.COLUMN_SIZES,
          DataFile.VALUE_COUNTS,
          DataFile.NULL_VALUE_COUNTS,
          DataFile.LOWER_BOUNDS,
          DataFile.UPPER_BOUNDS,
          DataFile.KEY_METADATA,
          DataFile.SPLIT_OFFSETS
      );
    } else {
      return Types.StructType.of(
          DataFile.FILE_PATH,
          DataFile.FILE_FORMAT,
          DataFile.RECORD_COUNT,
          DataFile.FILE_SIZE,
          DataFile.COLUMN_SIZES,
          DataFile.VALUE_COUNTS,
          DataFile.NULL_VALUE_COUNTS,
          DataFile.LOWER_BOUNDS,
          DataFile.UPPER_BOUNDS,
          DataFile.KEY_METADATA,
          DataFile.SPLIT_OFFSETS
      );
    }
  }
}
