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

import java.nio.ByteBuffer;
import org.apache.iceberg.types.Types;

/** Summary information about a manifest referenced by a root manifest entry. */
interface ManifestInfo {
  Types.NestedField ADDED_FILES_COUNT =
      Types.NestedField.required(
          504, "added_files_count", Types.IntegerType.get(), "Number of files added");
  Types.NestedField EXISTING_FILES_COUNT =
      Types.NestedField.required(
          505, "existing_files_count", Types.IntegerType.get(), "Number of existing files");
  Types.NestedField DELETED_FILES_COUNT =
      Types.NestedField.required(
          506, "deleted_files_count", Types.IntegerType.get(), "Number of deleted files");
  Types.NestedField REPLACED_FILES_COUNT =
      Types.NestedField.required(
          520, "replaced_files_count", Types.IntegerType.get(), "Number of replaced files");
  Types.NestedField ADDED_ROWS_COUNT =
      Types.NestedField.required(
          512, "added_rows_count", Types.LongType.get(), "Number of rows in added files");
  Types.NestedField EXISTING_ROWS_COUNT =
      Types.NestedField.required(
          513, "existing_rows_count", Types.LongType.get(), "Number of rows in existing files");
  Types.NestedField DELETED_ROWS_COUNT =
      Types.NestedField.required(
          514, "deleted_rows_count", Types.LongType.get(), "Number of rows in deleted files");
  Types.NestedField REPLACED_ROWS_COUNT =
      Types.NestedField.required(
          521, "replaced_rows_count", Types.LongType.get(), "Number of rows in replaced files");
  Types.NestedField MIN_SEQUENCE_NUMBER =
      Types.NestedField.required(
          516,
          "min_sequence_number",
          Types.LongType.get(),
          "Minimum sequence number of files in this manifest");
  Types.NestedField DV =
      Types.NestedField.optional(
          522, "dv", Types.BinaryType.get(), "Deletion vector for manifest entries");
  Types.NestedField DV_CARDINALITY =
      Types.NestedField.optional(
          523,
          "dv_cardinality",
          Types.LongType.get(),
          "Number of entries marked as deleted in the DV");

  static Types.StructType schema() {
    return Types.StructType.of(
        ADDED_FILES_COUNT,
        EXISTING_FILES_COUNT,
        DELETED_FILES_COUNT,
        REPLACED_FILES_COUNT,
        ADDED_ROWS_COUNT,
        EXISTING_ROWS_COUNT,
        DELETED_ROWS_COUNT,
        REPLACED_ROWS_COUNT,
        MIN_SEQUENCE_NUMBER,
        DV,
        DV_CARDINALITY);
  }

  /** Returns the number of files added by this manifest. */
  int addedFilesCount();

  /** Returns the number of existing files referenced by this manifest. */
  int existingFilesCount();

  /** Returns the number of deleted files in this manifest. */
  int deletedFilesCount();

  /** Returns the number of replaced files in this manifest. */
  int replacedFilesCount();

  /** Returns the number of rows in added files. */
  long addedRowsCount();

  /** Returns the number of rows in existing files. */
  long existingRowsCount();

  /** Returns the number of rows in deleted files. */
  long deletedRowsCount();

  /** Returns the number of rows in replaced files. */
  long replacedRowsCount();

  /** Returns the minimum sequence number of files in this manifest. */
  long minSequenceNumber();

  /** Returns the deletion vector bitmap, or null if not present. */
  ByteBuffer dv();

  /** Returns the number of entries marked as deleted in the DV, or null if not present. */
  Long dvCardinality();
}
