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

/**
 * Statistics for manifest entries in a V4 tracked file.
 *
 * <p>This encapsulates added/removed/existing files/row counts and min_sequence_number for a
 * manifest. This must be defined when the content_type is a manifest (3 or 4), and null otherwise.
 */
public interface ManifestStats {
  Types.NestedField ADDED_FILES_COUNT =
      Types.NestedField.required(
          504, "added_files_count", Types.IntegerType.get(), "Number of files added");
  Types.NestedField EXISTING_FILES_COUNT =
      Types.NestedField.required(
          505, "existing_files_count", Types.IntegerType.get(), "Number of existing files");
  Types.NestedField DELETED_FILES_COUNT =
      Types.NestedField.required(
          506, "deleted_files_count", Types.IntegerType.get(), "Number of deleted files");
  Types.NestedField ADDED_ROWS_COUNT =
      Types.NestedField.required(
          512, "added_rows_count", Types.LongType.get(), "Number of rows in added files");
  Types.NestedField EXISTING_ROWS_COUNT =
      Types.NestedField.required(
          513, "existing_rows_count", Types.LongType.get(), "Number of rows in existing files");
  Types.NestedField DELETED_ROWS_COUNT =
      Types.NestedField.required(
          514, "deleted_rows_count", Types.LongType.get(), "Number of rows in deleted files");
  Types.NestedField MIN_SEQUENCE_NUMBER =
      Types.NestedField.required(
          516,
          "min_sequence_number",
          Types.LongType.get(),
          "Minimum sequence number of files in this manifest");

  /** Returns the number of files added by this manifest. */
  int addedFilesCount();

  /** Returns the number of existing files referenced by this manifest. */
  int existingFilesCount();

  /** Returns the number of deleted files in this manifest. */
  int deletedFilesCount();

  /** Returns the number of rows in added files. */
  long addedRowsCount();

  /** Returns the number of rows in existing files. */
  long existingRowsCount();

  /** Returns the number of rows in deleted files. */
  long deletedRowsCount();

  /** Returns the minimum sequence number of files in this manifest. */
  long minSequenceNumber();
}
