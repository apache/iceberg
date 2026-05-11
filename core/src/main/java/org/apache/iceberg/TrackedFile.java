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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.types.Types;

/** A file tracked by a manifest. */
interface TrackedFile {
  Types.NestedField TRACKING =
      Types.NestedField.required(
          147, "tracking", Tracking.schema(), "Tracking information for this entry");
  Types.NestedField CONTENT_TYPE =
      Types.NestedField.required(
          134,
          "content_type",
          Types.IntegerType.get(),
          "Type of content: 0=DATA, 2=EQUALITY_DELETES, 3=DATA_MANIFEST, 4=DELETE_MANIFEST");
  Types.NestedField LOCATION =
      Types.NestedField.required(100, "location", Types.StringType.get(), "Location of the file");
  Types.NestedField FILE_FORMAT =
      Types.NestedField.required(
          101,
          "file_format",
          Types.StringType.get(),
          "String file format name: avro, orc, or parquet");
  Types.NestedField RECORD_COUNT =
      Types.NestedField.required(
          103, "record_count", Types.LongType.get(), "Number of records in this file");
  Types.NestedField FILE_SIZE_IN_BYTES =
      Types.NestedField.required(
          104, "file_size_in_bytes", Types.LongType.get(), "Total file size in bytes");
  Types.NestedField SPEC_ID =
      Types.NestedField.optional(
          141, "spec_id", Types.IntegerType.get(), "Spec ID used to partition the file");

  int CONTENT_STATS_ID = 146;
  String CONTENT_STATS_NAME = "content_stats";
  String CONTENT_STATS_DOC = "Content statistics for this entry";

  Types.NestedField SORT_ORDER_ID =
      Types.NestedField.optional(
          140, "sort_order_id", Types.IntegerType.get(), "ID of the sort order for this file");
  Types.NestedField DELETION_VECTOR =
      Types.NestedField.optional(
          148, "deletion_vector", DeletionVector.schema(), "Deletion vector for the data file");
  Types.NestedField MANIFEST_INFO =
      Types.NestedField.optional(
          150,
          "manifest_info",
          ManifestInfo.schema(),
          "Metadata fields specific to manifest files");
  Types.NestedField KEY_METADATA =
      Types.NestedField.optional(
          131,
          "key_metadata",
          Types.BinaryType.get(),
          "Implementation-specific key metadata for encryption");
  Types.NestedField SPLIT_OFFSETS =
      Types.NestedField.optional(
          132,
          "split_offsets",
          Types.ListType.ofRequired(133, Types.LongType.get()),
          "Split offsets for the data file");
  Types.NestedField EQUALITY_IDS =
      Types.NestedField.optional(
          135,
          "equality_ids",
          Types.ListType.ofRequired(136, Types.IntegerType.get()),
          "Field ids used to determine row equality in equality delete files");

  static Types.StructType schemaWithContentStats(Types.StructType contentStatsType) {
    return Types.StructType.of(
        TRACKING,
        CONTENT_TYPE,
        LOCATION,
        FILE_FORMAT,
        RECORD_COUNT,
        FILE_SIZE_IN_BYTES,
        SPEC_ID,
        Types.NestedField.optional(
            CONTENT_STATS_ID, CONTENT_STATS_NAME, contentStatsType, CONTENT_STATS_DOC),
        SORT_ORDER_ID,
        DELETION_VECTOR,
        MANIFEST_INFO,
        KEY_METADATA,
        SPLIT_OFFSETS,
        EQUALITY_IDS);
  }

  /** Returns the tracking information for this entry. */
  Tracking tracking();

  /** Returns the type of content stored by this entry. */
  FileContent contentType();

  /** Returns the location of the file. */
  String location();

  /** Returns the format of the file. */
  FileFormat fileFormat();

  /** Returns the number of records in this file. */
  long recordCount();

  /** Returns the total file size in bytes. */
  long fileSizeInBytes();

  /** Returns the ID of the partition spec used to partition this file, or null. */
  Integer specId();

  /** Returns the content stats for this entry. */
  ContentStats contentStats();

  /** Returns the ID of the sort order for this file, or null. */
  Integer sortOrderId();

  /** Returns the deletion vector for this entry, or null if there is no deletion vector. */
  DeletionVector deletionVector();

  /** Returns the manifest summary information, or null for non-manifest entries. */
  ManifestInfo manifestInfo();

  /** Returns encryption key metadata, or null if the file is not encrypted. */
  ByteBuffer keyMetadata();

  /** Returns the list of recommended split locations, or null. */
  List<Long> splitOffsets();

  /** Returns the set of field IDs used for equality comparison in equality delete files. */
  List<Integer> equalityIds();

  /** Copies this tracked file. */
  TrackedFile copy();

  /**
   * Copies this tracked file with stats only for specific columns.
   *
   * @param requestedColumnIds table field IDs for which to keep stats
   */
  TrackedFile copyWithStats(Set<Integer> requestedColumnIds);

  /** Copies this tracked file without stats. */
  default TrackedFile copyWithoutStats() {
    return copyWithStats(Collections.emptySet());
  }
}
