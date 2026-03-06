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
import java.util.List;
import java.util.Set;
import org.apache.iceberg.stats.ContentStats;
import org.apache.iceberg.types.Types;

/**
 * Represents a v4 combined content entry in a manifest file.
 *
 * <p>TrackedFile is the v4 equivalent of ContentFile. It provides a unified representation for all
 * entry types in a v4 manifest: data files (optionally with an associated deletion vector),
 * equality delete files, and manifest entries.
 *
 * <p>In the combined entry model, data files and their deletion vectors are represented as a single
 * entry. Each DATA entry may optionally carry DV information via {@link #deletionVector()},
 * eliminating the need for separate delete manifest entries for position deletes and avoiding
 * 2-phase scan planning.
 */
interface TrackedFile {
  // Field IDs from v4 specification
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
          "String file format name: avro, orc, parquet, or puffin");
  Types.NestedField TRACKING_INFO =
      Types.NestedField.required(
          147, "tracking_info", TrackingInfo.schema(), "Tracking information for this entry");
  Types.NestedField DELETION_VECTOR =
      Types.NestedField.optional(
          148,
          "dv_info",
          DeletionVector.schema(),
          "Deletion vector info. May only be defined for content_type DATA, must be null otherwise");
  Types.NestedField PARTITION_SPEC_ID =
      Types.NestedField.required(
          149,
          "partition_spec_id",
          Types.IntegerType.get(),
          "ID of partition spec used to write manifest or data file");
  Types.NestedField SORT_ORDER_ID =
      Types.NestedField.optional(
          140,
          "sort_order_id",
          Types.IntegerType.get(),
          "ID representing sort order for this file. Can only be set if content_type is DATA");
  Types.NestedField RECORD_COUNT =
      Types.NestedField.required(
          103, "record_count", Types.LongType.get(), "Number of records in this file");
  Types.NestedField FILE_SIZE_IN_BYTES =
      Types.NestedField.required(
          104, "file_size_in_bytes", Types.LongType.get(), "Total file size in bytes");
  Types.NestedField CONTENT_STATS =
      Types.NestedField.optional(
          146,
          "content_stats",
          Types.StructType.of(), // schema is derived from table schema at read/write time
          "Content statistics for this entry");
  Types.NestedField MANIFEST_INFO =
      Types.NestedField.optional(
          150,
          "manifest_info",
          ManifestInfo.schema(),
          "Manifest information. Must be set for DATA_MANIFEST and DELETE_MANIFEST");
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
          "Split offsets for the data file. Must be sorted ascending");
  Types.NestedField EQUALITY_IDS =
      Types.NestedField.optional(
          135,
          "equality_ids",
          Types.ListType.ofRequired(136, Types.IntegerType.get()),
          "Field ids used to determine row equality in equality delete files. Required when content=2");

  /**
   * Returns the path of the manifest which this file is referenced in or null if it was not read
   * from a manifest.
   */
  String manifestLocation();

  /**
   * Returns the tracking information for this entry.
   *
   * <p>Contains status, snapshot ID, DV snapshot ID, sequence numbers, first-row-id, and changed
   * positions.
   */
  TrackingInfo trackingInfo();

  /** Returns the type of content stored by this entry. */
  FileContent contentType();

  /** Returns the location of the file. */
  String location();

  /** Returns the format of the file (avro, orc, parquet, or puffin). */
  FileFormat fileFormat();

  /**
   * Returns the deletion vector for this entry, or null if there is no deletion vector.
   *
   * <p>May only be defined when content_type is DATA. Must be null for all other content types.
   */
  DeletionVector deletionVector();

  /** Returns the ID of the partition spec used to write this file or manifest. */
  int partitionSpecId();

  /**
   * Returns the ID representing sort order for this file.
   *
   * <p>Can only be set if content_type is DATA.
   */
  Integer sortOrderId();

  /** Returns the number of records in this file. */
  long recordCount();

  /** Returns the total file size in bytes. */
  long fileSizeInBytes();

  /** Returns the content stats for this entry. */
  ContentStats contentStats();

  /**
   * Returns the manifest info for this entry.
   *
   * <p>Must be set if content_type is DATA_MANIFEST or DELETE_MANIFEST, otherwise must be null.
   * Contains file/row counts, min sequence number, and the manifest deletion vector.
   */
  ManifestInfo manifestInfo();

  /** Returns metadata about how this file is encrypted, or null if stored in plain text. */
  ByteBuffer keyMetadata();

  /**
   * Returns list of recommended split locations, if applicable, null otherwise.
   *
   * <p>Must be sorted in ascending order.
   */
  List<Long> splitOffsets();

  /**
   * Returns the set of field IDs used for equality comparison, in equality delete files.
   *
   * <p>Required when content_type is EQUALITY_DELETES, must be null otherwise.
   */
  List<Integer> equalityIds();

  /**
   * Copies this tracked file.
   *
   * <p>Manifest readers can reuse file instances; use this method to copy data when collecting
   * files from tasks.
   */
  TrackedFile copy();

  /**
   * Copies this tracked file without stats.
   *
   * <p>Use this method to copy data without stats when collecting files.
   */
  TrackedFile copyWithoutStats();

  /**
   * Copies this tracked file with stats only for specific columns.
   *
   * <p>Manifest readers can reuse file instances; use this method to copy data with stats only for
   * specific columns when collecting files.
   *
   * @param requestedColumnIds column IDs for which to keep stats
   * @return a copy of this tracked file, with content stats for only the requested columns
   */
  TrackedFile copyWithStats(Set<Integer> requestedColumnIds);
}
