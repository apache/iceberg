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
import org.apache.iceberg.types.Types;

/**
 * Represents a V4 content entry in a manifest file.
 *
 * <p>TrackedFile is the V4 equivalent of ContentFile. It provides a unified representation for all
 * entry types in a V4 manifest: data files, delete files, manifests, and deletion vectors.
 *
 * @param <F> the concrete class of a TrackedFile instance
 */
public interface TrackedFile<F> {
  // Field IDs from V4 specification
  Types.NestedField CONTENT_TYPE =
      Types.NestedField.required(
          134,
          "content_type",
          Types.IntegerType.get(),
          "Type of content: 0=DATA, 1=POSITION_DELETES, 2=EQUALITY_DELETES, 3=DATA_MANIFEST, 4=DELETE_MANIFEST, 5=MANIFEST_DV");
  Types.NestedField LOCATION =
      Types.NestedField.optional(
          100,
          "location",
          Types.StringType.get(),
          "Location of the file. Optional if content_type is 5 and deletion_vector.inline_content is not null");
  Types.NestedField FILE_FORMAT =
      Types.NestedField.required(
          101,
          "file_format",
          Types.StringType.get(),
          "String file format name: avro, orc, parquet, or puffin");
  Types.NestedField PARTITION_SPEC_ID =
      Types.NestedField.required(
          148,
          "partition_spec_id",
          Types.IntegerType.get(),
          "ID of partition spec used to write manifest or data/delete files");
  Types.NestedField SORT_ORDER_ID =
      Types.NestedField.optional(
          140,
          "sort_order_id",
          Types.IntegerType.get(),
          "ID representing sort order for this file. Can only be set if content_type is 0");
  Types.NestedField RECORD_COUNT =
      Types.NestedField.required(
          103,
          "record_count",
          Types.LongType.get(),
          "Number of records in this file, or the cardinality of a deletion vector");
  Types.NestedField FILE_SIZE_IN_BYTES =
      Types.NestedField.optional(
          104,
          "file_size_in_bytes",
          Types.LongType.get(),
          "Total file size in bytes. Must be defined if location is defined");
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
  Types.NestedField REFERENCED_FILE =
      Types.NestedField.optional(
          143,
          "referenced_file",
          Types.StringType.get(),
          "Location of data file that a DV references if content_type is 1 or 5."
              + " Location of affiliated data manifest if content_type is 4 or null if delete manifest is unaffiliated");

  /**
   * Returns the tracking information for this entry.
   *
   * <p>Contains status, snapshot ID, sequence numbers, and first-row-id. Optional - may be null if
   * tracking info is inherited.
   */
  TrackingInfo trackingInfo();

  /**
   * Returns the type of content stored by this entry.
   *
   * <p>One of: DATA, POSITION_DELETES, EQUALITY_DELETES, DATA_MANIFEST, DELETE_MANIFEST, or
   * MANIFEST_DV.
   */
  FileContent contentType();

  /**
   * Returns the location of the file.
   *
   * <p>Optional if content_type is MANIFEST_DV and deletion_vector has inline content.
   */
  String location();

  /** Returns the format of the file (avro, orc, parquet, or puffin). */
  FileFormat fileFormat();

  /**
   * Returns the deletion vector for this entry.
   *
   * <p>Must be defined if content_type is POSITION_DELETES or MANIFEST_DV. Must be null for all
   * other types.
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

  /** Returns the number of records in this file, or the cardinality of a deletion vector. */
  long recordCount();

  /**
   * Returns the total file size in bytes.
   *
   * <p>Must be defined if location is defined.
   */
  Long fileSizeInBytes();

  /**
   * Returns the content stats for this entry.
   *
   * <p>TODO: Define ContentStats structure per V4 proposal.
   */
  Object contentStats();

  /**
   * Returns the manifest stats for this entry.
   *
   * <p>Must be set if content_type is DATA_MANIFEST or DELETE_MANIFEST, otherwise must be null.
   */
  ManifestStats manifestStats();

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
   * Returns the location of the referenced file.
   *
   * <p>For POSITION_DELETES or MANIFEST_DV: location of data file that the DV references.
   *
   * <p>For DELETE_MANIFEST: location of affiliated data manifest, or null if unaffiliated.
   */
  String referencedFile();

  /**
   * Copies this tracked file.
   *
   * <p>Manifest readers can reuse file instances; use this method to copy data when collecting
   * files from tasks.
   */
  F copy();

  /**
   * Copies this tracked file without stats.
   *
   * <p>Use this method to copy data without stats when collecting files.
   */
  F copyWithoutStats();

  /**
   * Returns the ordinal position in the manifest.
   *
   * <p>Used for applying manifest deletion vectors.
   */
  Long pos();

  /** Set the status for this tracked file entry. */
  void setStatus(TrackingInfo.Status status);

  /** Set the snapshot ID for this tracked file entry. */
  void setSnapshotId(Long snapshotId);

  /** Set the data sequence number for this tracked file entry. */
  void setSequenceNumber(Long sequenceNumber);

  /** Set the file sequence number for this tracked file entry. */
  void setFileSequenceNumber(Long fileSequenceNumber);

  /** Set the first row ID for this tracked file entry. */
  void setFirstRowId(Long firstRowId);

  /** Set the ordinal position in the manifest. */
  void setPos(Long position);
}
