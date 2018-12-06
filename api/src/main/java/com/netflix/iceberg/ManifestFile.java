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

package com.netflix.iceberg;

import com.netflix.iceberg.types.Types;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Represents a manifest file that can be scanned to find data files in a table.
 */
public interface ManifestFile {
  Schema SCHEMA = new Schema(
      Types.NestedField.required(500, "manifest_path", Types.StringType.get()),
      Types.NestedField.required(501, "manifest_length", Types.LongType.get()),
      Types.NestedField.required(502, "partition_spec_id", Types.IntegerType.get()),
      Types.NestedField.optional(503, "added_snapshot_id", Types.LongType.get()),
      Types.NestedField.optional(504, "added_data_files_count", Types.IntegerType.get()),
      Types.NestedField.optional(505, "existing_data_files_count", Types.IntegerType.get()),
      Types.NestedField.optional(506, "deleted_data_files_count", Types.IntegerType.get()),
      Types.NestedField.optional(507, "partitions", Types.ListType.ofRequired(508, Types.StructType.of(
          Types.NestedField.required(509, "contains_null", Types.BooleanType.get()),
          Types.NestedField.optional(510, "lower_bound", Types.BinaryType.get()), // null if no non-null values
          Types.NestedField.optional(511, "upper_bound", Types.BinaryType.get())
      ))));

  static Schema schema() {
    return SCHEMA;
  }

  /**
   * @return fully qualified path to the file, suitable for constructing a Hadoop Path
   */
  String path();

  /**
   * @return length of the manifest file
   */
  long length();

  /**
   * @return ID of the {@link PartitionSpec} used to write the manifest file
   */
  int partitionSpecId();

  /**
   * @return ID of the snapshot that added the manifest file to table metadata
   */
  Long snapshotId();

  /**
   * @return the number of data files with status ADDED in the manifest file
   */
  Integer addedFilesCount();

  /**
   * @return the number of data files with status EXISTING in the manifest file
   */
  Integer existingFilesCount();

  /**
   * @return the number of data files with status DELETED in the manifest file
   */
  Integer deletedFilesCount();

  /**
   * Returns a list of {@link PartitionFieldSummary partition field summaries}.
   * <p>
   * Each summary corresponds to a field in the manifest file's partition spec, by ordinal. For
   * example, the partition spec [ ts_day=date(ts), type=identity(type) ] will have 2 summaries.
   * The first summary is for the ts_day partition field and the second is for the type partition
   * field.
   *
   * @return a list of partition field summaries, one for each field in the manifest's spec
   */
  List<PartitionFieldSummary> partitions();

  /**
   * Copies this {@link ManifestFile manifest file}. Readers can reuse manifest file instances; use
   * this method to make defensive copies.
   *
   * @return a copy of this manifest file
   */
  ManifestFile copy();

  /**
   * Summarizes the values of one partition field stored in a manifest file.
   */
  interface PartitionFieldSummary {
    Types.StructType TYPE = ManifestFile.schema()
        .findType("partitions")
        .asListType()
        .elementType()
        .asStructType();

    static Types.StructType getType() {
      return TYPE;
    }

    /**
     * @return true if at least one data file in the manifest has a null value for the field
     */
    boolean containsNull();

    /**
     * @return a ByteBuffer that contains a serialized bound lower than all values of the field
     */
    ByteBuffer lowerBound();

    /**
     * @return a ByteBuffer that contains a serialized bound higher than all values of the field
     */
    ByteBuffer upperBound();

    /**
     * Copies this {@link PartitionFieldSummary summary}. Readers can reuse instances; use this
     * method to make defensive copies.
     *
     * @return a copy of this partition field summary
     */
    PartitionFieldSummary copy();
  }
}
