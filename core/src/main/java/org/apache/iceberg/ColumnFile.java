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

interface ColumnFile {
  Types.NestedField LOCATION =
      Types.NestedField.required(
          160, "location", Types.StringType.get(), "Location of the column file");
  Types.NestedField FIELD_IDS =
      Types.NestedField.required(
          161,
          "field_ids",
          Types.ListType.ofRequired(162, Types.IntegerType.get()),
          "Live field IDs in this column file");
  Types.NestedField FILE_FORMAT =
      Types.NestedField.required(
          163,
          "file_format",
          Types.StringType.get(),
          "String file format name for this column file");
  Types.NestedField FILE_SIZE_IN_BYTES =
      Types.NestedField.required(
          164, "file_size_in_bytes", Types.LongType.get(), "Total column file size in bytes");
  Types.NestedField KEY_METADATA =
      Types.NestedField.optional(
          165,
          "key_metadata",
          Types.BinaryType.get(),
          "Key metadata for encryption; specific to the encryption scheme.");

  static Types.StructType schema() {
    return Types.StructType.of(LOCATION, FIELD_IDS, FILE_FORMAT, FILE_SIZE_IN_BYTES, KEY_METADATA);
  }

  /** Returns the location of this column file. */
  String location();

  /** Returns the field IDs contained in this column file. */
  List<Integer> fieldIds();

  /** Returns the format of this column file. */
  FileFormat fileFormat();

  /** Returns the total size of this column file in bytes. */
  long fileSizeInBytes();

  /** Returns encryption key metadata, or null if this column file is not encrypted. */
  ByteBuffer keyMetadata();

  /** Copies this column file. */
  ColumnFile copy();
}
