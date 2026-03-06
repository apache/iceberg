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
 * Metadata about a deletion vector (DV) associated with a data file entry.
 *
 * <p>In the combined entry model, each DATA entry may optionally carry DV information. The DV
 * content is stored at the specified location, offset, and size.
 *
 * <p>This struct may only be defined when content_type is DATA (0), and must be null for all other
 * content types.
 */
interface DeletionVector {
  Types.NestedField LOCATION =
      Types.NestedField.required(
          155, "location", Types.StringType.get(), "Location of the file containing the DV");
  Types.NestedField OFFSET =
      Types.NestedField.required(
          144, "offset", Types.LongType.get(), "Offset in the file where the DV content starts");
  Types.NestedField SIZE_IN_BYTES =
      Types.NestedField.required(
          145,
          "size_in_bytes",
          Types.LongType.get(),
          "Length of the referenced DV content stored in the file");
  Types.NestedField CARDINALITY =
      Types.NestedField.required(
          156, "cardinality", Types.LongType.get(), "Cardinality of the deletion vector");

  static Types.StructType schema() {
    return Types.StructType.of(LOCATION, OFFSET, SIZE_IN_BYTES, CARDINALITY);
  }

  /** Returns the location of the file containing the deletion vector. */
  String location();

  /** Returns the offset in the file where the deletion vector content starts. */
  long offset();

  /** Returns the size in bytes of the deletion vector content. */
  long sizeInBytes();

  /** Returns the cardinality (number of deleted positions) of the deletion vector. */
  long cardinality();
}
