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
 * Metadata about externally stored content such as deletion vectors.
 *
 * <p>The deletion vector content is stored at the specified offset and size within the file
 * referenced by {@link TrackedFile#location()}.
 *
 * <p>This struct must be defined when content_type is POSITION_DELETES, and must be null otherwise.
 *
 * <p>Note: For manifest-level deletion vectors (marking entries in a manifest as deleted), see
 * {@link TrackedFile#manifestDV()} which stores the DV inline as a binary field.
 */
interface ContentInfo {
  Types.NestedField OFFSET =
      Types.NestedField.required(
          144, "offset", Types.LongType.get(), "Offset in the file where the content starts");
  Types.NestedField SIZE_IN_BYTES =
      Types.NestedField.required(
          145,
          "size_in_bytes",
          Types.LongType.get(),
          "Length of the referenced content stored in the file");

  static Types.StructType schema() {
    return Types.StructType.of(OFFSET, SIZE_IN_BYTES);
  }

  /**
   * Returns the offset in the file where the deletion vector content starts.
   *
   * <p>The file location is specified in the {@link TrackedFile#location()} field.
   */
  long offset();

  /** Returns the size in bytes of the deletion vector content. */
  long sizeInBytes();
}
