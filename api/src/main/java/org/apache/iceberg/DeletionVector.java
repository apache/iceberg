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

/**
 * Deletion vector information for a tracked file entry in a V4 manifest.
 *
 * <p>This encapsulates both inline and out-of-line deletion vector content. This struct must be
 * defined for types 1 (POSITION_DELETES) and 5 (MANIFEST_DV).
 */
public interface DeletionVector {
  Types.NestedField OFFSET =
      Types.NestedField.optional(
          144, "offset", Types.LongType.get(), "Offset in the file where the content starts");
  Types.NestedField SIZE_IN_BYTES =
      Types.NestedField.optional(
          145,
          "size_in_bytes",
          Types.LongType.get(),
          "Length of a referenced content stored in the file; required if offset is present");
  Types.NestedField INLINE_CONTENT =
      Types.NestedField.optional(
          146, "inline_content", Types.BinaryType.get(), "Serialized bitmap for inline DVs");

  /**
   * Returns the offset in the file where the deletion vector content starts.
   *
   * <p>This is used for out-of-line deletion vectors stored in Puffin files.
   */
  Long offset();

  /**
   * Returns the size in bytes of the deletion vector content.
   *
   * <p>Required if offset is present.
   */
  Long sizeInBytes();

  /**
   * Returns the serialized bitmap for inline deletion vectors.
   *
   * <p>When present, the deletion vector is stored inline in the manifest rather than in a separate
   * Puffin file.
   */
  ByteBuffer inlineContent();
}
