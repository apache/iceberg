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
package org.apache.iceberg.variants;

import java.nio.ByteBuffer;

/** A variant value. */
public interface VariantValue {
  /** Returns the {@link PhysicalType} of this value. */
  PhysicalType type();

  /** Returns the serialized size in bytes of this value. */
  int sizeInBytes();

  /**
   * Writes this value to the buffer at the given offset, ignoring the buffer's position and limit.
   */
  int writeTo(ByteBuffer buffer, int offset);

  /**
   * Returns this value as a {@link VariantPrimitive}.
   *
   * @throws IllegalArgumentException if the value is not a primitive
   */
  default VariantPrimitive<?> asPrimitive() {
    throw new IllegalArgumentException("Not a primitive: " + this);
  }

  /**
   * Returns this value as a {@link VariantObject}.
   *
   * @throws IllegalArgumentException if the value is not an object
   */
  default VariantObject asObject() {
    throw new IllegalArgumentException("Not an object: " + this);
  }

  /**
   * Returns this value as a {@link VariantArray}.
   *
   * @throws IllegalArgumentException if the value is not an array
   */
  default VariantArray asArray() {
    throw new IllegalArgumentException("Not an array: " + this);
  }

  static VariantValue from(VariantMetadata metadata, ByteBuffer value) {
    int header = VariantUtil.readByte(value, 0);
    BasicType basicType = VariantUtil.basicType(header);
    switch (basicType) {
      case PRIMITIVE:
        return SerializedPrimitive.from(value, header);
      case SHORT_STRING:
        return SerializedShortString.from(value, header);
      case OBJECT:
        return SerializedObject.from(metadata, value, header);
      case ARRAY:
        return SerializedArray.from(metadata, value, header);
    }

    throw new UnsupportedOperationException("Unsupported basic type: " + basicType);
  }
}
