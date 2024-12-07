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

public final class Variant {
  private final byte[] value;
  private final byte[] metadata;
  // The variant value doesn't use the whole `value` binary, but starts from its `pos` index and
  // spans a size of `valueSize(value, pos)`. This design avoids frequent copies of the value binary
  // when reading a sub-variant in the array/object element.
  private final int pos;

  public Variant(byte[] value, byte[] metadata) {
    this(value, metadata, 0);
  }

  Variant(byte[] value, byte[] metadata, int pos) {
    this.value = value;
    this.metadata = metadata;
    this.pos = pos;
    // There is currently only one allowed version.
    if (metadata.length < 1
        || (metadata[0] & VariantConstants.VERSION_MASK) != VariantConstants.VERSION) {
      throw new IllegalStateException();
    }
    // Don't attempt to use a Variant larger than 16 MiB. We'll never produce one, and it risks
    // memory instability.
    if (metadata.length > VariantConstants.SIZE_LIMIT
        || value.length > VariantConstants.SIZE_LIMIT) {
      throw new VariantSizeLimitException();
    }
  }

  public byte[] getMetadata() {
    return metadata;
  }

  public byte[] getValue() {
    return value;
  }

  public int getPos() {
    return pos;
  }
}
