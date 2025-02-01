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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public final class VariantImpl implements Variant {
  // The mask to retrieve the version from first metadata byte
  private static final byte VERSION_MASK = 0x0F;

  private final VariantMetadata metadata;
  private final VariantValue value;

  public VariantImpl(ByteBuffer metadata, ByteBuffer value) {
    Preconditions.checkArgument(
        metadata != null && metadata.limit() > 0, "Metadata must not be null or empty.");
    Preconditions.checkArgument(
        value != null && value.limit() > 0, "Value must not be null or empty.");

    Preconditions.checkArgument(
        (metadata.get(0) & VERSION_MASK) == Variants.VERSION, "Unsupported metadata version.");

    this.metadata = SerializedMetadata.from(metadata);

    int header = value.get(0);
    Variants.BasicType basicType = VariantUtil.basicType(header);
    switch (basicType) {
      case PRIMITIVE:
        this.value = SerializedPrimitive.from(value.array());
        break;
      case ARRAY:
        this.value = SerializedArray.from((SerializedMetadata) this.metadata, value.array());
        break;
      case OBJECT:
        this.value = SerializedObject.from((SerializedMetadata) this.metadata, value.array());
        break;
      case SHORT_STRING:
        this.value = SerializedShortString.from(value.array());
        break;
      default:
        throw new UnsupportedOperationException("Unsupported basic type: " + basicType);
    }
  }

  @Override
  public VariantMetadata metadata() {
    return metadata;
  }

  @Override
  public VariantValue value() {
    return value;
  }
}
