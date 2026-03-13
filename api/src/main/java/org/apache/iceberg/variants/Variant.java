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

/** A variant metadata and value pair. */
public interface Variant {

  /** The current version of the Variant spec */
  byte VARIANT_SPEC_VERSION = (byte) 1;

  /** Returns the metadata for all values in the variant. */
  VariantMetadata metadata();

  /** Returns the variant value. */
  VariantValue value();

  static Variant of(VariantMetadata metadata, VariantValue value) {
    return new VariantData(metadata, value);
  }

  static Variant from(ByteBuffer buffer) {
    VariantMetadata metadata = VariantMetadata.from(buffer);
    ByteBuffer valueBuffer =
        VariantUtil.slice(
            buffer, metadata.sizeInBytes(), buffer.remaining() - metadata.sizeInBytes());
    VariantValue value = VariantValue.from(metadata, valueBuffer);
    return of(metadata, value);
  }

  static String toString(Variant variant) {
    return "Variant(metadata=" + variant.metadata() + ", value=" + variant.value() + ")";
  }
}
