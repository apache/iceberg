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
import java.nio.ByteOrder;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

class SerializedShortString implements VariantPrimitive<String>, SerializedValue {
  private static final int HEADER_SIZE = 1;
  private static final int LENGTH_MASK = 0b11111100;
  private static final int LENGTH_SHIFT = 2;

  static SerializedShortString from(byte[] bytes) {
    return from(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN), bytes[0]);
  }

  static SerializedShortString from(ByteBuffer value, int header) {
    Preconditions.checkArgument(
        value.order() == ByteOrder.LITTLE_ENDIAN, "Unsupported byte order: big endian");
    BasicType basicType = VariantUtil.basicType(header);
    Preconditions.checkArgument(
        basicType == BasicType.SHORT_STRING, "Invalid short string, basic type: " + basicType);
    return new SerializedShortString(value, header);
  }

  private final ByteBuffer value;
  private final int length;
  private String string = null;

  private SerializedShortString(ByteBuffer value, int header) {
    this.value = value;
    this.length = ((header & LENGTH_MASK) >> LENGTH_SHIFT);
  }

  @Override
  public PhysicalType type() {
    return PhysicalType.STRING;
  }

  @Override
  public String get() {
    if (null == string) {
      this.string = VariantUtil.readString(value, HEADER_SIZE, length);
    }
    return string;
  }

  @Override
  public ByteBuffer buffer() {
    return value;
  }

  @Override
  public int hashCode() {
    return VariantPrimitive.hash(this);
  }

  @Override
  public boolean equals(Object other) {
    return VariantPrimitive.equals(this, other);
  }

  @Override
  public String toString() {
    return VariantPrimitive.asString(this);
  }
}
