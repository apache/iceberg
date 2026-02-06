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
import java.util.Objects;
import org.apache.iceberg.relocated.com.google.common.io.BaseEncoding;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.DateTimeUtil;

/** A primitive variant value. */
public interface VariantPrimitive<T> extends VariantValue {
  T get();

  @Override
  default VariantPrimitive<?> asPrimitive() {
    return this;
  }

  private String valueAsString() {
    switch (type()) {
      case DATE:
        return DateTimeUtil.daysToIsoDate((Integer) get());
      case TIME:
        return DateTimeUtil.microsToIsoTime((Long) get());
      case TIMESTAMPTZ:
        return DateTimeUtil.microsToIsoTimestamptz((Long) get());
      case TIMESTAMPNTZ:
        return DateTimeUtil.microsToIsoTimestamp((Long) get());
      case TIMESTAMPTZ_NANOS:
        return DateTimeUtil.nanosToIsoTimestamptz((Long) get());
      case TIMESTAMPNTZ_NANOS:
        return DateTimeUtil.nanosToIsoTimestamp((Long) get());
      case BINARY:
        return BaseEncoding.base16().encode(ByteBuffers.toByteArray((ByteBuffer) get()));
      default:
        return String.valueOf(get());
    }
  }

  static String asString(VariantPrimitive<?> primitive) {
    return "Variant(type=" + primitive.type() + ", value=" + primitive.valueAsString() + ")";
  }

  static int hash(VariantPrimitive<?> self) {
    return Objects.hash(self.type(), self.get());
  }

  static boolean equals(VariantPrimitive<?> self, Object obj) {
    if (self == obj) {
      return true;
    }

    if (!(obj instanceof VariantPrimitive)) {
      return false;
    }

    VariantPrimitive<?> other = (VariantPrimitive<?>) obj;
    return Objects.equals(self.type(), other.type()) && Objects.equals(self.get(), other.get());
  }
}
