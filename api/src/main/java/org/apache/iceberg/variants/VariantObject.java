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

import java.util.Objects;

/** An variant object value. */
public interface VariantObject extends VariantValue {
  /** Returns the {@link VariantValue} for the field named {@code name} in this object. */
  VariantValue get(String name);

  /** Returns the names of fields stored in this object. */
  Iterable<String> fieldNames();

  /** Returns the number of fields stored in this object. */
  int numFields();

  @Override
  default PhysicalType type() {
    return PhysicalType.OBJECT;
  }

  @Override
  default VariantObject asObject() {
    return this;
  }

  static String asString(VariantObject object) {
    StringBuilder builder = new StringBuilder();

    builder.append("VariantObject(fields={");
    boolean first = true;
    for (String field : object.fieldNames()) {
      if (first) {
        first = false;
      } else {
        builder.append(", ");
      }

      builder.append(field).append(": ").append(object.get(field));
    }
    builder.append("})");

    return builder.toString();
  }

  static int hash(VariantObject self) {
    int hash = 17;
    for (String field : self.fieldNames()) {
      hash = 59 * hash + field.hashCode();
      hash = 59 * hash + self.get(field).hashCode();
    }

    return hash;
  }

  static boolean equals(VariantObject self, Object obj) {
    if (self == obj) {
      return true;
    }

    if (!(obj instanceof VariantObject)) {
      return false;
    }

    VariantObject other = (VariantObject) obj;
    if (self.numFields() != other.numFields()) {
      return false;
    }

    for (String field : self.fieldNames()) {
      if (!Objects.equals(self.get(field), other.get(field))) {
        return false;
      }
    }

    return true;
  }
}
