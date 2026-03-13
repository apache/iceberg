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

/** An variant array value. */
public interface VariantArray extends VariantValue {
  /** Returns the {@link VariantValue} at {@code index} in this array. */
  VariantValue get(int index);

  /** Returns the number of fields stored in this array. */
  int numElements();

  @Override
  default PhysicalType type() {
    return PhysicalType.ARRAY;
  }

  @Override
  default VariantArray asArray() {
    return this;
  }

  static String asString(VariantArray arr) {
    StringBuilder builder = new StringBuilder();

    builder.append("VariantArray([");
    boolean first = true;
    for (int i = 0; i < arr.numElements(); i++) {
      if (first) {
        first = false;
      } else {
        builder.append(", ");
      }

      builder.append(arr.get(i));
    }
    builder.append("])");

    return builder.toString();
  }
}
