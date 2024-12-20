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

/** An variant object value. */
public interface VariantObject extends VariantValue {
  default int numElements() {
    throw new UnsupportedOperationException();
  }

  /** Returns the {@link VariantValue} for the field named {@code name} in this object. */
  VariantValue get(String name);

  /** Returns the names of fields stored in this object. */
  Iterable<String> fieldNames();

  /** Returns the number of fields stored in this object. */
  int numFields();

  @Override
  default Variants.PhysicalType type() {
    return Variants.PhysicalType.OBJECT;
  }

  @Override
  default VariantObject asObject() {
    return this;
  }
}
