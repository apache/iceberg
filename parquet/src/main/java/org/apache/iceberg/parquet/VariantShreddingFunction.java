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
package org.apache.iceberg.parquet;

import java.util.function.BiFunction;
import org.apache.parquet.schema.Type;

public interface VariantShreddingFunction extends BiFunction<Integer, String, Type> {
  /**
   * A function to produce the shredded type for a variant field. This function is called with the
   * ID and name of a variant field to produce the shredded type as a {@code typed_value} field.
   * This field is added to the result variant struct alongside the {@code metadata} and {@code
   * value} fields.
   *
   * @param fieldId field ID of the variant field to shred
   * @param name name of the variant field to shred
   * @return a Parquet {@link Type} to use as the Variant's {@code typed_value} field
   */
  @Override
  Type apply(Integer fieldId, String name);
}
