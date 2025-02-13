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

/** A variant metadata and value pair. */
public interface Variant {
  /** Returns the metadata for all values in the variant. */
  VariantMetadata metadata();

  /** Returns the variant value. */
  VariantValue value();

  /**
   * Query the value with a given path.
   *
   * <p>The path format is a subset of JSON path that supports:
   * <ul>
   *   <li><code>$</code> the root value</li>
   *   <li><code>.name</code> accesses a field by name</li>
   * </ul>
   *
   * <p>If the query result is a list, the value is a variant array of results.
   *
   * @param path a JSON-path formatted string.
   */
  VariantValue query(String path);

  static Variant of(VariantMetadata metadata, VariantValue value) {
    return new VariantData(metadata, value);
  }
}
