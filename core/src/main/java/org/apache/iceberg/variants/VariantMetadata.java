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

import java.util.NoSuchElementException;

/** A variant metadata dictionary. */
public interface VariantMetadata extends Variants.Serialized {
  /** Returns the ID for a {@code name} in the dictionary, or -1 if not present. */
  int id(String name);

  /**
   * Returns the field name for an ID in metadata.
   *
   * @throws NoSuchElementException if the dictionary does not contain the ID
   */
  String get(int id);
}
