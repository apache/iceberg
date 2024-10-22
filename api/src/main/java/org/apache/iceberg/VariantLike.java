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
package org.apache.iceberg;

import org.apache.iceberg.types.Type;

/** Interface for accessing Variant fields. */
public interface VariantLike {
  int size();

  /**
   * Retrieves the value of the current variant element based on the provided `javaClass` type. The
   * `javaClass` parameter should be the `javaClass` field value of an Iceberg data type TypeID
   * {@link Type#typeId()}, such as `Boolean.class`.
   *
   * <p>If the current variant element holds a primitive value that can be extracted or promoted to
   * match the specified `javaClass`, the method will return that value. The method errors out if
   * the value is an object or fails to cast to the desired type.
   *
   * @param javaClass the Java class type to extract the value from the current variant element.
   * @return the extracted value if successful, or if the value cannot be extracted or promoted.
   */
  <T> T get(Class<T> javaClass);

  /**
   * Retrieves the sub-element from the current variant based on the provided path. The path is an
   * array of strings that represents the hierarchical path to access the sub-element within the
   * variant, such as ["a", "b"], for the path `a.b`.
   *
   * <p>If the sub-element exists for the specified path, it will return the corresponding
   * VariantLike` element. Otherwise, if the path is invalid or the sub-element is not found, this
   * method will return `null`. Empty array and null are invalid inputs.
   *
   * @param path an array of strings representing the hierarchical path to the sub-element.
   * @return the sub-element at the specified path as a `VariantLike`, or `null` if not found.
   */
  VariantLike get(String[] path);

  /**
   * Returns the JSON representation of the current variant.
   *
   * <p>If the variant element is an object, this method serializes it into a JSON string. For
   * primitive types such as boolean, int, long, float, double, and string, it returns the exact
   * value in its serialized form. For any other types, the value is serialized as a double-quoted
   * string.
   *
   * @return a JSON string representing the current variant.
   */
  String toJson();
}
