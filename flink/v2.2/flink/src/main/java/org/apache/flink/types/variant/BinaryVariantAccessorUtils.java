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
package org.apache.flink.types.variant;

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * These Accessor utils should be part of Flink Variant interface itself. Added in the PR, <a
 * href="https://github.com/apache/flink/pull/27600">...</a>. This is a workaround until we upgrade
 * Flink to a release version with above changes.
 */
public class BinaryVariantAccessorUtils extends BinaryVariantUtil {
  /**
   * Get the size of an array variant.
   *
   * @return Number of elements if this variant is an instance of BinaryVariant and an array
   * @throws IllegalArgumentException If this variant is not an instance of BinaryVariant or not an
   *     array.
   */
  public static int arraySize(Variant variant) {
    Preconditions.checkArgument(
        variant instanceof BinaryVariant,
        "Invalid variant implementation instance:%s. Only BinaryVariant is supported.",
        variant.getClass().getName());

    Preconditions.checkArgument(
        variant.getType() == Variant.Type.ARRAY,
        "Invalid variant type:%s. Expecting ARRAY variant type.",
        variant.getType());

    BinaryVariant binaryArrayVariant = (BinaryVariant) variant;
    return handleArray(
        binaryArrayVariant.getValue(), 0, (size, offsetSize, offsetStart, dataStart) -> size);
  }

  /**
   * Get the field names of an object variant only at top level. Doesn't include the nested fields.
   *
   * @return List of field names if this is an instance of BinaryVariant and is an object
   * @throws IllegalArgumentException If this variant is not an instance of BinaryVariant or not an
   *     object.
   */
  public static List<String> fieldNames(Variant variant) {
    Preconditions.checkArgument(
        variant instanceof BinaryVariant,
        "Invalid variant implementation instance:%s. Only BinaryVariant is supported.",
        variant.getClass().getName());

    Preconditions.checkArgument(
        variant.getType() == Variant.Type.OBJECT,
        "Invalid variant type:%s. Expecting OBJECT variant type.",
        variant.getType());

    BinaryVariant binaryArrayVariant = (BinaryVariant) variant;
    return BinaryVariantUtil.handleObject(
        binaryArrayVariant.getValue(),
        0,
        (size, idSize, offsetSize, idStart, offsetStart, dataStart) -> {
          List<String> fieldNames = Lists.newArrayList();
          for (int i = 0; i < size; i++) {
            int id = readUnsigned(binaryArrayVariant.getValue(), idStart + idSize * i, idSize);
            String fieldName = getMetadataKey(binaryArrayVariant.getMetadata(), id);
            fieldNames.add(fieldName);
          }
          return fieldNames;
        });
  }
}
