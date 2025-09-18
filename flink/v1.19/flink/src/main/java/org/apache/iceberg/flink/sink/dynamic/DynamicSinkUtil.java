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
package org.apache.iceberg.flink.sink.dynamic;

import java.util.Collections;
import java.util.Set;
import org.apache.hadoop.util.Sets;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;

class DynamicSinkUtil {

  private DynamicSinkUtil() {}

  static Set<Integer> getEqualityFieldIds(Set<String> equalityFields, Schema schema) {
    if (equalityFields == null || equalityFields.isEmpty()) {
      if (!schema.identifierFieldIds().isEmpty()) {
        return schema.identifierFieldIds();
      } else {
        return Collections.emptySet();
      }
    }

    Set<Integer> equalityFieldIds = Sets.newHashSetWithExpectedSize(equalityFields.size());
    for (String equalityField : equalityFields) {
      Types.NestedField field = schema.findField(equalityField);
      Preconditions.checkNotNull(
          field, "Equality field %s does not exist in schema", equalityField);
      equalityFieldIds.add(field.fieldId());
    }

    return equalityFieldIds;
  }

  static int safeAbs(int input) {
    if (input >= 0) {
      return input;
    }

    if (input == Integer.MIN_VALUE) {
      // -Integer.MIN_VALUE would be Integer.MIN_VALUE due to integer overflow. Map to
      // Integer.MAX_VALUE instead!
      return Integer.MAX_VALUE;
    }

    return -input;
  }
}
