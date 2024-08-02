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
package org.apache.iceberg.flink.sink.shuffle;

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

class SortKeyUtil {
  private SortKeyUtil() {}

  /** Compute the result schema of {@code SortKey} transformation */
  static Schema sortKeySchema(Schema schema, SortOrder sortOrder) {
    List<SortField> sortFields = sortOrder.fields();
    int size = sortFields.size();
    List<Types.NestedField> transformedFields = Lists.newArrayListWithCapacity(size);
    for (int i = 0; i < size; ++i) {
      int sourceFieldId = sortFields.get(i).sourceId();
      Types.NestedField sourceField = schema.findField(sourceFieldId);
      Preconditions.checkArgument(
          sourceField != null, "Cannot find source field: %s", sourceFieldId);
      Type transformedType = sortFields.get(i).transform().getResultType(sourceField.type());
      // There could be multiple transformations on the same source column, like in the PartitionKey
      // case. To resolve the collision, field id is set to transform index and field name is set to
      // sourceFieldName_transformIndex
      Types.NestedField transformedField =
          Types.NestedField.of(
              i,
              sourceField.isOptional(),
              sourceField.name() + '_' + i,
              transformedType,
              sourceField.doc());
      transformedFields.add(transformedField);
    }

    return new Schema(transformedFields);
  }
}
