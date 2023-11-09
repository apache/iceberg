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
package org.apache.iceberg.util;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class PartitionUtil {
  private PartitionUtil() {}

  public static Map<Integer, ?> constantsMap(ContentScanTask<?> task) {
    return constantsMap(task, null, (type, constant) -> constant);
  }

  public static Map<Integer, ?> constantsMap(
      ContentScanTask<?> task, BiFunction<Type, Object, Object> convertConstant) {
    return constantsMap(task, null, convertConstant);
  }

  public static Map<Integer, ?> constantsMap(
      ContentScanTask<?> task,
      Types.StructType partitionType,
      BiFunction<Type, Object, Object> convertConstant) {
    PartitionSpec spec = task.spec();
    StructLike partitionData = task.file().partition();

    // use java.util.HashMap because partition data may contain null values
    Map<Integer, Object> idToConstant = Maps.newHashMap();

    // add _file
    idToConstant.put(
        MetadataColumns.FILE_PATH.fieldId(),
        convertConstant.apply(Types.StringType.get(), task.file().path()));

    // add _spec_id
    idToConstant.put(
        MetadataColumns.SPEC_ID.fieldId(),
        convertConstant.apply(Types.IntegerType.get(), task.file().specId()));

    // add _partition
    if (partitionType != null) {
      if (!partitionType.fields().isEmpty()) {
        StructLike coercedPartition = coercePartition(partitionType, spec, partitionData);
        idToConstant.put(
            MetadataColumns.PARTITION_COLUMN_ID,
            convertConstant.apply(partitionType, coercedPartition));
      } else {
        // use null as some query engines may not be able to handle empty structs
        idToConstant.put(MetadataColumns.PARTITION_COLUMN_ID, null);
      }
    }

    List<Types.NestedField> partitionFields = spec.partitionType().fields();
    List<PartitionField> fields = spec.fields();
    for (int pos = 0; pos < fields.size(); pos += 1) {
      PartitionField field = fields.get(pos);
      if (field.transform().isIdentity()) {
        Object converted =
            convertConstant.apply(
                partitionFields.get(pos).type(), partitionData.get(pos, Object.class));
        idToConstant.put(field.sourceId(), converted);
      }
    }

    return idToConstant;
  }

  // adapts the provided partition data to match the table partition type
  public static StructLike coercePartition(
      Types.StructType partitionType, PartitionSpec spec, StructLike partition) {
    StructProjection projection =
        StructProjection.createAllowMissing(spec.partitionType(), partitionType);
    projection.wrap(partition);
    return projection;
  }
}
