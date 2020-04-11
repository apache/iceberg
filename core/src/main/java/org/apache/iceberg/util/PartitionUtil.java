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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class PartitionUtil {
  private PartitionUtil() {
  }

  public static Map<Integer, ?> constantsMap(FileScanTask task) {
    return constantsMap(task, (type, constant) -> constant);
  }

  public static Map<Integer, ?> constantsMap(FileScanTask task, BiFunction<Type, Object, Object> convertConstant) {
    return constantsMap(task.spec(), task.file().partition(), convertConstant);
  }

  private static Map<Integer, ?> constantsMap(PartitionSpec spec, StructLike partitionData,
                                              BiFunction<Type, Object, Object> convertConstant) {
    // use java.util.HashMap because partition data may contain null values
    Map<Integer, Object> idToConstant = new HashMap<>();
    List<Types.NestedField> partitionFields = spec.partitionType().fields();
    List<PartitionField> fields = spec.fields();
    for (int pos = 0; pos < fields.size(); pos += 1) {
      PartitionField field = fields.get(pos);
      if (field.transform().isIdentity()) {
        Object converted = convertConstant.apply(partitionFields.get(pos).type(), partitionData.get(pos, Object.class));
        idToConstant.put(field.sourceId(), converted);
      }
    }
    return idToConstant;
  }
}
