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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type.Repetition;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * A visitor that converts a {@link MessageType} to a {@link Type} in Iceberg without assigning ids.
 * Columns without ids are pruned.
 */
public class MessageTypeToTypeWithoutAssigningIds extends BaseMessageTypeToType {
  private static final Joiner DOT = Joiner.on(".");

  private final Map<String, Integer> aliasToId = Maps.newHashMap();

  MessageTypeToTypeWithoutAssigningIds() {}

  public Map<String, Integer> getAliases() {
    return aliasToId;
  }

  @Override
  public Type message(MessageType message, List<Type> fields) {
    return struct(message, fields);
  }

  @Override
  public Type struct(GroupType parquetStruct, List<Type> fieldTypes) {
    List<org.apache.parquet.schema.Type> parquetFields = parquetStruct.getFields();
    List<Types.NestedField> fields = Lists.newArrayList();

    for (int i = 0; i < parquetFields.size(); i += 1) {
      org.apache.parquet.schema.Type parquetField = parquetFields.get(i);

      Preconditions.checkArgument(
          !parquetField.isRepetition(Repetition.REPEATED),
          "Fields cannot have repetition REPEATED: %s", parquetField);

      Integer fieldId = getId(parquetField);
      Type fieldType = fieldTypes.get(i);

      // keep the field if it has an id and it was not pruned (i.e. its type is not null)
      if (fieldId != null && fieldType != null) {
        addAlias(parquetField.getName(), fieldId);

        if (parquetFields.get(i).isRepetition(Repetition.OPTIONAL)) {
          fields.add(optional(fieldId, parquetField.getName(), fieldType));
        } else {
          fields.add(required(fieldId, parquetField.getName(), fieldType));
        }
      }
    }

    return fields.isEmpty() ? null : Types.StructType.of(fields);
  }

  @Override
  public Type list(GroupType parquetList, Type elementType) {
    GroupType repeated = parquetList.getType(0).asGroupType();
    org.apache.parquet.schema.Type parquetElement = repeated.getType(0);

    Preconditions.checkArgument(
        !parquetElement.isRepetition(Repetition.REPEATED),
        "Elements cannot have repetition REPEATED: %s", parquetElement);

    Integer elementFieldId = getId(parquetElement);

    // keep the list if its element has an id and it was not pruned (i.e. its type is not null)
    if (elementFieldId != null && elementType != null) {
      addAlias(parquetElement.getName(), elementFieldId);

      if (parquetElement.isRepetition(Repetition.OPTIONAL)) {
        return Types.ListType.ofOptional(elementFieldId, elementType);
      } else {
        return Types.ListType.ofRequired(elementFieldId, elementType);
      }
    }

    return null;
  }

  @Override
  public Type map(GroupType map, Type keyType, Type valueType) {
    GroupType keyValue = map.getType(0).asGroupType();
    org.apache.parquet.schema.Type parquetKey = keyValue.getType(0);
    org.apache.parquet.schema.Type parquetValue = keyValue.getType(1);

    Integer keyFieldId = getId(parquetKey);
    Integer valueFieldId = getId(parquetValue);

    // keep the map if its key and values have ids and were not pruned (i.e. their types are not null)
    if (keyFieldId != null && valueFieldId != null && keyType != null && valueType != null) {
      addAlias(parquetKey.getName(), keyFieldId);
      addAlias(parquetValue.getName(), valueFieldId);

      // check only values as keys are required by the spec
      if (parquetValue.isRepetition(Repetition.OPTIONAL)) {
        return Types.MapType.ofOptional(keyFieldId, valueFieldId, keyType, valueType);
      } else {
        return Types.MapType.ofRequired(keyFieldId, valueFieldId, keyType, valueType);
      }
    }

    return null;
  }

  private void addAlias(String name, int fieldId) {
    aliasToId.put(DOT.join(path(name)), fieldId);
  }

  private Integer getId(org.apache.parquet.schema.Type type) {
    return type.getId() != null ? type.getId().intValue() : null;
  }
}
