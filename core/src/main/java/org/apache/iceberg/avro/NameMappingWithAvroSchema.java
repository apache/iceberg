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
package org.apache.iceberg.avro;

import java.util.List;
import org.apache.avro.Schema;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.MappedFields;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class NameMappingWithAvroSchema extends AvroSchemaWithDerivedTypeVisitor<MappedFields> {

  @Override
  public MappedFields record(
      Types.StructType iStruct,
      Schema record,
      List<String> names,
      List<MappedFields> fieldResults) {
    List<MappedField> fields = Lists.newArrayListWithExpectedSize(fieldResults.size());

    for (int i = 0; i < fieldResults.size(); i += 1) {
      Types.NestedField field = iStruct.fields().get(i);
      MappedFields result = fieldResults.get(i);
      fields.add(MappedField.of(field.fieldId(), field.name(), result));
    }

    return MappedFields.of(fields);
  }

  @Override
  public MappedFields union(Type iType, Schema union, List<MappedFields> optionResults) {
    if (AvroSchemaUtil.isOptionSchema(union)) {
      for (int i = 0; i < optionResults.size(); i += 1) {
        if (union.getTypes().get(i).getType() != Schema.Type.NULL) {
          return optionResults.get(i);
        }
      }
    } else { // Complex union
      Preconditions.checkArgument(
          iType instanceof Types.StructType,
          "Cannot visit invalid Iceberg type: %s for Avro complex union type: %s",
          iType,
          union);
      Types.StructType iStruct = (Types.StructType) iType;
      List<MappedField> fields = Lists.newArrayListWithExpectedSize(optionResults.size());
      int index = 0;
      for (Schema option : union.getTypes()) {
        if (option.getType() != Schema.Type.NULL) {
          // Check if current option is a named type, i.e., a RECORD, ENUM, or FIXED type. If so,
          // use the record name
          // of the Avro type as the field name. Otherwise, use the Avro type toString().
          if (option.getType() == Schema.Type.RECORD
              || option.getType() == Schema.Type.ENUM
              || option.getType() == Schema.Type.FIXED) {
            fields.add(
                MappedField.of(
                    iStruct.fields().get(index).fieldId(),
                    option.getName(),
                    optionResults.get(index)));
          } else {
            fields.add(
                MappedField.of(
                    iStruct.fields().get(index).fieldId(),
                    option.toString(),
                    optionResults.get(index)));
          }
          // Both iStruct and optionResults do not contain an entry for the NULL type, so we need to
          // increment i only
          // when we encounter a non-NULL type.
          index++;
        }
      }
      return MappedFields.of(fields);
    }
    return null;
  }

  @Override
  public MappedFields array(Types.ListType iList, Schema array, MappedFields elementResult) {
    return MappedFields.of(MappedField.of(iList.elementId(), "element", elementResult));
  }

  @Override
  public MappedFields map(
      Types.MapType iMap, Schema map, MappedFields keyResult, MappedFields valueResult) {
    return MappedFields.of(
        MappedField.of(iMap.keyId(), "key", keyResult),
        MappedField.of(iMap.valueId(), "value", valueResult));
  }

  @Override
  public MappedFields map(Types.MapType iMap, Schema map, MappedFields valueResult) {
    return MappedFields.of(MappedField.of(iMap.valueId(), "value", valueResult));
  }

  @Override
  public MappedFields primitive(Type.PrimitiveType iPrimitive, Schema primitive) {
    return null; // no mapping because primitives have no nested fields
  }
}
