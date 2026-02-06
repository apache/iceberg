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

public class NameMappingWithAvroSchema extends AvroWithTypeByStructureVisitor<MappedFields> {
  @Override
  public MappedFields record(
      Type struct, Schema record, List<String> names, List<MappedFields> fieldResults) {
    List<MappedField> fields = Lists.newArrayListWithExpectedSize(fieldResults.size());

    for (int i = 0; i < fieldResults.size(); i += 1) {
      Types.NestedField field = struct.asStructType().fields().get(i);
      MappedFields result = fieldResults.get(i);
      fields.add(MappedField.of(field.fieldId(), field.name(), result));
    }

    return MappedFields.of(fields);
  }

  @Override
  public MappedFields union(Type type, Schema union, List<MappedFields> optionResults) {
    if (AvroSchemaUtil.isOptionSchema(union)) {
      for (int i = 0; i < optionResults.size(); i += 1) {
        if (union.getTypes().get(i).getType() != Schema.Type.NULL) {
          return optionResults.get(i);
        }
      }
    } else { // Complex union
      Preconditions.checkArgument(
          type instanceof Types.StructType,
          "Cannot visit invalid Iceberg type: %s for Avro complex union type: %s",
          type,
          union);
      Types.StructType struct = (Types.StructType) type;
      List<MappedField> fields = Lists.newArrayListWithExpectedSize(optionResults.size());
      int index = 0;
      // Avro spec for union types states that unions may not contain more than one schema with the
      // same type, except for the named types record, fixed and enum. For example, unions
      // containing two array types or two map types are not permitted, but two named types with
      // different names are permitted.
      // Therefore, for non-named types, use the Avro type toString() as the field mapping key. For
      // named types, use the record name of the Avro type as the field mapping key.
      for (Schema option : union.getTypes()) {
        if (option.getType() != Schema.Type.NULL) {
          // Check if current option is a named type, i.e., a RECORD, ENUM, or FIXED type. If so,
          // use the record name of the Avro type as the field name. Otherwise, use the Avro
          // type toString().
          if (option.getType() == Schema.Type.RECORD
              || option.getType() == Schema.Type.ENUM
              || option.getType() == Schema.Type.FIXED) {
            fields.add(
                MappedField.of(
                    struct.fields().get(index).fieldId(),
                    option.getName(),
                    optionResults.get(index)));
          } else {
            fields.add(
                MappedField.of(
                    struct.fields().get(index).fieldId(),
                    option.getType().getName(),
                    optionResults.get(index)));
          }

          // Both iStruct and optionResults do not contain an entry for the NULL type, so we need to
          // increment i only when we encounter a non-NULL type.
          index++;
        }
      }
      return MappedFields.of(fields);
    }
    return null;
  }

  @Override
  public MappedFields array(Type list, Schema array, MappedFields elementResult) {
    return MappedFields.of(MappedField.of(list.asListType().elementId(), "element", elementResult));
  }

  @Override
  public MappedFields map(Type sMap, Schema map, MappedFields keyResult, MappedFields valueResult) {
    return MappedFields.of(
        MappedField.of(sMap.asMapType().keyId(), "key", keyResult),
        MappedField.of(sMap.asMapType().valueId(), "value", valueResult));
  }

  @Override
  public MappedFields map(Type sMap, Schema map, MappedFields valueResult) {
    return MappedFields.of(
        MappedField.of(sMap.asMapType().keyId(), "key", null),
        MappedField.of(sMap.asMapType().valueId(), "value", valueResult));
  }

  @Override
  public MappedFields primitive(Type type, Schema primitive) {
    return null; // no mapping because primitives have no nested fields
  }
}
