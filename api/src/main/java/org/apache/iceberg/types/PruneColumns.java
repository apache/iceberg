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
package org.apache.iceberg.types;

import java.util.List;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.StructType;

class PruneColumns extends TypeUtil.SchemaVisitor<Type> {
  private final Set<Integer> selected;
  private final boolean selectFullTypes;

  /**
   * Visits a schema and returns only the fields selected by the id set.
   *
   * <p>When selectFullTypes is false selecting list or map types is undefined and forbidden.
   *
   * @param selected ids of elements to return
   * @param selectFullTypes whether to select all subfields of a selected nested type
   */
  PruneColumns(Set<Integer> selected, boolean selectFullTypes) {
    Preconditions.checkNotNull(selected, "Selected field ids cannot be null");
    this.selected = selected;
    this.selectFullTypes = selectFullTypes;
  }

  @Override
  public Type schema(Schema schema, Type structResult) {
    return structResult;
  }

  @Override
  public Type struct(Types.StructType struct, List<Type> fieldResults) {
    List<Types.NestedField> fields = struct.fields();
    List<Types.NestedField> selectedFields = Lists.newArrayListWithExpectedSize(fields.size());
    boolean sameTypes = true;

    for (int i = 0; i < fieldResults.size(); i += 1) {
      Types.NestedField field = fields.get(i);
      Type projectedType = fieldResults.get(i);
      if (field.type() == projectedType) {
        // uses identity because there is no need to check structure. if identity doesn't match
        // then structure should not either.
        selectedFields.add(field);
      } else if (projectedType != null) {
        sameTypes = false; // signal that some types were altered
        if (field.isOptional()) {
          selectedFields.add(
              Types.NestedField.optional(
                  field.fieldId(), field.name(), projectedType, field.doc()));
        } else {
          selectedFields.add(
              Types.NestedField.required(
                  field.fieldId(), field.name(), projectedType, field.doc()));
        }
      }
    }

    if (!selectedFields.isEmpty()) {
      if (selectedFields.size() == fields.size() && sameTypes) {
        return struct;
      } else {
        return Types.StructType.of(selectedFields);
      }
    }

    return null;
  }

  @Override
  public Type field(Types.NestedField field, Type fieldResult) {
    if (selected.contains(field.fieldId())) {
      if (selectFullTypes) {
        return field.type();
      } else if (field.type().isStructType()) {
        return projectSelectedStruct(fieldResult);
      } else {
        Preconditions.checkArgument(
            !field.type().isNestedType(),
            "Cannot explicitly project List or Map types, %s:%s of type %s was selected",
            field.fieldId(),
            field.name(),
            field.type());
        // Selected non-struct field
        return field.type();
      }
    } else if (fieldResult != null) {
      // This field wasn't selected but a subfield was so include that
      return fieldResult;
    }
    return null;
  }

  @Override
  public Type list(Types.ListType list, Type elementResult) {
    if (selected.contains(list.elementId())) {
      if (selectFullTypes) {
        return list;
      } else if (list.elementType().isStructType()) {
        StructType projectedStruct = projectSelectedStruct(elementResult);
        return projectList(list, projectedStruct);
      } else {
        Preconditions.checkArgument(
            list.elementType().isPrimitiveType(),
            "Cannot explicitly project List or Map types, List element %s of type %s was selected",
            list.elementId(),
            list.elementType());
        return list;
      }
    } else if (elementResult != null) {
      return projectList(list, elementResult);
    }
    return null;
  }

  @Override
  public Type map(Types.MapType map, Type ignored, Type valueResult) {
    if (selected.contains(map.valueId())) {
      if (selectFullTypes) {
        return map;
      } else if (map.valueType().isStructType()) {
        Type projectedStruct = projectSelectedStruct(valueResult);
        return projectMap(map, projectedStruct);
      } else {
        Preconditions.checkArgument(
            map.valueType().isPrimitiveType(),
            "Cannot explicitly project List or Map types, Map value %s of type %s was selected",
            map.valueId(),
            map.valueType());
        return map;
      }
    } else if (valueResult != null) {
      return projectMap(map, valueResult);
    } else if (selected.contains(map.keyId())) {
      // right now, maps can't be selected without values
      return map;
    }
    return null;
  }

  @Override
  public Type variant(Types.VariantType variant) {
    return null;
  }

  @Override
  public Type primitive(Type.PrimitiveType primitive) {
    return null;
  }

  private ListType projectList(ListType list, Type elementResult) {
    Preconditions.checkArgument(
        elementResult != null, "Cannot project a list when the element result is null");
    if (list.elementType() == elementResult) {
      return list;
    } else if (list.isElementOptional()) {
      return Types.ListType.ofOptional(list.elementId(), elementResult);
    } else {
      return Types.ListType.ofRequired(list.elementId(), elementResult);
    }
  }

  private MapType projectMap(MapType map, Type valueResult) {
    Preconditions.checkArgument(
        valueResult != null, "Attempted to project a map without a defined map value type");
    if (map.valueType() == valueResult) {
      return map;
    } else if (map.isValueOptional()) {
      return Types.MapType.ofOptional(map.keyId(), map.valueId(), map.keyType(), valueResult);
    } else {
      return Types.MapType.ofRequired(map.keyId(), map.valueId(), map.keyType(), valueResult);
    }
  }

  /**
   * If select full types is disabled we need to recreate the struct with only the selected
   * subfields. If no subfields are selected we return an empty struct.
   *
   * @param projectedField subfields already selected in this projection
   * @return projected struct
   */
  private StructType projectSelectedStruct(Type projectedField) {
    Preconditions.checkArgument(projectedField == null || projectedField.isStructType());
    // the struct was selected, ensure at least an empty struct is returned
    if (projectedField == null) {
      // no sub-fields were selected but the struct was, return an empty struct
      return Types.StructType.of();
    } else {
      // sub-fields were selected so return the projected struct
      return projectedField.asStructType();
    }
  }
}
