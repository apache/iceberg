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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.schema.SchemaWithPartnerVisitor;

public class RestoreColumns extends SchemaWithPartnerVisitor<Type, Type> {
  /**
   * Restores columns from a base schema in a projection that may not include them.
   *
   * @param base a base schema, from which the projection was produced
   * @param projection a projection of the base schema
   * @param fieldIds a set of field IDs to add to the projection if they are missing
   * @return an updated projection with the fields restored
   */
  public static Schema restore(Schema base, Schema projection, Set<Integer> fieldIds) {
    return SchemaWithPartnerVisitor.visit(
            base, projection.asStruct(), new RestoreColumns(fieldIds), FieldIdAccessors.get())
        .asStructType()
        .asSchema();
  }

  private final Set<Integer> restoredFields;

  private RestoreColumns(Set<Integer> fieldsToRestore) {
    this.restoredFields = fieldsToRestore;
  }

  @Override
  public Type schema(Schema schema, Type partner, Type structResult) {
    return structResult;
  }

  @Override
  public Type struct(Types.StructType struct, Type partner, List<Type> fieldResults) {
    List<Types.NestedField> fields = struct.fields();

    boolean hasFields = false;
    List<Types.NestedField> newFields = Lists.newArrayList();
    for (int i = 0; i < fields.size(); i += 1) {
      Type newType = fieldResults.get(i);
      if (newType != null) {
        hasFields = true;
        newFields.add(Types.NestedField.from(fields.get(i)).ofType(newType).build());
      }
    }

    if (hasFields) {
      return Types.StructType.of(newFields);
    }

    return partner;
  }

  @Override
  public Type field(Types.NestedField field, Type projection, Type fieldResult) {
    if (restoredFields.contains(field.fieldId())) {
      return field.type();
    }

    if (fieldResult != null) {
      return fieldResult;
    }

    return projection;
  }

  @Override
  public Type list(Types.ListType list, Type projection, Type elementResult) {
    if (restoredFields.contains(list.elementId())) {
      // replace the original element type with the original, by returning the original list
      return list;
    }

    if (elementResult != null) {
      if (list.isElementRequired()) {
        return Types.ListType.ofRequired(list.elementId(), elementResult);
      } else {
        return Types.ListType.ofOptional(list.elementId(), elementResult);
      }
    }

    return projection;
  }

  @Override
  public Type map(Types.MapType map, Type projection, Type keyResult, Type valueResult) {
    Type projectedKeyType = null;
    Type projectedValueType = null;
    if (projection != null) {
      Types.MapType partnerMap = projection.asMapType();
      projectedKeyType = partnerMap.keyType();
      projectedValueType = partnerMap.valueType();
    }

    Type keyType = field(map.field(map.keyId()), projectedKeyType, keyResult);
    Type valueType = field(map.field(map.valueId()), projectedValueType, valueResult);

    if (keyType != null || valueType != null) {
      keyType = keyType != null ? keyType : map.keyType();
      valueType = valueType != null ? valueType : map.valueType();
      if (map.isValueRequired()) {
        return Types.MapType.ofRequired(map.keyId(), map.valueId(), keyType, valueType);
      } else {
        return Types.MapType.ofOptional(map.keyId(), map.valueId(), keyType, valueType);
      }
    }

    return projection;
  }

  @Override
  public Type variant(Types.VariantType variant, Type projection) {
    return projection;
  }

  @Override
  public Type primitive(Type.PrimitiveType primitive, Type projection) {
    return projection;
  }

  private static class FieldIdAccessors implements SchemaWithPartnerVisitor.PartnerAccessors<Type> {
    private static final FieldIdAccessors INSTANCE = new FieldIdAccessors();

    public static FieldIdAccessors get() {
      return INSTANCE;
    }

    @Override
    public Type fieldPartner(Type partnerStruct, int fieldId, String name) {
      Types.NestedField partnerField = partnerStruct.asStructType().field(fieldId);
      return partnerField != null ? partnerField.type() : null;
    }

    @Override
    public Type mapKeyPartner(Type partnerMap) {
      return partnerMap.asMapType().keyType();
    }

    @Override
    public Type mapValuePartner(Type partnerMap) {
      return partnerMap.asMapType().valueType();
    }

    @Override
    public Type listElementPartner(Type partnerList) {
      return partnerList.asListType().elementType();
    }
  }
}
