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
package org.apache.iceberg.schema;

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public abstract class SchemaWithPartnerVisitor<P, R> {

  public interface PartnerAccessors<P> {

    P fieldPartner(P partnerStruct, int fieldId, String name);

    P mapKeyPartner(P partnerMap);

    P mapValuePartner(P partnerMap);

    P listElementPartner(P partnerList);
  }

  public static <P, T> T visit(
      Schema schema,
      P partner,
      SchemaWithPartnerVisitor<P, T> visitor,
      PartnerAccessors<P> accessors) {
    return visitor.schema(schema, partner, visit(schema.asStruct(), partner, visitor, accessors));
  }

  public static <P, T> T visit(
      Type type, P partner, SchemaWithPartnerVisitor<P, T> visitor, PartnerAccessors<P> accessors) {
    switch (type.typeId()) {
      case STRUCT:
        Types.StructType struct = type.asNestedType().asStructType();
        List<T> results = Lists.newArrayListWithExpectedSize(struct.fields().size());
        for (Types.NestedField field : struct.fields()) {
          P fieldPartner =
              partner != null
                  ? accessors.fieldPartner(partner, field.fieldId(), field.name())
                  : null;
          visitor.beforeField(field, fieldPartner);
          T result;
          try {
            result = visit(field.type(), fieldPartner, visitor, accessors);
          } finally {
            visitor.afterField(field, fieldPartner);
          }
          results.add(visitor.field(field, fieldPartner, result));
        }
        return visitor.struct(struct, partner, results);

      case LIST:
        Types.ListType list = type.asNestedType().asListType();
        T elementResult;

        Types.NestedField elementField = list.field(list.elementId());
        P partnerElement = partner != null ? accessors.listElementPartner(partner) : null;
        visitor.beforeListElement(elementField, partnerElement);
        try {
          elementResult = visit(list.elementType(), partnerElement, visitor, accessors);
        } finally {
          visitor.afterListElement(elementField, partnerElement);
        }

        return visitor.list(list, partner, elementResult);

      case MAP:
        Types.MapType map = type.asNestedType().asMapType();
        T keyResult;
        T valueResult;

        Types.NestedField keyField = map.field(map.keyId());
        P keyPartner = partner != null ? accessors.mapKeyPartner(partner) : null;
        visitor.beforeMapKey(keyField, keyPartner);
        try {
          keyResult = visit(map.keyType(), keyPartner, visitor, accessors);
        } finally {
          visitor.afterMapKey(keyField, keyPartner);
        }

        Types.NestedField valueField = map.field(map.valueId());
        P valuePartner = partner != null ? accessors.mapValuePartner(partner) : null;
        visitor.beforeMapValue(valueField, valuePartner);
        try {
          valueResult = visit(map.valueType(), valuePartner, visitor, accessors);
        } finally {
          visitor.afterMapValue(valueField, valuePartner);
        }

        return visitor.map(map, partner, keyResult, valueResult);

      default:
        return visitor.primitive(type.asPrimitiveType(), partner);
    }
  }

  public void beforeField(Types.NestedField field, P partnerField) {}

  public void afterField(Types.NestedField field, P partnerField) {}

  public void beforeListElement(Types.NestedField elementField, P partnerField) {
    beforeField(elementField, partnerField);
  }

  public void afterListElement(Types.NestedField elementField, P partnerField) {
    afterField(elementField, partnerField);
  }

  public void beforeMapKey(Types.NestedField keyField, P partnerField) {
    beforeField(keyField, partnerField);
  }

  public void afterMapKey(Types.NestedField keyField, P partnerField) {
    afterField(keyField, partnerField);
  }

  public void beforeMapValue(Types.NestedField valueField, P partnerField) {
    beforeField(valueField, partnerField);
  }

  public void afterMapValue(Types.NestedField valueField, P partnerField) {
    afterField(valueField, partnerField);
  }

  public R schema(Schema schema, P partner, R structResult) {
    return null;
  }

  public R struct(Types.StructType struct, P partner, List<R> fieldResults) {
    return null;
  }

  public R field(Types.NestedField field, P partner, R fieldResult) {
    return null;
  }

  public R list(Types.ListType list, P partner, R elementResult) {
    return null;
  }

  public R map(Types.MapType map, P partner, R keyResult, R valueResult) {
    return null;
  }

  public R primitive(Type.PrimitiveType primitive, P partner) {
    return null;
  }
}
