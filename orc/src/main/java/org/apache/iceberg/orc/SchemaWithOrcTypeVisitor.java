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
package org.apache.iceberg.orc;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;

public abstract class SchemaWithOrcTypeVisitor<T> {

  // TODO: Convert order here
  public static <T> T visit(
      Type iType, TypeDescription schema, SchemaWithOrcTypeVisitor<T> visitor) {

    if (iType.isStructType()) {
      String structType = schema.getAttributeValue(ORCSchemaUtil.ICEBERG_STRUCT_TYPE_ATTRIBUTE);
      if (ORCSchemaUtil.VARIANT.equalsIgnoreCase(structType)) {
        return visitVariant(iType.asVariantType(), schema, visitor);
      } else {
        return visitRecord(iType.asStructType(), schema, visitor);
      }

    } else if (iType.isListType()) {
      Types.ListType list = iType.asListType();
      return visitor.list(
          list,
          schema,
          visit(list != null ? list.elementType() : null, schema.getChildren().get(0), visitor));

    } else if (iType.isMapType()) {
      Types.MapType map = iType.asMapType();
      return visitor.map(
          map,
          schema,
          visit(map != null ? map.keyType() : null, schema.getChildren().get(0), visitor),
          visit(map != null ? map.valueType() : null, schema.getChildren().get(1), visitor));

    } else {
      return visitor.primitive(iType.asPrimitiveType(), schema);
    }
  }

  private static <T> T visitRecord(
      Types.StructType struct, TypeDescription record, SchemaWithOrcTypeVisitor<T> visitor) {
    Map<Integer, TypeDescription> idToField =
        record.getChildren().stream().collect(Collectors.toMap(ORCSchemaUtil::fieldId, s -> s));
    List<T> results = Lists.newArrayListWithExpectedSize(struct.fields().size());

    for (Types.NestedField iField : struct.fields()) {
      TypeDescription orcField = idToField.get(iField.fieldId());

      if (iField.type().typeId() != Type.TypeID.UNKNOWN) {
          results.add(visit(iField.type(), orcField, visitor));
      }
    }
    List<String> names =
        struct.fields().stream().map(Types.NestedField::name).collect(Collectors.toList());

    return visitor.record(struct, record, names, results);
  }

  private static <T> T visitVariant(
      Types.VariantType iVariant, TypeDescription variant, SchemaWithOrcTypeVisitor<T> visitor) {
    List<String> names = variant.getFieldNames();
    Preconditions.checkArgument(
        names.size() == 2
            && ORCSchemaUtil.VARIANT_METADATA.equals(names.get(0))
            && ORCSchemaUtil.VARIANT_VALUE.equals(names.get(1)),
        "Invalid variant metadata fields: %s",
        String.join(", ", names));

    List<TypeDescription> children = variant.getChildren();
    T metadataResult = visit((Type) null, children.get(0), visitor);
    T valueResult = visit((Type) null, children.get(1), visitor);

    return visitor.variant(iVariant, variant, metadataResult, valueResult);
  }

  public T record(
      Types.StructType iStruct, TypeDescription record, List<String> names, List<T> fields) {
    return null;
  }

  public T list(Types.ListType iList, TypeDescription array, T element) {
    return null;
  }

  public T map(Types.MapType iMap, TypeDescription map, T key, T value) {
    return null;
  }

  public T variant(Types.VariantType iVariant, TypeDescription variant, T metadata, T value) {
    throw new UnsupportedOperationException("Variant is not supported");
  }

  public T primitive(Type.PrimitiveType iPrimitive, TypeDescription primitive) {
    return null;
  }
}
