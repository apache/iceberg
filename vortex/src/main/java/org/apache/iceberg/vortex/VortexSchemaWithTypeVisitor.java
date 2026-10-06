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
package org.apache.iceberg.vortex;

import java.util.List;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Walks a file's Arrow {@link Field} schema in parallel with the expected Iceberg {@link Type} so
 * that visitors can build readers that bind a target Iceberg shape to the file's columns.
 */
public abstract class VortexSchemaWithTypeVisitor<T> {
  public abstract T struct(Types.StructType iStruct, List<Field> fields, List<T> children);

  public abstract T list(Types.ListType iList, Field listField, T element);

  public abstract T map(Types.MapType iMap, Field mapField, T key, T value);

  public abstract T primitive(Type.PrimitiveType iPrimitive, Field primField);

  public abstract T variant(Types.VariantType variantType, Field variantField);

  public static <T> T visit(
      Schema expectedSchema,
      org.apache.arrow.vector.types.pojo.Schema fileSchema,
      VortexSchemaWithTypeVisitor<T> visitor) {
    return visitStruct(expectedSchema.asStruct(), fileSchema.getFields(), visitor);
  }

  public static <T> T visit(Type iType, Field field, VortexSchemaWithTypeVisitor<T> visitor) {
    if (isVariant(iType, field)) {
      return visitor.variant(iType != null ? iType.asVariantType() : null, field);
    }

    ArrowType arrowType = field.getType();
    if (arrowType instanceof ArrowType.Struct) {
      return visitStruct(iType != null ? iType.asStructType() : null, field.getChildren(), visitor);
    } else if (arrowType instanceof ArrowType.List
        || arrowType instanceof ArrowType.LargeList
        || arrowType instanceof ArrowType.FixedSizeList) {
      return visitList(iType != null ? iType.asListType() : null, field, visitor);
    } else if (arrowType instanceof ArrowType.Map) {
      return visitMap(iType != null ? iType.asMapType() : null, field, visitor);
    } else {
      return visitor.primitive(iType != null ? iType.asPrimitiveType() : null, field);
    }
  }

  private static <T> T visitList(
      Types.ListType list, Field listField, VortexSchemaWithTypeVisitor<T> visitor) {
    Field element = listField.getChildren().get(0);
    return visitor.list(
        list, listField, visit(list != null ? list.elementType() : null, element, visitor));
  }

  /** Arrow maps nest their key and value under a single non-nullable {@code entries} struct. */
  private static <T> T visitMap(
      Types.MapType map, Field mapField, VortexSchemaWithTypeVisitor<T> visitor) {
    List<Field> entries = mapField.getChildren().get(0).getChildren();
    return visitor.map(
        map,
        mapField,
        visit(map != null ? map.keyType() : null, entries.get(0), visitor),
        visit(map != null ? map.valueType() : null, entries.get(1), visitor));
  }

  private static boolean isVariant(Type iType, Field field) {
    return (iType != null && iType.isVariantType()) || VortexSchemas.isVariantField(field);
  }

  private static <T> T visitStruct(
      Types.StructType struct, List<Field> fields, VortexSchemaWithTypeVisitor<T> visitor) {
    if (struct == null) {
      // No expected Iceberg type to bind to (a file-only column). Walk children positionally; the
      // resulting reader is discarded by callers that pass a null target type.
      List<T> results = Lists.newArrayListWithExpectedSize(fields.size());
      for (Field field : fields) {
        results.add(visit(null, field, visitor));
      }
      return visitor.struct(null, fields, results);
    }

    // Expected struct fields are bound to file columns by Iceberg id when the file carries them
    // (VortexIterable tags the Arrow schema from the file's stored Iceberg schema) and by name
    // otherwise; the top-level reader resolves columns the same way. Driving the walk from the
    // expected fields lets a projection reorder, drop, or add struct fields relative to the
    // physical file layout. The returned fields/children are aligned to the expected fields, with a
    // null entry wherever the file does not contain the expected field.
    VortexSchemas.FieldBinding binding = VortexSchemas.FieldBinding.of(fields);

    List<Types.NestedField> expectedFields = struct.fields();
    List<Field> matchedFields = Lists.newArrayListWithExpectedSize(expectedFields.size());
    List<T> results = Lists.newArrayListWithExpectedSize(expectedFields.size());
    for (Types.NestedField expectedField : expectedFields) {
      Field fileField = binding.resolve(expectedField);
      matchedFields.add(fileField);
      results.add(fileField == null ? null : visit(expectedField.type(), fileField, visitor));
    }

    return visitor.struct(struct, matchedFields, results);
  }
}
