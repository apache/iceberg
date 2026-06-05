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
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
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

  public abstract T primitive(Type.PrimitiveType iPrimitive, Field primField);

  public static <T> T visit(
      Schema expectedSchema,
      org.apache.arrow.vector.types.pojo.Schema fileSchema,
      VortexSchemaWithTypeVisitor<T> visitor) {
    return visitStruct(expectedSchema.asStruct(), fileSchema.getFields(), visitor);
  }

  public static <T> T visit(Type iType, Field field, VortexSchemaWithTypeVisitor<T> visitor) {
    ArrowType arrowType = field.getType();
    if (arrowType instanceof ArrowType.Struct) {
      return visitStruct(iType != null ? iType.asStructType() : null, field.getChildren(), visitor);
    } else if (arrowType instanceof ArrowType.List
        || arrowType instanceof ArrowType.LargeList
        || arrowType instanceof ArrowType.FixedSizeList) {
      Types.ListType list = iType != null ? iType.asListType() : null;
      Field element = field.getChildren().get(0);
      return visitor.list(
          list, field, visit(list != null ? list.elementType() : null, element, visitor));
    } else {
      return visitor.primitive(iType != null ? iType.asPrimitiveType() : null, field);
    }
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

    // Arrow/Vortex schemas carry no Iceberg field ids, so expected struct fields are bound to file
    // columns by name (the top-level reader resolves columns the same way). Driving the walk from
    // the expected fields lets a projection reorder, drop, or add struct fields relative to the
    // physical file layout. The returned fields/children are aligned to the expected fields, with a
    // null entry wherever the file does not contain the expected field.
    Map<String, Field> fileFieldsByName =
        fields.stream().collect(Collectors.toUnmodifiableMap(Field::getName, Function.identity()));

    List<Types.NestedField> expectedFields = struct.fields();
    List<Field> matchedFields = Lists.newArrayListWithExpectedSize(expectedFields.size());
    List<T> results = Lists.newArrayListWithExpectedSize(expectedFields.size());
    for (Types.NestedField expectedField : expectedFields) {
      Field fileField = fileFieldsByName.get(expectedField.name());
      matchedFields.add(fileField);
      results.add(fileField == null ? null : visit(expectedField.type(), fileField, visitor));
    }

    return visitor.struct(struct, matchedFields, results);
  }
}
