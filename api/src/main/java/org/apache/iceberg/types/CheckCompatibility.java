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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.iceberg.Schema;

public class CheckCompatibility extends TypeUtil.CustomOrderSchemaVisitor<List<String>> {
  /**
   * Returns a list of compatibility errors for writing with the given write schema.
   *
   * @param readSchema a read schema
   * @param writeSchema a write schema
   * @return a list of error details, or an empty list if there are no compatibility problems
   */
  public static List<String> writeCompatibilityErrors(Schema readSchema, Schema writeSchema) {
    return TypeUtil.visit(readSchema, new CheckCompatibility(writeSchema, true));
  }

  /**
   * Returns a list of compatibility errors for reading with the given read schema.
   *
   * @param readSchema a read schema
   * @param writeSchema a write schema
   * @return a list of error details, or an empty list if there are no compatibility problems
   */
  public static List<String> readCompatibilityErrors(Schema readSchema, Schema writeSchema) {
    return TypeUtil.visit(readSchema, new CheckCompatibility(writeSchema, false));
  }

  private static final List<String> NO_ERRORS = ImmutableList.of();

  private final Schema schema;
  private final boolean checkOrdering;

  // the current file schema, maintained while traversing a write schema
  private Type currentType;

  private CheckCompatibility(Schema schema, boolean checkOrdering) {
    this.schema = schema;
    this.checkOrdering = checkOrdering;
  }

  @Override
  public List<String> schema(Schema readSchema, Supplier<List<String>> structErrors) {
    this.currentType = this.schema.asStruct();
    try {
      return structErrors.get();
    } finally {
      this.currentType = null;
    }
  }

  @Override
  public List<String> struct(Types.StructType readStruct, Iterable<List<String>> fieldErrorLists) {
    Preconditions.checkNotNull(readStruct, "Evaluation must start with a schema.");

    if (!currentType.isStructType()) {
      return ImmutableList.of(String.format(": %s cannot be read as a struct", currentType));
    }

    List<String> errors = Lists.newArrayList();

    for (List<String> fieldErrors : fieldErrorLists) {
      errors.addAll(fieldErrors);
    }

    // detect reordered fields
    if (checkOrdering) {
      Types.StructType struct = currentType.asStructType();
      List<Types.NestedField> fields = struct.fields();
      Map<Integer, Integer> idToOrdinal = Maps.newHashMap();
      for (int i = 0; i < fields.size(); i += 1) {
        idToOrdinal.put(fields.get(i).fieldId(), i);
      }

      int lastOrdinal = -1;
      for (Types.NestedField readField : readStruct.fields()) {
        int id = readField.fieldId();
        Types.NestedField field = struct.field(id);
        if (field != null) {
          int ordinal = idToOrdinal.get(id);
          if (lastOrdinal >= ordinal) {
            errors.add(
                readField.name() + " is out of order, before " + fields.get(lastOrdinal).name());
          }
          lastOrdinal = ordinal;
        }
      }
    }

    return errors;
  }

  @Override
  public List<String> field(Types.NestedField readField, Supplier<List<String>> fieldErrors) {
    Types.StructType struct = currentType.asStructType();
    Types.NestedField field = struct.field(readField.fieldId());
    List<String> errors = Lists.newArrayList();

    if (field == null) {
      if (readField.isRequired()) {
        return ImmutableList.of(readField.name() + " is required, but is missing");
      }
      // if the field is optional, it will be read as nulls
      return NO_ERRORS;
    }

    this.currentType = field.type();
    try {
      if (readField.isRequired() && field.isOptional()) {
        errors.add(readField.name() + " should be required, but is optional");
      }

      for (String error : fieldErrors.get()) {
        if (error.startsWith(":")) {
          // this is the last field name before the error message
          errors.add(readField.name() + error);
        } else {
          // this has a nested field, add '.' for nesting
          errors.add(readField.name() + "." + error);
        }
      }

      return errors;

    } finally {
      this.currentType = struct;
    }
  }

  @Override
  public List<String> list(Types.ListType readList, Supplier<List<String>> elementErrors) {
    if (!currentType.isListType()) {
      return ImmutableList.of(String.format(": %s cannot be read as a list", currentType));
    }

    Types.ListType list = currentType.asNestedType().asListType();
    List<String> errors = Lists.newArrayList();

    this.currentType = list.elementType();
    try {
      if (readList.isElementRequired() && list.isElementOptional()) {
        errors.add(": elements should be required, but are optional");
      }

      errors.addAll(elementErrors.get());

      return errors;

    } finally {
      this.currentType = list;
    }
  }

  @Override
  public List<String> map(Types.MapType readMap, Supplier<List<String>> keyErrors, Supplier<List<String>> valueErrors) {
    if (!currentType.isMapType()) {
      return ImmutableList.of(String.format(": %s cannot be read as a map", currentType));
    }

    Types.MapType map = currentType.asNestedType().asMapType();
    List<String> errors = Lists.newArrayList();

    try {
      if (readMap.isValueRequired() && map.isValueOptional()) {
        errors.add(": values should be required, but are optional");
      }

      this.currentType = map.keyType();
      errors.addAll(keyErrors.get());

      this.currentType = map.valueType();
      errors.addAll(valueErrors.get());

      return errors;

    } finally {
      this.currentType = map;
    }
  }

  @Override
  public List<String> primitive(Type.PrimitiveType readPrimitive) {
    if (currentType.equals(readPrimitive)) {
      return NO_ERRORS;
    }

    if (!currentType.isPrimitiveType()) {
      return ImmutableList.of(String.format(": %s cannot be read as a %s",
          currentType.typeId().toString().toLowerCase(Locale.ENGLISH), readPrimitive));
    }

    if (!TypeUtil.isPromotionAllowed(currentType.asPrimitiveType(), readPrimitive)) {
      return ImmutableList.of(String.format(": %s cannot be promoted to %s",
          currentType, readPrimitive));
    }

    // both are primitives and promotion is allowed to the read type
    return NO_ERRORS;
  }
}
