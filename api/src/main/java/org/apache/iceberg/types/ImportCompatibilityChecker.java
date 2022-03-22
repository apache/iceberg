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
import java.util.Locale;
import java.util.function.Supplier;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class ImportCompatibilityChecker extends CheckCompatibility {

  public ImportCompatibilityChecker(Schema schema, boolean checkOrdering, boolean checkNullability) {
    super(schema, checkOrdering, checkNullability);
  }

  @Override
  public List<String> field(Types.NestedField readField, Supplier<List<String>> fieldErrors) {
    Types.StructType struct = currentType().asStructType();
    Types.NestedField field = struct.field(readField.fieldId());
    List<String> errors = Lists.newArrayList();

    if (field == null) {
      return NO_ERRORS;
    }
    setCurrentType(field.type());
    try {
      if (checkNullability() && field.isRequired() && readField.isOptional()) {
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
      return ImmutableList.copyOf(errors);
    } finally {
      setCurrentType(struct);
    }
  }

  @Override
  public List<String> list(Types.ListType readList, Supplier<List<String>> elementErrors) {
    if (!currentType().isListType()) {
      return ImmutableList.of(String.format(": %s cannot be read as a list", currentType()));
    }

    Types.ListType list = currentType().asNestedType().asListType();
    List<String> errors = Lists.newArrayList();

    setCurrentType(list.elementType());
    try {
      if (list.isElementRequired() && readList.isElementOptional()) {
        errors.add(": elements should be required, but are optional");
      }

      errors.addAll(elementErrors.get());

      return ImmutableList.copyOf(errors);
    } finally {
      setCurrentType(list);
    }
  }

  @Override
  public List<String> map(
      Types.MapType readMap, Supplier<List<String>> keyErrors, Supplier<List<String>> valueErrors) {
    if (!currentType().isMapType()) {
      return ImmutableList.of(String.format(": %s cannot be read as a map", currentType()));
    }

    Types.MapType map = currentType().asNestedType().asMapType();
    List<String> errors = Lists.newArrayList();

    try {
      if (map.isValueRequired() && readMap.isValueOptional()) {
        errors.add(": values should be required, but are optional");
      }

      this.setCurrentType(map.keyType());
      errors.addAll(keyErrors.get());

      this.setCurrentType(map.valueType());
      errors.addAll(valueErrors.get());

      return ImmutableList.copyOf(errors);
    } finally {
      this.setCurrentType(map);
    }
  }


  @Override
  public List<String> primitive(Type.PrimitiveType readPrimitive) {
    if (currentType().equals(readPrimitive)) {
      return NO_ERRORS;
    }

    if (!currentType().isPrimitiveType()) {
      return ImmutableList.of(String.format(": %s cannot be read as a %s",
          currentType().typeId().toString().toLowerCase(Locale.ENGLISH), readPrimitive));
    }

    if (!TypeUtil.isPromotionAllowed(readPrimitive, currentType().asPrimitiveType())) {
      return ImmutableList.of(String.format(": %s cannot be promoted to %s",
          currentType(), readPrimitive));
    }

    // both are primitives and promotion is allowed to the read type
    return NO_ERRORS;
  }

  /**
   * Returns a list of compatibility errors for writing with the given write schema.
   * This includes nullability: writing optional (nullable) values to a required field is an error
   * Optionally this method allows case where input schema has different ordering than table schema.
   * @param tableSchema a read schema
   * @param importSchema a write schema
   * @param checkOrdering If false, allow input schema to have different ordering than table schema
   * @return a list of error details, or an empty list if there are no compatibility problems
   */
  public static List<String> importCompatibilityErrors(Schema tableSchema, Schema importSchema,
                                                       boolean checkOrdering) {
    return TypeUtil.visit(importSchema, new ImportCompatibilityChecker(tableSchema, checkOrdering, true));
  }
}
