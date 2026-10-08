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
package org.apache.iceberg;

import java.util.List;
import java.util.function.Supplier;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

/**
 * Utility methods for working with fields in schema definition order.
 *
 * <p>Schema order is a pre-order traversal of fields in the schema. This produces fields in the
 * same left-to-right order as a type definition. For example, the schema <code>
 * id bigint, data string, point struct&lt;x float, y float&gt;</code> produces fields in schema
 * order: [ id, data, point, point.x, point.y ]
 */
class SchemaOrder {
  private SchemaOrder() {}

  /**
   * Produces a list of all field IDs in schema order.
   *
   * @param schema a schema
   * @return all field IDs in schema order
   */
  public static List<Integer> allFieldIds(Schema schema) {
    return TypeUtil.visit(schema, new IdVisitor(true));
  }

  /**
   * Produces a list of leaf field IDs in schema order.
   *
   * @param schema a schema
   * @return leaf field IDs in schema order
   */
  public static List<Integer> leafFieldIds(Schema schema) {
    return TypeUtil.visit(schema, new IdVisitor(false));
  }

  private static class IdVisitor extends TypeUtil.CustomOrderSchemaVisitor<List<Integer>> {
    private final List<Integer> orderedIds = Lists.newArrayList();
    private final boolean includeNonLeafIds;

    private IdVisitor(boolean includeNonLeafIds) {
      this.includeNonLeafIds = includeNonLeafIds;
    }

    @Override
    @SuppressWarnings("ReturnValueIgnored")
    public List<Integer> schema(Schema schema, Supplier<List<Integer>> structResult) {
      structResult.get(); // visit the top-level struct
      return orderedIds;
    }

    @Override
    public List<Integer> struct(Types.StructType struct, Iterable<List<Integer>> fieldResults) {
      for (List<Integer> ignored : fieldResults) {
        // no handling is required; iterating visits the struct's fields
      }

      return null;
    }

    @Override
    @SuppressWarnings("ReturnValueIgnored")
    public List<Integer> field(Types.NestedField field, Supplier<List<Integer>> fieldResult) {
      if (field.type().isNestedType()) {
        if (includeNonLeafIds) {
          orderedIds.add(field.fieldId());
        }

        fieldResult.get(); // visit the field
      } else {
        orderedIds.add(field.fieldId());
      }

      return null;
    }

    @Override
    public List<Integer> list(Types.ListType list, Supplier<List<Integer>> elementResult) {
      field(list.fields().get(0), elementResult);
      return null;
    }

    @Override
    public List<Integer> map(
        Types.MapType map, Supplier<List<Integer>> keyResult, Supplier<List<Integer>> valueResult) {
      field(map.fields().get(0), keyResult);
      field(map.fields().get(1), valueResult);
      return null;
    }

    @Override
    public List<Integer> variant(Types.VariantType variant) {
      return null;
    }

    @Override
    public List<Integer> primitive(Type.PrimitiveType primitive) {
      return null;
    }
  }
}
