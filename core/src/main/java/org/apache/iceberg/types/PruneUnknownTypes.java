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
import org.apache.hadoop.util.Lists;
import org.apache.iceberg.Schema;

/** This is used to remove UnknownTypes that should not be persistent in files */
public class PruneUnknownTypes extends TypeUtil.SchemaVisitor<Type> {
  private static final PruneUnknownTypes INSTANCE = new PruneUnknownTypes();

  /**
   * Visits a schema and removes the UnknownTypes.
   *
   * <p>Used when creating a write schema, and skip over the UnknownTypes, since they are not
   * materialized in the data files
   */
  private PruneUnknownTypes() {}

  /**
   * Prunes any UnknownType from a Schema
   *
   * @param schema a Schema
   */
  public static Schema prune(Schema schema) {
    Types.StructType struct = (Types.StructType) TypeUtil.visit(schema, INSTANCE);
    return new Schema(struct.fields(), schema.identifierFieldIds());
  }

  public static Types.StructType prune(Types.StructType structType) {
    Object obj = TypeUtil.visit(structType, INSTANCE);

    if (obj instanceof Types.StructType) {
      return (Types.StructType) obj;
    } else {
      return ((Schema) obj).asStruct();
    }
  }

  @Override
  public Type schema(Schema schema, Type structResult) {
    if (schema == null) {
      return null;
    }

    if (structResult.typeId().equals(Type.TypeID.UNKNOWN)) {
      return Types.StructType.of();
    } else {
      return structResult;
    }
  }

  private boolean needsRewrite(Types.NestedField field, Type type) {
    return field.type().typeId().equals(Type.TypeID.UNKNOWN)
        || type.typeId().equals(Type.TypeID.UNKNOWN)
        || !field.type().equals(type);
  }

  @Override
  public Type struct(Types.StructType struct, List<Type> fieldResults) {
    List<Types.NestedField> fields = struct.fields();
    List<Types.NestedField> newFields = Lists.newArrayList();

    int pos = 0;
    boolean rewritten = false;
    for (Types.NestedField field : fields) {
      Type fieldResult = fieldResults.get(pos);

      if (needsRewrite(field, fieldResult)) {
        newFields.add(Types.NestedField.from(field).ofType(fieldResult).build());
        rewritten = true;
      } else {
        newFields.add(field);
      }

      pos += 1;
    }

    if (rewritten) {
      return Types.StructType.of(newFields);
    } else {
      return struct;
    }
  }

  @Override
  public Type field(Types.NestedField field, Type fieldResult) {
    return fieldResult;
  }

  @Override
  public Type list(Types.ListType list, Type elementResult) {
    if (!list.elementType().equals(elementResult)) {
      if (list.isElementOptional()) {
        return Types.ListType.ofOptional(list.elementId(), elementResult);
      } else {
        return Types.ListType.ofRequired(list.elementId(), elementResult);
      }
    } else {
      return list;
    }
  }

  @Override
  public Type map(Types.MapType map, Type keyType, Type valueResult) {
    if (!map.valueType().equals(valueResult)) {
      if (map.isValueOptional()) {
        return Types.MapType.ofOptional(map.keyId(), map.valueId(), keyType, valueResult);
      } else {
        return Types.MapType.ofRequired(map.keyId(), map.valueId(), keyType, valueResult);
      }
    } else {
      return map;
    }
  }

  @Override
  public Type variant(Types.VariantType variant) {
    return variant;
  }

  @Override
  public Type primitive(Type.PrimitiveType primitive) {
    return primitive;
  }
}
