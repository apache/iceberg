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

import dev.vortex.api.DType;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public abstract class VortexSchemaWithTypeVisitor<T> {
  public abstract T struct(
      Types.StructType iStruct, List<DType> types, List<String> names, List<T> fields);

  public abstract T list(Types.ListType iList, DType array, T element);

  public abstract T primitive(Type.PrimitiveType iPrimitive, DType primitive);

  // What is the point of this??
  public static <T> T visit(
      Schema expectedSchema, DType readVortexSchema, VortexSchemaWithTypeVisitor<T> visitor) {
    return visit(expectedSchema.asStruct(), readVortexSchema, visitor);
  }

  public static <T> T visit(Type iType, DType schema, VortexSchemaWithTypeVisitor<T> visitor) {
    switch (schema.getVariant()) {
      case STRUCT:
        return visitStruct(iType != null ? iType.asStructType() : null, schema, visitor);

      case LIST:
        Types.ListType list = iType != null ? iType.asListType() : null;
        return visitor.list(
            list,
            schema,
            visit(list != null ? list.elementType() : null, schema.getElementType(), visitor));
      default:
        return visitor.primitive(iType != null ? iType.asPrimitiveType() : null, schema);
    }
  }

  private static <T> T visitStruct(
      Types.StructType struct, DType record, VortexSchemaWithTypeVisitor<T> visitor) {
    List<DType> fields = record.getFieldTypes();
    List<String> names = record.getFieldNames();

    List<T> results = Lists.newArrayListWithExpectedSize(fields.size());
    // TODO(aduffy): metadata in Vortex schemas to allow embedding the Iceberg field ID number?
    //  For now we just use the field index, which might not be right when we have projections...
    for (int fieldId = 0; fieldId < fields.size(); fieldId++) {
      DType field = fields.get(fieldId);
      Types.NestedField iField = struct != null ? struct.field(fieldId) : null;
      results.add(visit(iField != null ? iField.type() : null, field, visitor));
    }
    return visitor.struct(struct, fields, names, results);
  }
}
