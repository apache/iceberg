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
package org.apache.iceberg.hudi;

import java.util.List;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.Types;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public abstract class HudiDataTypeVisitor<T> {

  public static <T> T visit(Type type, HudiDataTypeVisitor<T> visitor) {
    if (type instanceof Types.RecordType) {
      List<Types.Field> fields = ((Types.RecordType) type).fields();
      List<T> fieldResults = Lists.newArrayListWithExpectedSize(fields.size());

      for (Types.Field field : fields) {
        fieldResults.add(visitor.field(field, visit(field.type(), visitor)));
      }

      return visitor.record((Types.RecordType) type, fieldResults);
    } else if (type instanceof Types.MapType) {
      return visitor.map(
          (Types.MapType) type,
          visit(((Types.MapType) type).keyType(), visitor),
          visit(((Types.MapType) type).valueType(), visitor));
    } else if (type instanceof Types.ArrayType) {
      return visitor.array(
          (Types.ArrayType) type, visit(((Types.ArrayType) type).elementType(), visitor));
    }
    return visitor.atomic(type);
  }

  public abstract T record(Types.RecordType record, List<T> fieldResults);

  public abstract T field(Types.Field field, T typeResult);

  public abstract T array(Types.ArrayType array, T elementResult);

  public abstract T map(Types.MapType map, T keyResult, T valueResult);

  public abstract T atomic(Type atomic);
}
