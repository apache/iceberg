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
package org.apache.iceberg.flink;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.FixupTypes;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

/**
 * The uuid and fixed are converted to the same Flink type. Conversion back can produce only one,
 * which may not be correct.
 */
class FlinkFixupTypes extends FixupTypes {

  private FlinkFixupTypes(Schema referenceSchema) {
    super(referenceSchema);
  }

  static Schema fixup(Schema schema, Schema referenceSchema) {
    return new Schema(
        TypeUtil.visit(schema, new FlinkFixupTypes(referenceSchema)).asStructType().fields());
  }

  @Override
  protected boolean fixupPrimitive(Type.PrimitiveType type, Type source) {
    if (type instanceof Types.FixedType) {
      int length = ((Types.FixedType) type).length();
      return source.typeId() == Type.TypeID.UUID && length == 16;
    }
    return false;
  }
}
