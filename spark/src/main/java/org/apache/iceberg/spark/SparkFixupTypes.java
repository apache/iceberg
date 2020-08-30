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

package org.apache.iceberg.spark;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.FixupTypes;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;

/**
 * Some types, like binary and fixed, are converted to the same Spark type. Conversion back
 * can produce only one, which may not be correct.
 */
class SparkFixupTypes extends FixupTypes {

  private SparkFixupTypes(Schema referenceSchema) {
    super(referenceSchema);
  }

  static Schema fixup(Schema schema, Schema referenceSchema) {
    return new Schema(TypeUtil.visit(schema,
        new SparkFixupTypes(referenceSchema)).asStructType().fields());
  }

  @Override
  protected boolean fixupPrimitive(Type.PrimitiveType type, Type source) {
    switch (type.typeId()) {
      case STRING:
        if (source.typeId() == Type.TypeID.UUID) {
          return true;
        }
        break;
      case BINARY:
        if (source.typeId() == Type.TypeID.FIXED) {
          return true;
        }
        break;
      default:
    }
    return false;
  }
}
