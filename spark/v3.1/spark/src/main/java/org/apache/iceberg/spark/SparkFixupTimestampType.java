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
import org.apache.iceberg.types.Types;

/**
 * By default Spark type {@link org.apache.iceberg.types.Types.TimestampType} should be converted to
 * {@link Types.TimestampType#withZone()} iceberg type. But we also can convert {@link
 * org.apache.iceberg.types.Types.TimestampType} to {@link Types.TimestampType#withoutZone()}
 * iceberg type by setting {@link SparkSQLProperties#USE_TIMESTAMP_WITHOUT_TIME_ZONE_IN_NEW_TABLES}
 * to 'true'
 */
class SparkFixupTimestampType extends FixupTypes {

  private SparkFixupTimestampType(Schema referenceSchema) {
    super(referenceSchema);
  }

  static Schema fixup(Schema schema) {
    return new Schema(
        TypeUtil.visit(schema, new SparkFixupTimestampType(schema)).asStructType().fields());
  }

  @Override
  public Type primitive(Type.PrimitiveType primitive) {
    if (primitive.typeId() == Type.TypeID.TIMESTAMP) {
      return Types.TimestampType.withoutZone();
    }
    return primitive;
  }

  @Override
  protected boolean fixupPrimitive(Type.PrimitiveType type, Type source) {
    return Type.TypeID.TIMESTAMP.equals(type.typeId());
  }
}
