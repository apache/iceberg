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
package org.apache.iceberg.lance.spark;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/** Converts Iceberg types to Spark DataTypes. Minimal subset needed for ConstantColumnVector. */
class IcebergTypeToSparkType {

  private IcebergTypeToSparkType() {}

  static DataType convert(Type type) {
    switch (type.typeId()) {
      case BOOLEAN:
        return DataTypes.BooleanType;
      case INTEGER:
        return DataTypes.IntegerType;
      case LONG:
        return DataTypes.LongType;
      case FLOAT:
        return DataTypes.FloatType;
      case DOUBLE:
        return DataTypes.DoubleType;
      case DATE:
        return DataTypes.DateType;
      case TIME:
        return DataTypes.LongType;
      case TIMESTAMP:
        return DataTypes.TimestampType;
      case STRING:
        return DataTypes.StringType;
      case BINARY:
        return DataTypes.BinaryType;
      case UUID:
        return DataTypes.BinaryType;
      case FIXED:
        return DataTypes.BinaryType;
      case DECIMAL:
        Types.DecimalType dec = (Types.DecimalType) type;
        return DataTypes.createDecimalType(dec.precision(), dec.scale());
      default:
        throw new UnsupportedOperationException("Unsupported Iceberg type for Spark: " + type);
    }
  }
}
