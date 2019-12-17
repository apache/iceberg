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

package org.apache.iceberg.spark.arrow;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ArrowUtils {

  private static ArrowUtils instance;
  private RootAllocator rootAllocator;

  private ArrowUtils() {
    rootAllocator = new RootAllocator(Long.MAX_VALUE);
  }

  public static ArrowUtils instance() {
    if (instance == null) {
      instance = new ArrowUtils();
    }
    return instance;
  }

  public RootAllocator rootAllocator() {
    return rootAllocator;
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public DataType fromArrowType(ArrowType data) {

    if (data instanceof ArrowType.Bool) {
      return DataTypes.BooleanType;
    } else if (data instanceof ArrowType.Int) {
      ArrowType.Int intData = (ArrowType.Int) data;
      if (intData.getIsSigned() && intData.getBitWidth() == 8) {
        return DataTypes.ByteType;
      } else if (intData.getIsSigned() && intData.getBitWidth() == 8 * 2) {
        return DataTypes.ShortType;
      } else if (intData.getIsSigned() && intData.getBitWidth() == 8 * 4) {
        return DataTypes.IntegerType;
      } else if (intData.getIsSigned() && intData.getBitWidth() == 8 * 8) {
        return DataTypes.LongType;
      }
    } else if (data instanceof ArrowType.FloatingPoint) {
      ArrowType.FloatingPoint floatData = (ArrowType.FloatingPoint) data;
      if (floatData.getPrecision() == FloatingPointPrecision.SINGLE) {
        return DataTypes.FloatType;
      } else if (floatData.getPrecision() == FloatingPointPrecision.DOUBLE) {
        return DataTypes.DoubleType;
      }
    } else if (data instanceof ArrowType.Utf8) {
      return DataTypes.StringType;
    } else if (data instanceof ArrowType.Binary) {
      return DataTypes.BinaryType;
    } else if (data instanceof ArrowType.Decimal) {
      ArrowType.Decimal decimalData = (ArrowType.Decimal) data;
      return new DecimalType(decimalData.getPrecision(), decimalData.getScale());
    } else if (data instanceof ArrowType.Date && ((ArrowType.Date) data).getUnit() == DateUnit.DAY) {
      return DataTypes.DateType;
    } else if (data instanceof ArrowType.Timestamp && ((ArrowType.Timestamp) data).getUnit() == TimeUnit.MICROSECOND) {
      return DataTypes.TimestampType;
    }

    throw new UnsupportedOperationException("Unsupported data type: " + data);
  }

  public DataType fromArrowField(Field field) {
    ArrowType arrowType = field.getType();
    if (arrowType instanceof ArrowType.List) {
      Field elementField = field.getChildren().get(0);
      DataType elementType = fromArrowField(elementField);
      return new ArrayType(elementType, elementField.isNullable());
    } else if (arrowType instanceof ArrowType.Struct) {
      StructField[] fields = new StructField[field.getChildren().size()];
      int index = 0;
      for (Field f : field.getChildren()) {
        DataType dt = fromArrowField(f);
        fields[index++] = new StructField(f.getName(), dt, f.isNullable(), Metadata.empty());
      }
      return new StructType(fields);
    } else {
      return fromArrowType(arrowType);
    }
  }
}
