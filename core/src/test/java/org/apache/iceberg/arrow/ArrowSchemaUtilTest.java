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

package org.apache.iceberg.arrow;


import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.BooleanType;
import org.apache.iceberg.types.Types.DateType;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.spark.sql.execution.arrow.ArrowUtils;
import org.apache.spark.sql.execution.arrow.ArrowWriter;
import org.apache.spark.sql.execution.arrow.BooleanWriter;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.Test;

import static org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID.Bool;
import static org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID.Date;
import static org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID.FloatingPoint;
import static org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID.Int;
import static org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID.List;
import static org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID.Timestamp;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class ArrowSchemaUtilTest {

  @Test
  public void convertPrimitive() {
    Schema iceberg = new Schema(
        optional(0, "i", Types.IntegerType.get()),
        optional(1, "b", BooleanType.get()),
        required(2, "d", DoubleType.get()),
        required(3, "s", StringType.get()),
        optional(4, "d2", DateType.get()),
        optional(5, "ts", TimestampType.withoutZone())
    );

    org.apache.arrow.vector.types.pojo.Schema arrow = ArrowSchemaUtil.convert(iceberg);

    System.out.println(iceberg);
    System.out.println(arrow);

    validate(iceberg, arrow);
  }

  @Test
  public void convertComplex() {
    Schema iceberg = new Schema(
        optional(0, "m", MapType.ofOptional(
            1, 2, StringType.get(),
            LongType.get())
        ),
        required(3, "m2", MapType.ofOptional(
            4, 5, StringType.get(),
            ListType.ofOptional(6, TimestampType.withoutZone()))
        )
    );

    org.apache.arrow.vector.types.pojo.Schema arrow = ArrowSchemaUtil.convert(iceberg);

    System.out.println(iceberg);
    System.out.println(arrow);

    assertEquals(iceberg.columns().size(), arrow.getFields().size());
  }

  private void validate(Schema iceberg, org.apache.arrow.vector.types.pojo.Schema arrow) {
    assertEquals(iceberg.columns().size(), arrow.getFields().size());

    for (Types.NestedField nf : iceberg.columns()) {
      Field field = arrow.findField(nf.name());
      assertNotNull("Missing filed: " + nf, field);

      validate(nf.type(), field.getType());
    }
  }

  private void validate(Type iceberg, ArrowType arrow) {
    switch (iceberg.typeId()) {
      case BOOLEAN: assertEquals(Bool, arrow.getTypeID());
        break;
      case INTEGER: assertEquals(Int, arrow.getTypeID());
        break;
      case LONG: assertEquals(Int, arrow.getTypeID());
        break;
      case DOUBLE: assertEquals(FloatingPoint, arrow.getTypeID());
        break;
      case STRING: assertEquals(ArrowType.Utf8.INSTANCE.getTypeID(), arrow.getTypeID());
        break;
      case DATE: assertEquals(Date, arrow.getTypeID());
        break;
      case TIMESTAMP: assertEquals(Timestamp, arrow.getTypeID());
        break;
      case MAP: assertEquals(List, arrow.getTypeID());
        break;
      default: throw new UnsupportedOperationException("Check not implemented for type: " + iceberg);
    }
  }
}
