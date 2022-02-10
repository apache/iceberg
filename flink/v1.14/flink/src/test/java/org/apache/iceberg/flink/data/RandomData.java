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

package org.apache.iceberg.flink.data;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.RandomUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

public class RandomData {

  // Default percentage of number of values that are null for optional fields
  public static final float DEFAULT_NULL_PERCENTAGE = 0.05f;

  private RandomData() {
  }

  public static Iterable<InternalRow> generateSpark(Schema schema, int numRecords, long seed) {
    return () -> new Iterator<InternalRow>() {
      private SparkRandomDataGenerator generator = new SparkRandomDataGenerator(seed);
      private int count = 0;

      @Override
      public boolean hasNext() {
        return count < numRecords;
      }

      @Override
      public InternalRow next() {
        if (count >= numRecords) {
          throw new NoSuchElementException();
        }
        count += 1;
        return (InternalRow) TypeUtil.visit(schema, generator);
      }
    };
  }

  private static class SparkRandomDataGenerator extends TypeUtil.CustomOrderSchemaVisitor<Object> {
    private final Random random;

    private SparkRandomDataGenerator(long seed) {
      this.random = new Random(seed);
    }

    @Override
    public InternalRow schema(Schema schema, Supplier<Object> structResult) {
      return (InternalRow) structResult.get();
    }

    @Override
    public InternalRow struct(Types.StructType struct, Iterable<Object> fieldResults) {
      List<Object> values = Lists.newArrayList(fieldResults);
      GenericInternalRow row = new GenericInternalRow(values.size());
      for (int i = 0; i < values.size(); i += 1) {
        row.update(i, values.get(i));
      }

      return row;
    }

    @Override
    public Object field(Types.NestedField field, Supplier<Object> fieldResult) {
      // return null 5% of the time when the value is optional
      if (field.isOptional() && random.nextInt(20) == 1) {
        return null;
      }
      return fieldResult.get();
    }

    @Override
    public GenericArrayData list(Types.ListType list, Supplier<Object> elementResult) {
      int numElements = random.nextInt(20);
      Object[] arr = new Object[numElements];
      GenericArrayData result = new GenericArrayData(arr);

      for (int i = 0; i < numElements; i += 1) {
        // return null 5% of the time when the value is optional
        if (list.isElementOptional() && random.nextInt(20) == 1) {
          arr[i] = null;
        } else {
          arr[i] = elementResult.get();
        }
      }

      return result;
    }

    @Override
    public Object map(Types.MapType map, Supplier<Object> keyResult, Supplier<Object> valueResult) {
      int numEntries = random.nextInt(20);

      Object[] keysArr = new Object[numEntries];
      Object[] valuesArr = new Object[numEntries];
      GenericArrayData keys = new GenericArrayData(keysArr);
      GenericArrayData values = new GenericArrayData(valuesArr);
      ArrayBasedMapData result = new ArrayBasedMapData(keys, values);

      Set<Object> keySet = Sets.newHashSet();
      for (int i = 0; i < numEntries; i += 1) {
        Object key = keyResult.get();
        // ensure no collisions
        while (keySet.contains(key)) {
          key = keyResult.get();
        }

        keySet.add(key);

        keysArr[i] = key;
        // return null 5% of the time when the value is optional
        if (map.isValueOptional() && random.nextInt(20) == 1) {
          valuesArr[i] = null;
        } else {
          valuesArr[i] = valueResult.get();
        }
      }

      return result;
    }

    @Override
    public Object primitive(Type.PrimitiveType primitive) {
      Object obj = RandomUtil.generatePrimitive(primitive, random);
      switch (primitive.typeId()) {
        case STRING:
          return UTF8String.fromString((String) obj);
        case DECIMAL:
          return Decimal.apply((BigDecimal) obj);
        default:
          return obj;
      }
    }
  }
}
