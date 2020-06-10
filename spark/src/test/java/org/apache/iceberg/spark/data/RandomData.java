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

package org.apache.iceberg.spark.data;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
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

  private RandomData() {}

  public static List<Record> generateList(Schema schema, int numRecords, long seed) {
    RandomDataGenerator generator = new RandomDataGenerator(schema, seed);
    List<Record> records = Lists.newArrayListWithExpectedSize(numRecords);
    for (int i = 0; i < numRecords; i += 1) {
      records.add((Record) TypeUtil.visit(schema, generator));
    }

    return records;
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

  public static Iterable<Record> generate(Schema schema, int numRecords, long seed) {
    return () -> new Iterator<Record>() {
      private RandomDataGenerator generator = new RandomDataGenerator(schema, seed);
      private int count = 0;

      @Override
      public boolean hasNext() {
        return count < numRecords;
      }

      @Override
      public Record next() {
        if (count >= numRecords) {
          throw new NoSuchElementException();
        }
        count += 1;
        return (Record) TypeUtil.visit(schema, generator);
      }
    };
  }

  private static class RandomDataGenerator extends TypeUtil.CustomOrderSchemaVisitor<Object> {
    private final Map<Type, org.apache.avro.Schema> typeToSchema;
    private final Random random;

    private RandomDataGenerator(Schema schema, long seed) {
      this.typeToSchema = AvroSchemaUtil.convertTypes(schema.asStruct(), "test");
      this.random = new Random(seed);
    }

    @Override
    public Record schema(Schema schema, Supplier<Object> structResult) {
      return (Record) structResult.get();
    }

    @Override
    public Record struct(Types.StructType struct, Iterable<Object> fieldResults) {
      Record rec = new Record(typeToSchema.get(struct));

      List<Object> values = Lists.newArrayList(fieldResults);
      for (int i = 0; i < values.size(); i += 1) {
        rec.put(i, values.get(i));
      }

      return rec;
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
    public Object list(Types.ListType list, Supplier<Object> elementResult) {
      int numElements = random.nextInt(20);

      List<Object> result = Lists.newArrayListWithExpectedSize(numElements);
      for (int i = 0; i < numElements; i += 1) {
        // return null 5% of the time when the value is optional
        if (list.isElementOptional() && random.nextInt(20) == 1) {
          result.add(null);
        } else {
          result.add(elementResult.get());
        }
      }

      return result;
    }

    @Override
    public Object map(Types.MapType map, Supplier<Object> keyResult, Supplier<Object> valueResult) {
      int numEntries = random.nextInt(20);

      Map<Object, Object> result = Maps.newLinkedHashMap();
      Set<Object> keySet = Sets.newHashSet();
      for (int i = 0; i < numEntries; i += 1) {
        Object key = keyResult.get();
        // ensure no collisions
        while (keySet.contains(key)) {
          key = keyResult.get();
        }

        keySet.add(key);

        // return null 5% of the time when the value is optional
        if (map.isValueOptional() && random.nextInt(20) == 1) {
          result.put(key, null);
        } else {
          result.put(key, valueResult.get());
        }
      }

      return result;
    }

    @Override
    public Object primitive(Type.PrimitiveType primitive) {
      Object result = RandomUtil.generatePrimitive(primitive, random);
      // For the primitives that Avro needs a different type than Spark, fix
      // them here.
      switch (primitive.typeId()) {
        case FIXED:
          return new GenericData.Fixed(typeToSchema.get(primitive),
              (byte[]) result);
        case BINARY:
          return ByteBuffer.wrap((byte[]) result);
        case UUID:
          return UUID.nameUUIDFromBytes((byte[]) result);
        default:
          return result;
      }
    }
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
