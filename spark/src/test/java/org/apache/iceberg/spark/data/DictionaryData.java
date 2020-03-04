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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;


public class DictionaryData {

  private DictionaryData() {}

  public static List<GenericData.Record> generateDictionaryEncodableData(Schema schema, int numRecords, long seed) {
    List<GenericData.Record> records = Lists.newArrayListWithExpectedSize(numRecords);
    DictionaryDataGenerator dictionaryDataGenerator = new DictionaryDataGenerator(schema, seed);
    for (int i = 0; i < numRecords; i += 1) {
      GenericData.Record rec = (GenericData.Record) TypeUtil.visit(schema, dictionaryDataGenerator);
      records.add(rec);
    }
    return records;
  }

  private static class DictionaryDataGenerator extends TypeUtil.CustomOrderSchemaVisitor<Object> {
    private final Map<Type, org.apache.avro.Schema> typeToSchema;
    private final Random random;

    private DictionaryDataGenerator(Schema schema, long seed) {
      this.typeToSchema = AvroSchemaUtil.convertTypes(schema.asStruct(), "test");
      this.random = new Random(seed);
    }

    @Override
    public GenericData.Record schema(Schema schema, Supplier<Object> structResult) {
      return (GenericData.Record) structResult.get();
    }

    @Override
    public GenericData.Record struct(Types.StructType struct, Iterable<Object> fieldResults) {
      GenericData.Record rec = new GenericData.Record(typeToSchema.get(struct));

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
      Object result = generatePrimitive(primitive, random);
      // For the primitives that Avro needs a different type than Spark, fix
      // them here.
      switch (primitive.typeId()) {
        case STRING:
          return ((UTF8String) result).toString();
        case FIXED:
          return new GenericData.Fixed(
              typeToSchema.get(primitive),
              (byte[]) result);
        case BINARY:
          return ByteBuffer.wrap((byte[]) result);
        case UUID:
          return UUID.nameUUIDFromBytes((byte[]) result);
        case DECIMAL:
          return ((Decimal) result).toJavaBigDecimal();
        default:
          return result;
      }
    }
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private static Object generatePrimitive(
      Type.PrimitiveType primitive,
      Random random) {
    // 3 choices
    int choice = random.nextInt(3);
    switch (primitive.typeId()) {
      case BOOLEAN:
        return true; // doesn't really matter for booleans since they are not dictionary encoded

      case INTEGER:
        switch (choice) {
          case 0:
            return 0;
          case 1:
            return 1;
          case 2:
            return 2;
        }

      case LONG:
        switch (choice) {
          case 0:
            return 0L;
          case 1:
            return 1L;
          case 2:
            return 2L;
        }

      case FLOAT:
        switch (choice) {
          case 0:
            return 0.0f;
          case 1:
            return 1.0f;
          case 2:
            return 2.0f;
        }

      case DOUBLE:
        switch (choice) {
          case 0:
            return 0.0d;
          case 1:
            return 1.0d;
          case 2:
            return 2.0d;
        }

      case DATE:
        switch (choice) {
          case 0:
            return 0;
          case 1:
            return 1;
          case 2:
            return 2;
        }

      case TIME:
        switch (choice) {
          case 0:
            return 0L;
          case 1:
            return 1L;
          case 2:
            return 2L;
        }

      case TIMESTAMP:
        switch (choice) {
          case 0:
            return 0L;
          case 1:
            return 1L;
          case 2:
            return 2L;
        }

      case STRING:
        switch (choice) {
          case 0:
            return UTF8String.fromString("0");
          case 1:
            return UTF8String.fromString("1");
          case 2:
            return UTF8String.fromString("2");
        }

      case FIXED:
        byte[] fixed = new byte[((Types.FixedType) primitive).length()];
        switch (choice) {
          case 0:
            fixed[0] = 0;
            return fixed;
          case 1:
            fixed[0] = 1;
            return fixed;
          case 2:
            fixed[0] = 2;
            return fixed;
        }

      case BINARY:
        byte[] binary = new byte[4];
        switch (choice) {
          case 0:
            binary[0] = 0;
            return binary;
          case 1:
            binary[0] = 1;
            return binary;
          case 2:
            binary[0] = 2;
            return binary;
        }

      case DECIMAL:
        Types.DecimalType type = (Types.DecimalType) primitive;
        switch (choice) {
          case 0:
            BigInteger unscaled = new BigInteger("1");
            return Decimal.apply(new BigDecimal(unscaled, type.scale()));
          case 1:
            unscaled = new BigInteger("2");
            return Decimal.apply(new BigDecimal(unscaled, type.scale()));
          case 2:
            unscaled = new BigInteger("3");
            return Decimal.apply(new BigDecimal(unscaled, type.scale()));
        }

      default:
        throw new IllegalArgumentException(
            "Cannot generate random value for unknown type: " + primitive);
    }
  }
}
