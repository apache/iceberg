/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.spark.data;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.avro.AvroSchemaUtil;
import com.netflix.iceberg.types.Type;
import com.netflix.iceberg.types.TypeUtil;
import com.netflix.iceberg.types.Types;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;

public class RandomData {
  public static List<Record> generate(Schema schema, int numRecords, long seed) {
    RandomDataGenerator generator = new RandomDataGenerator(schema, seed);
    List<Record> records = Lists.newArrayListWithExpectedSize(numRecords);
    for (int i = 0; i < numRecords; i += 1) {
      records.add((Record) TypeUtil.visit(schema, generator));
    }

    return records;
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
    public Object map(Types.MapType map, Supplier<Object> valueResult) {
      int numEntries = random.nextInt(20);

      Map<String, Object> result = Maps.newLinkedHashMap();
      for (int i = 0; i < numEntries; i += 1) {
        String key = randomString(random) + i; // add i to ensure no collisions
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
      int choice = random.nextInt(20);

      switch (primitive.typeId()) {
        case BOOLEAN:
          return choice < 10;

        case INTEGER:
          switch (choice) {
            case 1:
              return Integer.MIN_VALUE;
            case 2:
              return Integer.MAX_VALUE;
            case 3:
              return 0;
            default:
              return random.nextInt();
          }

        case LONG:
          switch (choice) {
            case 1:
              return Long.MIN_VALUE;
            case 2:
              return Long.MAX_VALUE;
            case 3:
              return 0L;
            default:
              return random.nextLong();
          }

        case FLOAT:
          switch (choice) {
            case 1:
              return Float.MIN_VALUE;
            case 2:
              return -Float.MIN_VALUE;
            case 3:
              return Float.MAX_VALUE;
            case 4:
              return -Float.MAX_VALUE;
            case 5:
              return Float.NEGATIVE_INFINITY;
            case 6:
              return Float.POSITIVE_INFINITY;
            case 7:
              return 0.0F;
            case 8:
              return Float.NaN;
            default:
              return random.nextFloat();
          }

        case DOUBLE:
          switch (choice) {
            case 1:
              return Double.MIN_VALUE;
            case 2:
              return -Double.MIN_VALUE;
            case 3:
              return Double.MAX_VALUE;
            case 4:
              return -Double.MAX_VALUE;
            case 5:
              return Double.NEGATIVE_INFINITY;
            case 6:
              return Double.POSITIVE_INFINITY;
            case 7:
              return 0.0D;
            case 8:
              return Double.NaN;
            default:
              return random.nextDouble();
          }

        case DATE:
          return random.nextInt();

        case TIME:
          return (random.nextLong() & Integer.MAX_VALUE) % ONE_DAY_IN_MICROS;

        case TIMESTAMP:
          return random.nextLong();

        case STRING:
          return randomString(random);

        case UUID:
          byte[] uuidBytes = new byte[16];
          random.nextBytes(uuidBytes);
          // this will hash the uuidBytes
          return UUID.nameUUIDFromBytes(uuidBytes);

        case FIXED:
          byte[] fixed = new byte[((Types.FixedType) primitive).length()];
          random.nextBytes(fixed);
          return new GenericData.Fixed(typeToSchema.get(primitive), fixed);

        case BINARY:
          int length = random.nextInt(50);
          ByteBuffer buffer = ByteBuffer.allocate(length);
          random.nextBytes(buffer.array());
          return buffer;

        case DECIMAL:
          Types.DecimalType decimal = (Types.DecimalType) primitive;
          byte[] unscaledBytes = new byte[TypeUtil.decimalRequriedBytes(decimal.precision())];
          random.nextBytes(unscaledBytes);
          return new BigDecimal(new BigInteger(unscaledBytes), decimal.scale());

        default:
          throw new IllegalArgumentException(
              "Cannot generate random value for unknown type: " + primitive);
      }
    }
  }

  private static long ONE_DAY_IN_MICROS = 24 * 60 * 60 * 1_000_000;
  private static String CHARS =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.!?";

  private static String randomString(Random random) {
    int length = random.nextInt(50);
    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < length; i += 1) {
      sb.append(CHARS.charAt(random.nextInt(CHARS.length())));
    }

    return sb.toString();
  }
}
