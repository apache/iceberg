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

  public static List<Record> generateListWithFallBackDictionaryEncodingForStrings(
          Schema schema,
          int numRecords,
          long seed,
          float fraction) {
    FallbackDictionaryEncodedDataGenerator generator =
            new FallbackDictionaryEncodedDataGenerator(schema, seed, numRecords, fraction);
    List<Record> records = Lists.newArrayListWithExpectedSize(numRecords);
    for (int i = 0; i < numRecords; i += 1) {
      Record rec = (Record) TypeUtil.visit(schema, generator);
      records.add(rec);
    }

    return records;
  }

  public static List<GenericData.Record> generateDictionaryEncodableData(Schema schema, int numRecords, long seed) {
    List<GenericData.Record> records = Lists.newArrayListWithExpectedSize(numRecords);
    DictionaryEncodedDataGenerator
        dictionaryDataGenerator = new DictionaryEncodedDataGenerator(schema, seed);
    for (int i = 0; i < numRecords; i += 1) {
      GenericData.Record rec = (GenericData.Record) TypeUtil.visit(schema, dictionaryDataGenerator);
      records.add(rec);
    }
    return records;
  }

  @SuppressWarnings("checkstyle:VisibilityModifier")
  private static class RandomDataGenerator extends TypeUtil.CustomOrderSchemaVisitor<Object> {
    private final Map<Type, org.apache.avro.Schema> typeToSchema;
    final Random random;

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
      return getPrimitive(primitive, result);
    }

    Object getPrimitive(Type.PrimitiveType primitive, Object result) {
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

  @SuppressWarnings("RandomModInteger")
  private static Object generatePrimitive(Type.PrimitiveType primitive,
                                         Random random) {
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
        // this will include negative values (dates before 1970-01-01)
        return random.nextInt() % ABOUT_380_YEARS_IN_DAYS;

      case TIME:
        return (random.nextLong() & Integer.MAX_VALUE) % ONE_DAY_IN_MICROS;

      case TIMESTAMP:
        return random.nextLong() % FIFTY_YEARS_IN_MICROS;

      case STRING:
        return randomString(random);

      case UUID:
        byte[] uuidBytes = new byte[16];
        random.nextBytes(uuidBytes);
        // this will hash the uuidBytes
        return uuidBytes;

      case FIXED:
        byte[] fixed = new byte[((Types.FixedType) primitive).length()];
        random.nextBytes(fixed);
        return fixed;

      case BINARY:
        byte[] binary = new byte[random.nextInt(50)];
        random.nextBytes(binary);
        return binary;

      case DECIMAL:
        Types.DecimalType type = (Types.DecimalType) primitive;
        BigInteger unscaled = randomUnscaled(type.precision(), random);
        return Decimal.apply(new BigDecimal(unscaled, type.scale()));

      default:
        throw new IllegalArgumentException(
            "Cannot generate random value for unknown type: " + primitive);
    }
  }

  private static final long FIFTY_YEARS_IN_MICROS =
      (50L * (365 * 3 + 366) * 24 * 60 * 60 * 1_000_000) / 4;
  private static final int ABOUT_380_YEARS_IN_DAYS = 380 * 365;
  private static final long ONE_DAY_IN_MICROS = 24 * 60 * 60 * 1_000_000L;
  private static final String CHARS =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.!?";

  private static UTF8String randomString(Random random) {
    int length = random.nextInt(50);
    byte[] buffer = new byte[length];

    for (int i = 0; i < length; i += 1) {
      buffer[i] = (byte) CHARS.charAt(random.nextInt(CHARS.length()));
    }

    return UTF8String.fromBytes(buffer);
  }

  private static final String DIGITS = "0123456789";

  private static BigInteger randomUnscaled(int precision, Random random) {
    int length = random.nextInt(precision);
    if (length == 0) {
      return BigInteger.ZERO;
    }

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i += 1) {
      sb.append(DIGITS.charAt(random.nextInt(DIGITS.length())));
    }
  }

  private static class DictionaryEncodedDataGenerator extends RandomDataGenerator {

    private DictionaryEncodedDataGenerator(Schema schema, long seed) {
      super(schema, seed);
    }

    @Override
    public Object primitive(Type.PrimitiveType primitive) {
      Object result = generateDictionaryEncodablePrimitive(primitive, random);
      return super.getPrimitive(primitive, result);
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private static Object generateDictionaryEncodablePrimitive(Type.PrimitiveType primitive, Random random) {
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

  private static class FallbackDictionaryEncodedDataGenerator extends RandomDataGenerator {

    private final int numRecords;
    private final float fraction;
    private int current;

    private FallbackDictionaryEncodedDataGenerator(Schema schema, long seed, int numRecords, float fraction) {
      super(schema, seed);
      this.numRecords = numRecords;
      this.fraction = fraction;
    }

    @Override
    public Object primitive(Type.PrimitiveType primitive) {
      switch (primitive.typeId()) {
        case STRING:
          if (current < fraction * numRecords) {
            current++;
            return "ABC";
          } else {
            current++;
            return super.primitive(primitive);
          }
      }
      return super.primitive(primitive);
    }
  }
}
