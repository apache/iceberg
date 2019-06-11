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

package org.apache.iceberg.data;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import static java.time.temporal.ChronoUnit.MICROS;

public class RandomGenericData {
  private RandomGenericData() {}

  public static List<Record> generate(Schema schema, int numRecords, long seed) {
    RandomDataGenerator generator = new RandomDataGenerator(seed);
    List<Record> records = Lists.newArrayListWithExpectedSize(numRecords);
    for (int i = 0; i < numRecords; i += 1) {
      records.add((Record) TypeUtil.visit(schema, generator));
    }

    return records;
  }

  private static class RandomDataGenerator extends TypeUtil.CustomOrderSchemaVisitor<Object> {
    private final Random random;

    private RandomDataGenerator(long seed) {
      this.random = new Random(seed);
    }

    @Override
    public Record schema(Schema schema, Supplier<Object> structResult) {
      return (Record) structResult.get();
    }

    @Override
    public Record struct(Types.StructType struct, Iterable<Object> fieldResults) {
      Record rec = GenericRecord.create(struct);

      List<Object> values = Lists.newArrayList(fieldResults);
      for (int i = 0; i < values.size(); i += 1) {
        rec.set(i, values.get(i));
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
      Supplier<Object> keyFunc;
      if (map.keyType() == Types.StringType.get()) {
        keyFunc = () -> keyResult.get().toString();
      } else {
        keyFunc = keyResult;
      }

      Set<Object> keySet = Sets.newHashSet();
      for (int i = 0; i < numEntries; i += 1) {
        Object key = keyFunc.get();
        // ensure no collisions
        while (keySet.contains(key)) {
          key = keyFunc.get();
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
      switch (primitive.typeId()) {
        case BINARY:
          return ByteBuffer.wrap((byte[]) result);
        case UUID:
          return UUID.nameUUIDFromBytes((byte[]) result);
        default:
          return result;
      }
    }
  }

  @SuppressWarnings("RandomModInteger")
  private static Object generatePrimitive(Type.PrimitiveType primitive, Random random) {
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
        return EPOCH_DAY.plusDays(random.nextInt() % ABOUT_380_YEARS_IN_DAYS);

      case TIME:
        return LocalTime.ofNanoOfDay(
            ((random.nextLong() & Integer.MAX_VALUE) % ONE_DAY_IN_MICROS) * 1000);

      case TIMESTAMP:
        Types.TimestampType ts = (Types.TimestampType) primitive;
        if (ts.shouldAdjustToUTC()) {
          return EPOCH.plus(random.nextLong() % FIFTY_YEARS_IN_MICROS, MICROS);
        } else {
          return EPOCH.plus(random.nextLong() % FIFTY_YEARS_IN_MICROS, MICROS).toLocalDateTime();
        }

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
        return new BigDecimal(unscaled, type.scale());

      default:
        throw new IllegalArgumentException(
            "Cannot generate random value for unknown type: " + primitive);
    }
  }

  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  private static final long FIFTY_YEARS_IN_MICROS =
      (50L * (365 * 3 + 366) * 24 * 60 * 60 * 1_000_000) / 4;
  private static final int ABOUT_380_YEARS_IN_DAYS = 380 * 365;
  private static final long ONE_DAY_IN_MICROS = 24 * 60 * 60 * 1_000_000L;
  private static final String CHARS =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.!?";

  private static String randomString(Random random) {
    int length = random.nextInt(50);
    byte[] buffer = new byte[length];

    for (int i = 0; i < length; i += 1) {
      buffer[i] = (byte) CHARS.charAt(random.nextInt(CHARS.length()));
    }

    return new String(buffer, StandardCharsets.UTF_8);
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

    return new BigInteger(sb.toString());
  }
}
