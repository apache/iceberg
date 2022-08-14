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

import static java.time.temporal.ChronoUnit.MICROS;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.RandomUtil;

public class RandomGenericData {
  private RandomGenericData() {}

  public static List<Record> generate(Schema schema, int numRecords, long seed) {
    return Lists.newArrayList(
        generateIcebergGenerics(schema, numRecords, () -> new RandomRecordGenerator(seed)));
  }

  public static Iterable<Record> generateFallbackRecords(
      Schema schema, int numRecords, long seed, long numDictRows) {
    return generateIcebergGenerics(
        schema, numRecords, () -> new FallbackGenerator(seed, numDictRows));
  }

  public static Iterable<Record> generateDictionaryEncodableRecords(
      Schema schema, int numRecords, long seed) {
    return generateIcebergGenerics(schema, numRecords, () -> new DictionaryEncodedGenerator(seed));
  }

  private static Iterable<Record> generateIcebergGenerics(
      Schema schema, int numRecords, Supplier<RandomDataGenerator<Record>> supplier) {
    return () ->
        new Iterator<Record>() {
          private final RandomDataGenerator<Record> generator = supplier.get();
          private int count = 0;

          @Override
          public boolean hasNext() {
            return count < numRecords;
          }

          @Override
          public Record next() {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            ++count;
            return (Record) TypeUtil.visit(schema, generator);
          }
        };
  }

  private static class RandomRecordGenerator extends RandomDataGenerator<Record> {
    private RandomRecordGenerator(long seed) {
      super(seed);
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
  }

  private static class DictionaryEncodedGenerator extends RandomRecordGenerator {
    DictionaryEncodedGenerator(long seed) {
      super(seed);
    }

    @Override
    protected int getMaxEntries() {
      // Here we limited the max entries in LIST or MAP to be 3, because we have the mechanism to
      // duplicate
      // the keys in RandomDataGenerator#map while the dictionary encoder will generate a string
      // with
      // limited values("0","1","2"). It's impossible for us to request the generator to generate
      // more than 3 keys,
      // otherwise we will get in a infinite loop in RandomDataGenerator#map.
      return 3;
    }

    @Override
    protected Object randomValue(Type.PrimitiveType primitive, Random random) {
      return RandomUtil.generateDictionaryEncodablePrimitive(primitive, random);
    }
  }

  private static class FallbackGenerator extends RandomRecordGenerator {
    private final long dictionaryEncodedRows;
    private long rowCount = 0;

    FallbackGenerator(long seed, long numDictionaryEncoded) {
      super(seed);
      this.dictionaryEncodedRows = numDictionaryEncoded;
    }

    @Override
    protected Object randomValue(Type.PrimitiveType primitive, Random rand) {
      this.rowCount += 1;
      if (rowCount > dictionaryEncodedRows) {
        return RandomUtil.generatePrimitive(primitive, rand);
      } else {
        return RandomUtil.generateDictionaryEncodablePrimitive(primitive, rand);
      }
    }
  }

  public abstract static class RandomDataGenerator<T>
      extends TypeUtil.CustomOrderSchemaVisitor<Object> {
    private final Random random;
    private static final int MAX_ENTRIES = 20;

    protected RandomDataGenerator(long seed) {
      this.random = new Random(seed);
    }

    protected int getMaxEntries() {
      return MAX_ENTRIES;
    }

    @Override
    public abstract T schema(Schema schema, Supplier<Object> structResult);

    @Override
    public abstract T struct(Types.StructType struct, Iterable<Object> fieldResults);

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
      int numElements = random.nextInt(getMaxEntries());

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
      int numEntries = random.nextInt(getMaxEntries());

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
      Object result = randomValue(primitive, random);
      switch (primitive.typeId()) {
        case BINARY:
          return ByteBuffer.wrap((byte[]) result);
        case UUID:
          return UUID.nameUUIDFromBytes((byte[]) result);
        case DATE:
          return EPOCH_DAY.plusDays((Integer) result);
        case TIME:
          return LocalTime.ofNanoOfDay((long) result * 1000);
        case TIMESTAMP:
          Types.TimestampType ts = (Types.TimestampType) primitive;
          if (ts.shouldAdjustToUTC()) {
            return EPOCH.plus((long) result, MICROS);
          } else {
            return EPOCH.plus((long) result, MICROS).toLocalDateTime();
          }
        default:
          return result;
      }
    }

    protected Object randomValue(Type.PrimitiveType primitive, Random rand) {
      return RandomUtil.generatePrimitive(primitive, rand);
    }
  }

  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();
}
