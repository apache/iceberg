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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.RandomUtil;

import static java.time.temporal.ChronoUnit.MICROS;

public class RandomRowData {
  private RandomRowData() {
  }

  public static Iterable<RowData> generate(Schema schema, int numRecords, long seed) {
    return generate(schema, numRecords, () -> new RandomRowGenerator(seed));
  }

  private static Iterable<RowData> generate(Schema schema, int numRecords, Supplier<RandomRowGenerator> supplier) {
    return () -> new Iterator<RowData>() {
      private final RandomRowGenerator generator = supplier.get();
      private int count = 0;

      @Override
      public boolean hasNext() {
        return count < numRecords;
      }

      @Override
      public RowData next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        ++count;
        return (RowData) TypeUtil.visit(schema, generator);
      }
    };
  }

  private static class RandomRowGenerator extends TypeUtil.CustomOrderSchemaVisitor<Object> {
    private final Random random;

    private RandomRowGenerator(long seed) {
      this.random = new Random(seed);
    }

    @Override
    public RowData schema(Schema schema, Supplier<Object> structResult) {
      return (RowData) structResult.get();
    }

    @Override
    public RowData struct(Types.StructType struct, Iterable<Object> fieldResults) {
      List<Object> values = Lists.newArrayList(fieldResults);
      GenericRowData rowData = new GenericRowData(values.size());
      for (int i = 0; i < values.size(); i += 1) {
        rowData.setField(i, values.get(i));
      }
      return rowData;
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
    public GenericMapData map(Types.MapType map, Supplier<Object> keyResult, Supplier<Object> valueResult) {
      int numEntries = random.nextInt(20);

      Map<Object, Object> hashMap = Maps.newHashMap();
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
          hashMap.put(key, null);
        } else {
          hashMap.put(key, valueResult.get());
        }
      }

      return new GenericMapData(hashMap);
    }

    @Override
    public Object primitive(Type.PrimitiveType primitive) {
      Object obj = RandomUtil.generatePrimitive(primitive, random);
      switch (primitive.typeId()) {
        case STRING:
          return StringData.fromString((String) obj);
        case DECIMAL:
          BigDecimal decimal = (BigDecimal) obj;
          return DecimalData.fromBigDecimal(decimal, decimal.precision(), decimal.scale());
        case TIME:
          long micros = (long) obj;
          // The iceberg's time is in microseconds, while flink's time is in milliseconds.
          return (int) (micros / 1_000);
        case TIMESTAMP:
          Types.TimestampType tsType = (Types.TimestampType) primitive;
          if (tsType.shouldAdjustToUTC()) {
            OffsetDateTime offsetDateTime = EPOCH.plus((long) obj, MICROS);
            return TimestampData.fromInstant(offsetDateTime.toInstant());
          } else {
            LocalDateTime localDateTime = EPOCH.plus((long) obj, MICROS).toLocalDateTime();
            return TimestampData.fromLocalDateTime(localDateTime);
          }
        default:
          return obj;
      }
    }
  }

  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
}
