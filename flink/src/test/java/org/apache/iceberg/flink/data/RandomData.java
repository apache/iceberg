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

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Supplier;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class RandomData {
  private RandomData() {
  }

  static final Schema COMPLEX_SCHEMA = new Schema(
      required(1, "roots", Types.LongType.get()),
      optional(3, "lime", Types.ListType.ofRequired(4, Types.DoubleType.get())),
      required(5, "strict", Types.StructType.of(
          required(9, "tangerine", Types.StringType.get()),
          optional(6, "hopeful", Types.StructType.of(
              required(7, "steel", Types.FloatType.get()),
              required(8, "lantern", Types.DateType.get())
          )),
          optional(10, "vehement", Types.LongType.get())
      )),
      optional(11, "metamorphosis", Types.MapType.ofRequired(12, 13,
          Types.StringType.get(), Types.TimestampType.withZone())),
      required(14, "winter", Types.ListType.ofOptional(15, Types.StructType.of(
          optional(16, "beet", Types.DoubleType.get()),
          required(17, "stamp", Types.FloatType.get()),
          optional(18, "wheeze", Types.StringType.get())
      ))),
      optional(19, "renovate", Types.MapType.ofRequired(20, 21,
          Types.StringType.get(), Types.StructType.of(
              optional(22, "jumpy", Types.DoubleType.get()),
              required(23, "koala", Types.IntegerType.get()),
              required(24, "couch rope", Types.IntegerType.get())
          ))),
      optional(2, "slide", Types.StringType.get())
  );

  private static Iterable<Row> generateData(Schema schema, int numRecords, Supplier<RandomRowGenerator> supplier) {
    return () -> new Iterator<Row>() {
      private final RandomRowGenerator generator = supplier.get();
      private int count = 0;

      @Override
      public boolean hasNext() {
        return count < numRecords;
      }

      @Override
      public Row next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        ++count;
        return (Row) TypeUtil.visit(schema, generator);
      }
    };
  }

  private static Iterable<RowData> generateRowData(Schema schema, int numRecords,
                                                   Supplier<RandomRowGenerator> supplier) {
    DataStructureConverter<Object, Object> converter =
        DataStructureConverters.getConverter(TypeConversions.fromLogicalToDataType(FlinkSchemaUtil.convert(schema)));
    converter.open(RandomData.class.getClassLoader());

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
        return (RowData) converter.toInternal(TypeUtil.visit(schema, generator));
      }
    };
  }

  public static Iterable<RowData> generateRowData(Schema schema, int numRecords, long seed) {
    return generateRowData(schema, numRecords, () -> new RandomRowGenerator(seed));
  }

  public static Iterable<Row> generate(Schema schema, int numRecords, long seed) {
    return generateData(schema, numRecords, () -> new RandomRowGenerator(seed));
  }

  private static class RandomRowGenerator extends RandomGenericData.RandomDataGenerator<Row> {
    RandomRowGenerator(long seed) {
      super(seed);
    }

    @Override
    public Row schema(Schema schema, Supplier<Object> structResult) {
      return (Row) structResult.get();
    }

    @Override
    public Row struct(Types.StructType struct, Iterable<Object> fieldResults) {
      Row row = new Row(struct.fields().size());

      List<Object> values = Lists.newArrayList(fieldResults);
      for (int i = 0; i < values.size(); i += 1) {
        row.setField(i, values.get(i));
      }

      return row;
    }
  }
}
