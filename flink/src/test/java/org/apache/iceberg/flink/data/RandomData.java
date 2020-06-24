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
import java.util.Random;
import java.util.function.Supplier;
import org.apache.flink.types.Row;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.RandomUtil;

class RandomData {
  private RandomData() {
  }

  private static Iterable<Row> generateData(Schema schema, int numRecords, Supplier<RandomRowDataGenerator> supplier) {
    return () -> new Iterator<Row>() {
      private final RandomRowDataGenerator generator = supplier.get();
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

  static Iterable<Row> generate(Schema schema, int numRecords, long seed) {
    return generateData(schema, numRecords, () -> new RandomRowDataGenerator(seed));
  }

  static Iterable<Row> generateFallbackData(Schema schema, int numRecords, long seed, long numDictRows) {
    return generateData(schema, numRecords, () -> new FallbackDataGenerator(seed, numDictRows));
  }

  static Iterable<Row> generateDictionaryEncodableData(Schema schema, int numRecords, long seed) {
    return generateData(schema, numRecords, () -> new DictionaryEncodedDataGenerator(seed));
  }

  private static class RandomRowDataGenerator extends RandomGenericData.RandomDataGenerator<Row> {

    RandomRowDataGenerator(long seed) {
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

  private static class DictionaryEncodedDataGenerator extends RandomRowDataGenerator {
    DictionaryEncodedDataGenerator(long seed) {
      super(seed);
    }

    @Override
    protected int getMaxEntries() {
      // Here we limited the max entries in LIST or MAP to be 3, because we have the mechanism to duplicate
      // the keys in RandomDataGenerator#map while the dictionary encoder will generate a string with
      // limited values("0","1","2"). It's impossible for us to request the generator to generate more than 3 keys,
      // otherwise we will get in a infinite loop in RandomDataGenerator#map.
      return 3;
    }

    @Override
    protected Object randomValue(Type.PrimitiveType primitive, Random random) {
      return RandomUtil.generateDictionaryEncodablePrimitive(primitive, random);
    }
  }

  private static class FallbackDataGenerator extends RandomRowDataGenerator {
    private final long dictionaryEncodedRows;
    private long rowCount = 0;

    FallbackDataGenerator(long seed, long numDictionaryEncoded) {
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
}
