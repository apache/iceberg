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

import java.util.List;
import java.util.function.Supplier;
import org.apache.flink.types.Row;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

class RandomData {
  private RandomData() {
  }

  static List<Row> generate(Schema schema, int numRecords, long seed) {
    RandomRowDataGenerator generator = new RandomRowDataGenerator(seed);
    List<Row> rows = Lists.newArrayListWithExpectedSize(numRecords);
    for (int i = 0; i < numRecords; i += 1) {
      rows.add((Row) TypeUtil.visit(schema, generator));
    }
    return rows;
  }

  private static class RandomRowDataGenerator extends RandomGenericData.RandomDataGenerator<Row> {

    private RandomRowDataGenerator(long seed) {
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
