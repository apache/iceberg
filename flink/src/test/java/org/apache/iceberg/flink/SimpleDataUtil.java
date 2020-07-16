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

package org.apache.iceberg.flink;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.Assert;

public class SimpleDataUtil {

  private SimpleDataUtil() {
  }

  static final Schema SCHEMA = new Schema(
      Types.NestedField.optional(1, "id", Types.IntegerType.get()),
      Types.NestedField.optional(2, "data", Types.StringType.get())
  );

  static final Record RECORD = GenericRecord.create(SCHEMA);

  static final Comparator<Record> COMPARATOR = (r1, r2) -> {
    int ret = Integer.compare((Integer) r1.getField("id"), (Integer) r2.getField("id"));
    if (ret != 0) {
      return ret;
    }
    return ((String) r1.getField("data")).compareTo((String) r2.getField("data"));
  };

  static Table createTable(String path, Map<String, String> properties, boolean partitioned) {
    PartitionSpec spec;
    if (partitioned) {
      spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    } else {
      spec = PartitionSpec.unpartitioned();
    }
    return new HadoopTables().create(SCHEMA, spec, properties, path);
  }

  static Record createRecord(int id, String data) {
    return RECORD.copy(ImmutableMap.of("id", id, "data", data));
  }

  static void assertTableRecords(String tablePath, List<Record> expected) throws IOException {
    Preconditions.checkArgument(expected != null, "expected records shouldn't be null");
    Table newTable = new HadoopTables().load(tablePath);
    List<Record> results;
    try (CloseableIterable<Record> iterable = (CloseableIterable<Record>) IcebergGenerics.read(newTable).build()) {
      results = Lists.newArrayList(iterable);
    }
    expected.sort(COMPARATOR);
    results.sort(COMPARATOR);
    Assert.assertEquals("Should produce the expected record", expected, results);
  }
}
