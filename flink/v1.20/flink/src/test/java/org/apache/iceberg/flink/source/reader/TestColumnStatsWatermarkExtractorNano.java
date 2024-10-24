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
package org.apache.iceberg.flink.source.reader;

import static org.apache.iceberg.flink.TestFixtures.DATABASE;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.HadoopTableExtension;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(ParameterizedTestExtension.class)
public class TestColumnStatsWatermarkExtractorNano extends TestColumnStatsWatermarkExtractorBase {

  public static final Schema SCHEMA =
      new Schema(
          required(1, "timestamp_column", Types.TimestampNanoType.withoutZone()),
          required(2, "timestamptz_column", Types.TimestampNanoType.withZone()),
          required(3, "long_column", Types.LongType.get()),
          required(4, "string_column", Types.StringType.get()));

  private static final List<List<Record>> TEST_RECORDS =
      ImmutableList.of(
          RandomGenericData.generate(SCHEMA, 3, 2L), RandomGenericData.generate(SCHEMA, 3, 19L));

  protected static final List<Map<String, Long>> MIN_VALUES =
      ImmutableList.of(Maps.newHashMapWithExpectedSize(3), Maps.newHashMapWithExpectedSize(3));

  @RegisterExtension
  private static final HadoopTableExtension SOURCE_TABLE_EXTENSION =
      new HadoopTableExtension(DATABASE, TestFixtures.TABLE, SCHEMA, true);

  @Override
  public Schema getSchema() {
    return SCHEMA;
  }

  @Override
  public List<List<Record>> getTestRecords() {
    return TEST_RECORDS;
  }

  @Override
  public List<Map<String, Long>> getMinValues() {
    return MIN_VALUES;
  }

  @BeforeAll
  public static void updateMinValue() {
    for (int i = 0; i < TEST_RECORDS.size(); ++i) {
      for (Record r : TEST_RECORDS.get(i)) {
        Map<String, Long> minValues = MIN_VALUES.get(i);

        LocalDateTime localDateTime = (LocalDateTime) r.get(0);
        minValues.merge(
            "timestamp_column", localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli(), Math::min);

        OffsetDateTime offsetDateTime = (OffsetDateTime) r.get(1);
        minValues.merge("timestamptz_column", offsetDateTime.toInstant().toEpochMilli(), Math::min);

        minValues.merge("long_column", (Long) r.get(2), Math::min);
      }
    }
  }
}
