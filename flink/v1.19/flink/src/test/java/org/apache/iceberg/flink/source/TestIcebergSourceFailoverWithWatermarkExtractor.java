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
package org.apache.iceberg.flink.source;

import static org.apache.iceberg.flink.TestFixtures.DATABASE;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkReadOptions;
import org.apache.iceberg.flink.HadoopTableExtension;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.util.StructLikeWrapper;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.extension.RegisterExtension;

public class TestIcebergSourceFailoverWithWatermarkExtractor extends TestIcebergSourceFailover {
  // Increment ts by 15 minutes for each generateRecords batch
  private static final long RECORD_BATCH_TS_INCREMENT_MILLI = TimeUnit.MINUTES.toMillis(15);
  // Within a batch, increment ts by 1 second
  private static final long RECORD_TS_INCREMENT_MILLI = TimeUnit.SECONDS.toMillis(1);

  private final AtomicLong tsMilli = new AtomicLong(System.currentTimeMillis());

  @RegisterExtension
  private static final HadoopTableExtension SOURCE_TABLE_EXTENSION =
      new HadoopTableExtension(DATABASE, TestFixtures.TABLE, TestFixtures.TS_SCHEMA);

  @Override
  protected IcebergSource.Builder<RowData> sourceBuilder() {
    Configuration config = new Configuration();
    return IcebergSource.forRowData()
        .tableLoader(SOURCE_TABLE_EXTENSION.tableLoader())
        .watermarkColumn("ts")
        .project(TestFixtures.TS_SCHEMA)
        // Prevent combining splits
        .set(
            FlinkReadOptions.SPLIT_FILE_OPEN_COST,
            Long.toString(TableProperties.SPLIT_SIZE_DEFAULT))
        .flinkConfig(config);
  }

  @Override
  protected Schema schema() {
    return TestFixtures.TS_SCHEMA;
  }

  @Override
  protected List<Record> generateRecords(int numRecords, long seed) {
    // Override the ts field to create a more realistic situation for event time alignment
    tsMilli.addAndGet(RECORD_BATCH_TS_INCREMENT_MILLI);
    return RandomGenericData.generate(schema(), numRecords, seed).stream()
        .peek(
            record -> {
              LocalDateTime ts =
                  LocalDateTime.ofInstant(
                      Instant.ofEpochMilli(tsMilli.addAndGet(RECORD_TS_INCREMENT_MILLI)),
                      ZoneId.of("Z"));
              record.setField("ts", ts);
            })
        .collect(Collectors.toList());
  }

  /**
   * This override is needed because {@link Comparators} used by {@link StructLikeWrapper} retrieves
   * Timestamp type using Long type as inner class, while the {@link RandomGenericData} generates
   * {@link LocalDateTime} for {@code TimestampType.withoutZone()}. This method normalizes the
   * {@link LocalDateTime} to a Long type so that Comparators can continue to work.
   */
  @Override
  protected void assertRecords(Table table, List<Record> expectedRecords, Duration timeout)
      throws Exception {
    List<Record> expectedNormalized = convertLocalDateTimeToMilli(expectedRecords);
    Awaitility.await("expected list of records should be produced")
        .atMost(timeout)
        .untilAsserted(
            () ->
                SimpleDataUtil.assertRecordsEqual(
                    expectedNormalized,
                    convertLocalDateTimeToMilli(SimpleDataUtil.tableRecords(table)),
                    table.schema()));
  }

  private List<Record> convertLocalDateTimeToMilli(List<Record> records) {
    return records.stream()
        .peek(
            r -> {
              LocalDateTime localDateTime = ((LocalDateTime) r.getField("ts"));
              r.setField("ts", localDateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli());
            })
        .collect(Collectors.toList());
  }
}
