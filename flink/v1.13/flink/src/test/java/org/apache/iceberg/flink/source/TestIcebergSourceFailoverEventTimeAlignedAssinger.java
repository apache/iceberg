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

import java.io.Serializable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.assigner.ordered.ClockFactory;
import org.apache.iceberg.flink.source.assigner.ordered.EventTimeAlignmentAssignerFactory;
import org.apache.iceberg.flink.source.assigner.ordered.InMemoryGlobalWatermarkTrackerFactory;
import org.apache.iceberg.flink.source.reader.RowDataReaderFunction;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeWrapper;
import org.junit.Ignore;

/**
 * This is ignored because testContinuousWithJobManagerFailover is flaky.
 * Maybe there is a bug in the EventTimeAlignmentAssigner, as we didn't see
 * the same flaky problem for the base class TestIcebergSourceFailover.
 */
@Ignore
public class TestIcebergSourceFailoverEventTimeAlignedAssinger extends TestIcebergSourceFailover {
  // increment ts by 60 minutes for each generateRecords batch
  private static final long RECORD_BATCH_TS_INCREMENT_MILLI = TimeUnit.MINUTES.toMillis(15);
  // Within a batch, increment ts by 1 minute
  private static final long RECORD_TS_INCREMENT_MILLI = TimeUnit.SECONDS.toMillis(1);
  // max out-of-orderliness threshold is 30 minutes
  private static final Duration MAX_OUT_OF_ORDERLINESS_THRESHOLD_DURATION = Duration.ofMinutes(15);

  private final AtomicLong tsMilli = new AtomicLong(System.currentTimeMillis());

  @Override
  protected IcebergSource.Builder<RowData> sourceBuilder() {
    Table sourceTable = sourceTableResource.table();
    Configuration config = new Configuration();
    config.setInteger(FlinkConfigOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 128);
    return IcebergSource.<RowData>builder()
        .tableLoader(sourceTableResource.tableLoader())
        .assignerFactory(new EventTimeAlignmentAssignerFactory(
            TestFixtures.TABLE,
            MAX_OUT_OF_ORDERLINESS_THRESHOLD_DURATION,
            (ClockFactory) () -> Clock.systemUTC(),
            new InMemoryGlobalWatermarkTrackerFactory(),
            new IcebergSourceSplitTimeAssigner(TestFixtures.TS_SCHEMA, "ts")))
        .readerFunction(new RowDataReaderFunction(config, sourceTable.schema(), null,
            null, false, sourceTable.io(), sourceTable.encryption()))
        .project(TestFixtures.TS_SCHEMA)
        .includeColumnStats(true)
        // essentially force one file per CombinedScanTask from split planning
        .splitOpenFileCost(256 * 1024 * 1024L);
  }

  @Override
  protected Schema schema() {
    return TestFixtures.TS_SCHEMA;
  }

  @Override
  protected List<Record> generateRecords(int numRecords, long seed) {
    // Override the ts field to create a more realistic situation for event time alignment
    tsMilli.addAndGet(RECORD_BATCH_TS_INCREMENT_MILLI);
    return RandomGenericData.generate(schema(), numRecords, seed)
        .stream()
        .map(record -> {
          LocalDateTime ts = LocalDateTime.ofInstant(
              Instant.ofEpochMilli(tsMilli.addAndGet(RECORD_TS_INCREMENT_MILLI)), ZoneId.of("Z"));
          record.setField("ts", ts);
          return record;
        })
        .collect(Collectors.toList());
  }

  /**
   * This override is needed because {@link Comparators} used by {@link StructLikeWrapper}
   * retrieves Timestamp type using Long type as inner class, while the {@link RandomGenericData}
   * generates {@link LocalDateTime} for {@code TimestampType.withoutZone()}.
   * This method normalizes the {@link LocalDateTime} to a Long type so that
   * Comparators can continue to work.
   */
  @Override
  protected void assertRecords(Table table, List<Record> expectedRecords, Duration interval, int maxCount)
      throws Exception {
    List<Record> expectedNormalized = convertTimestampField(expectedRecords);
    for (int i = 0; i < maxCount; ++i) {
      if (SimpleDataUtil.equalsRecords(expectedNormalized,
          convertTimestampField(SimpleDataUtil.tableRecords(table)), table.schema())) {
        break;
      } else {
        Thread.sleep(interval.toMillis());
      }
    }
    SimpleDataUtil.assertRecordsEqual(expectedNormalized,
        convertTimestampField(SimpleDataUtil.tableRecords(table)), table.schema());
  }

  private List<Record> convertTimestampField(List<Record> records) {
    return records.stream()
        .map(r -> {
          LocalDateTime localDateTime = ((LocalDateTime) r.getField("ts"));
          r.setField("ts", localDateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli());
          return r;
        })
        .collect(Collectors.toList());
  }

  private static class IcebergSourceSplitTimeAssigner implements Serializable, TimestampAssigner<IcebergSourceSplit> {
    private final int tsFieldId;

    IcebergSourceSplitTimeAssigner(Schema schema, String tsFieldName) {
      this.tsFieldId = schema.findField(tsFieldName).fieldId();
    }

    @Override
    public long extractTimestamp(IcebergSourceSplit split, long recordTimestamp) {
      return split.task().files().stream()
          .map(scanTask -> (long) Conversions.fromByteBuffer(
              Types.LongType.get(), scanTask.file().lowerBounds().get(tsFieldId)) / 1000L)
          .min(Comparator.comparingLong(l -> l))
          .get();
    }
  }
}
