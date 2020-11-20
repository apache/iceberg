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

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.StatefulSequenceSourceTest;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.required;

public class TestStreamingMonitorFunction {

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private static final Schema SCHEMA = new Schema(required(1, "data", Types.StringType.get()));

  private Table table;
  private String location;

  @Before
  public void before() throws IOException {
    File warehouseFile = TEMPORARY_FOLDER.newFolder();
    Assert.assertTrue(warehouseFile.delete());
    location = "file:" + warehouseFile;
    table = new HadoopTables(new Configuration()).create(SCHEMA, location);
  }

  private StreamingMonitorFunction create() {
    return new StreamingMonitorFunction(TableLoader.fromHadoopTable(location), SCHEMA, Collections.emptyList(),
        ScanContext.builder().monitorInterval(Duration.ofMillis(100)).build());
  }

  @Test
  public void testCheckpointRestore() throws Exception {
    GenericAppenderHelper appender = new GenericAppenderHelper(table, FileFormat.AVRO, TEMPORARY_FOLDER);
    final ConcurrentHashMap<String, List<FlinkInputSplit>> outputCollector = new ConcurrentHashMap<>();

    final OneShotLatch latchToTrigger1 = new OneShotLatch();
    final OneShotLatch latchToWait1 = new OneShotLatch();
    StreamingMonitorFunction source1 = create();
    StreamSource<FlinkInputSplit, StreamingMonitorFunction> src1 = new StreamSource<>(source1);
    AbstractStreamOperatorTestHarness<FlinkInputSplit> testHarness1 =
        new AbstractStreamOperatorTestHarness<>(src1, 1, 1, 0);
    testHarness1.setup();
    testHarness1.open();

    // run the source asynchronously
    Thread runner1 = new Thread(() -> {
      try {
        source1.run(new StatefulSequenceSourceTest.BlockingSourceContext<>(
            "1", latchToTrigger1, latchToWait1, outputCollector, 1));
      } catch (Throwable t) {
        throw new RuntimeException(t);
      }
    });

    runner1.start();

    appender.appendToTable(RandomGenericData.generate(SCHEMA, 1, 0L));
    if (!latchToTrigger1.isTriggered()) {
      latchToTrigger1.await();
    }

    OperatorSubtaskState snapshot = AbstractStreamOperatorTestHarness.repackageState(testHarness1.snapshot(0L, 0L));

    final OneShotLatch latchToTrigger2 = new OneShotLatch();
    final OneShotLatch latchToWait2 = new OneShotLatch();
    StreamingMonitorFunction source2 = create();
    StreamSource<FlinkInputSplit, StreamingMonitorFunction> src2 = new StreamSource<>(source2);
    AbstractStreamOperatorTestHarness<FlinkInputSplit> testHarness2 =
        new AbstractStreamOperatorTestHarness<>(src2, 1, 1, 0);
    testHarness2.setup();
    testHarness2.initializeState(snapshot);
    testHarness2.open();
    latchToWait2.trigger();

    // run the source asynchronously
    Thread runner2 = new Thread(() -> {
      try {
        source2.run(new StatefulSequenceSourceTest.BlockingSourceContext<>(
            "2", latchToTrigger2, latchToWait2, outputCollector, 1));
      } catch (Throwable t) {
        throw new RuntimeException(t);
      }
    });
    runner2.start();

    appender.appendToTable(RandomGenericData.generate(SCHEMA, 1, 0L));
    if (!latchToTrigger2.isTriggered()) {
      latchToTrigger2.await();
    }

    Assert.assertEquals(2, outputCollector.size()); // we have 2 tasks.
    Assert.assertEquals(1, outputCollector.get("1").size());
    Assert.assertEquals(1, outputCollector.get("2").size());

    // wait for everybody ot finish.
    latchToWait1.trigger();
    latchToWait2.trigger();
    source1.cancel();
    source2.cancel();
    runner1.join();
    runner2.join();
  }
}
