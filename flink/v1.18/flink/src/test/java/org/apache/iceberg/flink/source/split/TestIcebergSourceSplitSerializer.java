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
package org.apache.iceberg.flink.source.split;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.flink.source.SplitHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestIcebergSourceSplitSerializer {

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private final IcebergSourceSplitSerializer serializer = new IcebergSourceSplitSerializer(true);

  @Test
  public void testLatestVersion() throws Exception {
    serializeAndDeserialize(1, 1);
    serializeAndDeserialize(10, 2);
  }

  private void serializeAndDeserialize(int splitCount, int filesPerSplit) throws Exception {
    final List<IcebergSourceSplit> splits =
        SplitHelpers.createSplitsFromTransientHadoopTable(
            TEMPORARY_FOLDER, splitCount, filesPerSplit);
    for (IcebergSourceSplit split : splits) {
      byte[] result = serializer.serialize(split);
      IcebergSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), result);
      assertSplitEquals(split, deserialized);

      byte[] cachedResult = serializer.serialize(split);
      Assert.assertSame(result, cachedResult);
      IcebergSourceSplit deserialized2 =
          serializer.deserialize(serializer.getVersion(), cachedResult);
      assertSplitEquals(split, deserialized2);

      split.updatePosition(0, 100);
      byte[] resultAfterUpdatePosition = serializer.serialize(split);
      // after position change, serialized bytes should have changed
      Assert.assertNotSame(cachedResult, resultAfterUpdatePosition);
      IcebergSourceSplit deserialized3 =
          serializer.deserialize(serializer.getVersion(), resultAfterUpdatePosition);
      assertSplitEquals(split, deserialized3);
    }
  }

  @Test
  public void testV1() throws Exception {
    serializeAndDeserializeV1(1, 1);
    serializeAndDeserializeV1(10, 2);
  }

  private void serializeAndDeserializeV1(int splitCount, int filesPerSplit) throws Exception {
    final List<IcebergSourceSplit> splits =
        SplitHelpers.createSplitsFromTransientHadoopTable(
            TEMPORARY_FOLDER, splitCount, filesPerSplit);
    for (IcebergSourceSplit split : splits) {
      byte[] result = split.serializeV1();
      IcebergSourceSplit deserialized = IcebergSourceSplit.deserializeV1(result);
      assertSplitEquals(split, deserialized);
    }
  }

  @Test
  public void testV2() throws Exception {
    serializeAndDeserializeV2(1, 1);
    serializeAndDeserializeV2(10, 2);
  }

  private void serializeAndDeserializeV2(int splitCount, int filesPerSplit) throws Exception {
    final List<IcebergSourceSplit> splits =
        SplitHelpers.createSplitsFromTransientHadoopTable(
            TEMPORARY_FOLDER, splitCount, filesPerSplit);
    for (IcebergSourceSplit split : splits) {
      byte[] result = split.serializeV2();
      IcebergSourceSplit deserialized = IcebergSourceSplit.deserializeV2(result, true);
      assertSplitEquals(split, deserialized);
    }
  }

  @Test
  public void testV3WithTooManyDeleteFiles() throws Exception {
    serializeAndDeserializeV3(1, 1, 5000);
  }

  private void serializeAndDeserializeV3(int splitCount, int filesPerSplit, int mockDeletesPerSplit)
      throws Exception {
    final List<IcebergSourceSplit> splits =
        SplitHelpers.createSplitsFromTransientHadoopTable(
            TEMPORARY_FOLDER, splitCount, filesPerSplit);
    final List<IcebergSourceSplit> splitsWithMockDeleteFiles =
        SplitHelpers.equipSplitsWithMockDeleteFiles(splits, TEMPORARY_FOLDER, mockDeletesPerSplit);

    for (IcebergSourceSplit split : splitsWithMockDeleteFiles) {
      byte[] result = split.serializeV3();
      IcebergSourceSplit deserialized = IcebergSourceSplit.deserializeV3(result, true);
      assertSplitEquals(split, deserialized);
    }
  }

  @Test
  public void testDeserializeV1() throws Exception {
    final List<IcebergSourceSplit> splits =
        SplitHelpers.createSplitsFromTransientHadoopTable(TEMPORARY_FOLDER, 1, 1);
    for (IcebergSourceSplit split : splits) {
      byte[] result = split.serializeV1();
      IcebergSourceSplit deserialized = serializer.deserialize(1, result);
      assertSplitEquals(split, deserialized);
    }
  }

  @Test
  public void testCheckpointedPosition() throws Exception {
    final AtomicInteger index = new AtomicInteger();
    final List<IcebergSourceSplit> splits =
        SplitHelpers.createSplitsFromTransientHadoopTable(TEMPORARY_FOLDER, 10, 2).stream()
            .map(
                split -> {
                  IcebergSourceSplit result;
                  if (index.get() % 2 == 0) {
                    result = IcebergSourceSplit.fromCombinedScanTask(split.task(), 1, 1);
                  } else {
                    result = split;
                  }
                  index.incrementAndGet();
                  return result;
                })
            .collect(Collectors.toList());

    for (IcebergSourceSplit split : splits) {
      byte[] result = serializer.serialize(split);
      IcebergSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), result);
      assertSplitEquals(split, deserialized);

      byte[] cachedResult = serializer.serialize(split);
      Assert.assertSame(result, cachedResult);
      IcebergSourceSplit deserialized2 =
          serializer.deserialize(serializer.getVersion(), cachedResult);
      assertSplitEquals(split, deserialized2);
    }
  }

  private void assertSplitEquals(IcebergSourceSplit expected, IcebergSourceSplit actual) {
    List<FileScanTask> expectedTasks = Lists.newArrayList(expected.task().tasks().iterator());
    List<FileScanTask> actualTasks = Lists.newArrayList(actual.task().tasks().iterator());
    Assert.assertEquals(expectedTasks.size(), actualTasks.size());
    for (int i = 0; i < expectedTasks.size(); ++i) {
      FileScanTask expectedTask = expectedTasks.get(i);
      FileScanTask actualTask = actualTasks.get(i);
      Assert.assertEquals(expectedTask.file().path(), actualTask.file().path());
      Assert.assertEquals(expectedTask.sizeBytes(), actualTask.sizeBytes());
      Assert.assertEquals(expectedTask.filesCount(), actualTask.filesCount());
      Assert.assertEquals(expectedTask.start(), actualTask.start());
      Assert.assertEquals(expectedTask.length(), actualTask.length());
    }

    Assert.assertEquals(expected.fileOffset(), actual.fileOffset());
    Assert.assertEquals(expected.recordOffset(), actual.recordOffset());
  }
}
