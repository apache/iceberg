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
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestIcebergSourceSplitSerializer {

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private final IcebergSourceSplitSerializer serializer = IcebergSourceSplitSerializer.INSTANCE;

  @Test
  public void testLatestVersion() throws Exception {
    serializeAndDeserialize(1, 1);
    serializeAndDeserialize(10, 2);
  }

  private void serializeAndDeserialize(int splitCount, int filesPerSplit) throws Exception {
    final List<IcebergSourceSplit> splits = SplitHelpers
        .createSplitsFromTransientHadoopTable(TEMPORARY_FOLDER, splitCount, filesPerSplit);
    for (IcebergSourceSplit split : splits) {
      byte[] result = serializer.serialize(split);
      IcebergSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), result);
      Assert.assertEquals(split.splitId(), deserialized.splitId());
      Assert.assertEquals(split.position(), deserialized.position());

      byte[] cachedResult = serializer.serialize(split);
      Assert.assertSame(result, cachedResult);
      IcebergSourceSplit deserialized2 = serializer.deserialize(serializer.getVersion(), cachedResult);
      Assert.assertEquals(split.splitId(), deserialized2.splitId());
      Assert.assertEquals(split.position(), deserialized2.position());

      split.updatePosition(0, 100);
      byte[] resultAfterUpdatePosition = serializer.serialize(split);
      // after position change, serialized bytes should have changed
      Assert.assertNotSame(cachedResult, resultAfterUpdatePosition);
      IcebergSourceSplit deserialized3 = serializer.deserialize(serializer.getVersion(), resultAfterUpdatePosition);
      Assert.assertEquals(split.splitId(), deserialized3.splitId());
      Assert.assertEquals(split.position(), deserialized3.position());
    }
  }

  @Test
  public void testV1() throws Exception {
    serializeAndDeserializeV1(1, 1);
    serializeAndDeserializeV1(10, 2);
  }

  private void serializeAndDeserializeV1(int splitCount, int filesPerSplit) throws Exception {
    final List<IcebergSourceSplit> splits = SplitHelpers
        .createSplitsFromTransientHadoopTable(TEMPORARY_FOLDER, splitCount, filesPerSplit);
    for (IcebergSourceSplit split : splits) {
      byte[] result = split.serializeV1(split);
      IcebergSourceSplit deserialized = IcebergSourceSplit.deserializeV1(result);
      Assert.assertEquals(split.splitId(), deserialized.splitId());
      Assert.assertEquals(split.position(), deserialized.position());
    }
  }

  @Test
  public void testCheckpointedPosition() throws Exception {
    final AtomicInteger index = new AtomicInteger();
    final List<IcebergSourceSplit> splits = SplitHelpers
        .createSplitsFromTransientHadoopTable(TEMPORARY_FOLDER, 10, 2).stream()
        .map(split -> {
          IcebergSourceSplit result;
          if (index.get() % 2 == 0) {
            result = IcebergSourceSplit.fromCombinedScanTask(split.task(), index.get(), index.get());
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
      Assert.assertEquals(split.splitId(), deserialized.splitId());
      Assert.assertEquals(split.position(), deserialized.position());

      byte[] cachedResult = serializer.serialize(split);
      Assert.assertSame(result, cachedResult);
      IcebergSourceSplit deserialized2 = serializer.deserialize(serializer.getVersion(), cachedResult);
      Assert.assertEquals(split.splitId(), deserialized2.splitId());
      Assert.assertEquals(split.position(), deserialized2.position());
    }
  }
}
