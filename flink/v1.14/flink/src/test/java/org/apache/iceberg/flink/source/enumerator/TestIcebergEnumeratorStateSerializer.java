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

package org.apache.iceberg.flink.source.enumerator;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.flink.source.SplitHelpers;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitState;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitStatus;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestIcebergEnumeratorStateSerializer {
  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private final IcebergEnumeratorStateSerializer serializer = IcebergEnumeratorStateSerializer.INSTANCE;

  @Test
  public void testEmptySnapshotIdAndPendingSplits() throws Exception {
    IcebergEnumeratorState enumeratorState = new IcebergEnumeratorState(Collections.emptyList());
    byte[] result = serializer.serialize(enumeratorState);
    IcebergEnumeratorState deserialized = serializer.deserialize(serializer.getVersion(), result);
    assertEnumeratorStateEquals(enumeratorState, deserialized);
  }

  @Test
  public void testSomeSnapshotIdAndEmptyPendingSplits() throws Exception {
    IcebergEnumeratorPosition position = IcebergEnumeratorPosition.of(1L, System.currentTimeMillis());
    IcebergEnumeratorState enumeratorState = new IcebergEnumeratorState(position, Collections.emptyList());
    byte[] result = serializer.serialize(enumeratorState);
    IcebergEnumeratorState deserialized = serializer.deserialize(serializer.getVersion(), result);
    assertEnumeratorStateEquals(enumeratorState, deserialized);
  }

  @Test
  public void testSomeSnapshotIdAndPendingSplits() throws Exception {
    IcebergEnumeratorPosition position = IcebergEnumeratorPosition.of(2L, System.currentTimeMillis());
    List<IcebergSourceSplit> splits = SplitHelpers
        .createSplitsFromTransientHadoopTable(TEMPORARY_FOLDER, 3, 1);
    Collection<IcebergSourceSplitState> pendingSplits = Lists.newArrayList();
    pendingSplits.add(new IcebergSourceSplitState(splits.get(0), IcebergSourceSplitStatus.UNASSIGNED));
    pendingSplits.add(new IcebergSourceSplitState(splits.get(1), IcebergSourceSplitStatus.ASSIGNED));
    pendingSplits.add(new IcebergSourceSplitState(splits.get(2), IcebergSourceSplitStatus.COMPLETED));

    IcebergEnumeratorState enumeratorState = new IcebergEnumeratorState(position, pendingSplits);
    byte[] result = serializer.serialize(enumeratorState);
    IcebergEnumeratorState deserialized = serializer.deserialize(serializer.getVersion(), result);
    assertEnumeratorStateEquals(enumeratorState, deserialized);
  }

  private void assertEnumeratorStateEquals(IcebergEnumeratorState expected, IcebergEnumeratorState actual) {
    Assert.assertEquals(expected.lastEnumeratedPosition(), actual.lastEnumeratedPosition());
    Assert.assertEquals(expected.pendingSplits().size(), actual.pendingSplits().size());
    Iterator<IcebergSourceSplitState> expectedIterator = expected.pendingSplits().iterator();
    Iterator<IcebergSourceSplitState> actualIterator = actual.pendingSplits().iterator();
    for (int i = 0; i < expected.pendingSplits().size(); ++i) {
      IcebergSourceSplitState expectedSplitState = expectedIterator.next();
      IcebergSourceSplitState actualSplitState = actualIterator.next();
      Assert.assertEquals(expectedSplitState.split().splitId(), actualSplitState.split().splitId());
      Assert.assertEquals(expectedSplitState.split().fileOffset(), actualSplitState.split().fileOffset());
      Assert.assertEquals(expectedSplitState.split().recordOffset(), actualSplitState.split().recordOffset());
      Assert.assertEquals(expectedSplitState.status(), actualSplitState.status());
    }
  }
}
