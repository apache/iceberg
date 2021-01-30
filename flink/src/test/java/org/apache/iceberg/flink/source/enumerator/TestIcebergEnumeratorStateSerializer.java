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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitStatus;
import org.apache.iceberg.flink.source.split.SplitHelpers;
import org.junit.Assert;
import org.junit.Test;

public class TestIcebergEnumeratorStateSerializer {

  private final IcebergEnumeratorStateSerializer serializer = IcebergEnumeratorStateSerializer.INSTANCE;

  @Test
  public void testEmptySnapshotIdAndPendingSplits() throws Exception {
    final IcebergEnumeratorState enumeratorState = new IcebergEnumeratorState(Collections.emptyMap());
    final byte[] result = serializer.serialize(enumeratorState);
    final IcebergEnumeratorState deserialized = serializer.deserialize(serializer.getVersion(), result);
    Assert.assertEquals(enumeratorState, deserialized);
  }

  @Test
  public void testSomeSnapshotIdAndEmptyPendingSplits() throws Exception {
    final IcebergEnumeratorState enumeratorState = new IcebergEnumeratorState(Optional.of(1L), Collections.emptyMap());
    final byte[] result = serializer.serialize(enumeratorState);
    final IcebergEnumeratorState deserialized = serializer.deserialize(serializer.getVersion(), result);
    Assert.assertEquals(enumeratorState, deserialized);
  }

  @Test
  public void testSomeSnapshotIdAndPendingSplits() throws Exception {
    final List<IcebergSourceSplit> splits = SplitHelpers.createMockedSplits(3);
    final Map<IcebergSourceSplit, IcebergSourceSplitStatus> pendingSplits = new HashMap<>();
    pendingSplits.put(splits.get(0), new IcebergSourceSplitStatus(IcebergSourceSplitStatus.Status.UNASSIGNED));
    pendingSplits.put(splits.get(1), new IcebergSourceSplitStatus(IcebergSourceSplitStatus.Status.ASSIGNED, 1));
    pendingSplits.put(splits.get(2), new IcebergSourceSplitStatus(IcebergSourceSplitStatus.Status.COMPLETED, 1));

    final IcebergEnumeratorState enumeratorState = new IcebergEnumeratorState(Optional.of(1L), pendingSplits);
    final byte[] result = serializer.serialize(enumeratorState);
    final IcebergEnumeratorState deserialized = serializer.deserialize(serializer.getVersion(), result);
    Assert.assertEquals(enumeratorState, deserialized);
  }
}
