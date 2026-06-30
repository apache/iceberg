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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.flink.source.SplitHelpers;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitState;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitStatus;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestIcebergEnumeratorStateSerializer {

  @TempDir protected Path temporaryFolder;

  private final IcebergEnumeratorStateSerializer serializer =
      new IcebergEnumeratorStateSerializer(true);

  @Parameter(index = 0)
  protected int version;

  @Parameters(name = "version={0}")
  public static Object[][] parameters() {
    return new Object[][] {
      new Object[] {1}, new Object[] {2}, new Object[] {3},
    };
  }

  @TestTemplate
  public void testEmptySnapshotIdAndPendingSplits() throws Exception {
    IcebergEnumeratorState enumeratorState = new IcebergEnumeratorState(Collections.emptyList());
    testSerializer(enumeratorState);
  }

  @TestTemplate
  public void testSomeSnapshotIdAndEmptyPendingSplits() throws Exception {
    IcebergEnumeratorPosition position =
        IcebergEnumeratorPosition.of(1L, System.currentTimeMillis());

    IcebergEnumeratorState enumeratorState =
        new IcebergEnumeratorState(position, Collections.emptyList());
    testSerializer(enumeratorState);
  }

  @TestTemplate
  public void testSomeSnapshotIdAndPendingSplits() throws Exception {
    IcebergEnumeratorPosition position =
        IcebergEnumeratorPosition.of(2L, System.currentTimeMillis());
    List<IcebergSourceSplit> splits =
        SplitHelpers.createSplitsFromTransientHadoopTable(temporaryFolder, 3, 1);
    Collection<IcebergSourceSplitState> pendingSplits = Lists.newArrayList();
    pendingSplits.add(
        new IcebergSourceSplitState(splits.get(0), IcebergSourceSplitStatus.UNASSIGNED));
    pendingSplits.add(
        new IcebergSourceSplitState(splits.get(1), IcebergSourceSplitStatus.ASSIGNED));
    pendingSplits.add(
        new IcebergSourceSplitState(splits.get(2), IcebergSourceSplitStatus.COMPLETED));

    IcebergEnumeratorState enumeratorState = new IcebergEnumeratorState(position, pendingSplits);
    testSerializer(enumeratorState);
  }

  @TestTemplate
  public void testEnumerationSplitCountHistory() throws Exception {
    if (version >= 2) {
      IcebergEnumeratorPosition position =
          IcebergEnumeratorPosition.of(2L, System.currentTimeMillis());
      List<IcebergSourceSplit> splits =
          SplitHelpers.createSplitsFromTransientHadoopTable(temporaryFolder, 3, 1);
      Collection<IcebergSourceSplitState> pendingSplits = Lists.newArrayList();
      pendingSplits.add(
          new IcebergSourceSplitState(splits.get(0), IcebergSourceSplitStatus.UNASSIGNED));
      pendingSplits.add(
          new IcebergSourceSplitState(splits.get(1), IcebergSourceSplitStatus.ASSIGNED));
      pendingSplits.add(
          new IcebergSourceSplitState(splits.get(2), IcebergSourceSplitStatus.COMPLETED));
      int[] enumerationSplitCountHistory = {1, 2, 3};

      IcebergEnumeratorState enumeratorState =
          new IcebergEnumeratorState(position, pendingSplits, enumerationSplitCountHistory);
      testSerializer(enumeratorState);
    }
  }

  private void testSerializer(IcebergEnumeratorState enumeratorState) throws IOException {
    byte[] result;
    switch (version) {
      case 1:
        result = serializer.serializeV1(enumeratorState);
        break;
      case 2:
        result = serializer.serializeV2(enumeratorState);
        break;
      case 3:
        result = serializer.serialize(enumeratorState);
        break;
      default:
        throw new IllegalArgumentException("Unsupported version: " + version);
    }

    IcebergEnumeratorState deserialized = serializer.deserialize(version, result);
    assertEnumeratorStateEquals(enumeratorState, deserialized);
  }

  private void assertEnumeratorStateEquals(
      IcebergEnumeratorState expected, IcebergEnumeratorState actual) {
    assertThat(actual.lastEnumeratedPosition()).isEqualTo(expected.lastEnumeratedPosition());

    assertThat(actual.pendingSplits()).hasSameSizeAs(expected.pendingSplits());
    Iterator<IcebergSourceSplitState> expectedIterator = expected.pendingSplits().iterator();
    Iterator<IcebergSourceSplitState> actualIterator = actual.pendingSplits().iterator();
    for (int i = 0; i < expected.pendingSplits().size(); ++i) {
      IcebergSourceSplitState expectedSplitState = expectedIterator.next();
      IcebergSourceSplitState actualSplitState = actualIterator.next();
      assertThat(actualSplitState.split().splitId())
          .isEqualTo(expectedSplitState.split().splitId());
      assertThat(actualSplitState.split().fileOffset())
          .isEqualTo(expectedSplitState.split().fileOffset());
      assertThat(actualSplitState.split().recordOffset())
          .isEqualTo(expectedSplitState.split().recordOffset());
      assertThat(actualSplitState.status()).isEqualTo(expectedSplitState.status());
    }

    assertThat(actual.enumerationSplitCountHistory())
        .containsExactly(expected.enumerationSplitCountHistory());

    if (expected.lazyBulkScanCursor() == null) {
      assertThat(actual.lazyBulkScanCursor()).isNull();
    } else {
      assertThat(actual.lazyBulkScanCursor()).isNotNull();
      assertThat(actual.lazyBulkScanCursor().bulkSnapshotId())
          .isEqualTo(expected.lazyBulkScanCursor().bulkSnapshotId());
      assertThat(actual.lazyBulkScanCursor().combinedTasksEnumerated())
          .isEqualTo(expected.lazyBulkScanCursor().combinedTasksEnumerated());
      assertThat(actual.lazyBulkScanCursor().rollingHash())
          .isEqualTo(expected.lazyBulkScanCursor().rollingHash());
    }
  }

  // ----------------------------------------------------------------------------------------
  // v3-only tests for the lazy bulk-scan cursor

  @TestTemplate
  public void testLazyBulkScanCursorRoundTrip() throws Exception {
    if (version != 3) {
      return;
    }
    IcebergEnumeratorPosition position =
        IcebergEnumeratorPosition.of(2L, System.currentTimeMillis());
    LazyBulkScanCursor cursor = new LazyBulkScanCursor(1234567890L, 42L, 0xCAFEBABEDEADBEEFL);
    IcebergEnumeratorState enumeratorState =
        new IcebergEnumeratorState(position, Collections.emptyList(), new int[0], cursor);
    testSerializer(enumeratorState);
  }

  @TestTemplate
  public void testLazyBulkScanCursorAbsent() throws Exception {
    if (version != 3) {
      return;
    }
    // v3 state without a cursor should round-trip with cursor=null (lazy planner not in use, or
    // already past the bulk phase).
    IcebergEnumeratorPosition position =
        IcebergEnumeratorPosition.of(3L, System.currentTimeMillis());
    IcebergEnumeratorState enumeratorState =
        new IcebergEnumeratorState(position, Collections.emptyList(), new int[0], null);
    testSerializer(enumeratorState);
  }

  @TestTemplate
  public void testV2DeserializedAsV3HasNullCursor() throws Exception {
    if (version != 3) {
      return;
    }
    // Write with v2, read with the v3-aware deserializer.
    IcebergEnumeratorPosition position =
        IcebergEnumeratorPosition.of(4L, System.currentTimeMillis());
    IcebergEnumeratorState v2State = new IcebergEnumeratorState(position, Collections.emptyList());

    byte[] v2Bytes = serializer.serializeV2(v2State);
    IcebergEnumeratorState restored = serializer.deserialize(2, v2Bytes);
    assertThat(restored.lazyBulkScanCursor()).isNull();
  }
}
