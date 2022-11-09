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
package org.apache.iceberg.flink.source.assigner;

import java.util.List;
import org.apache.iceberg.flink.source.SplitHelpers;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestLocalitySplitAssigner extends TestSplitAssignerBase {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Test
  public void testEmptyInitialization() {
    LocalitySplitAssigner assigner = new LocalitySplitAssigner();
    assertSnapshot(assigner, 0);
  }

  /** Test a sequence of interactions for StaticEnumerator */
  @Test
  public void testStaticEnumeratorSequence() throws Exception {
    LocalitySplitAssigner assigner = new LocalitySplitAssigner();
    assigner.onDiscoveredSplits(
        SplitHelpers.createSplitsFromTransientHadoopTable(
            TEMPORARY_FOLDER, 4, 2, new String[] {"host1"}));

    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE, "host1");
    assertSnapshot(assigner, 1);
    assigner.onUnassignedSplits(
        SplitHelpers.createSplitsFromTransientHadoopTable(
            TEMPORARY_FOLDER, 1, 1, new String[] {"host2"}));

    assertSnapshot(assigner, 2);

    assertGetNext(
        assigner, GetSplitResult.Status.AVAILABLE, "host1", Sets.newHashSet("host1"), "host2");
    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE, "host2");

    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE, "host2");
    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE, "host1");
    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE, "unknown");
    assertSnapshot(assigner, 0);
  }

  /** Test a sequence of interactions for ContinuousEnumerator */
  @Test
  public void testContinuousEnumeratorSequence() throws Exception {
    LocalitySplitAssigner assigner = new LocalitySplitAssigner();
    List<IcebergSourceSplit> splits1 =
        SplitHelpers.createSplitsFromTransientHadoopTable(
            TEMPORARY_FOLDER, 1, 1, new String[] {"host1"});
    assertAvailableFuture(assigner, 1, () -> assigner.onDiscoveredSplits(splits1), "host1");
    List<IcebergSourceSplit> splits2 =
        SplitHelpers.createSplitsFromTransientHadoopTable(
            TEMPORARY_FOLDER, 1, 1, new String[] {"host2"});
    assertAvailableFuture(assigner, 1, () -> assigner.onUnassignedSplits(splits2), "host2");

    assigner.onDiscoveredSplits(
        SplitHelpers.createSplitsFromTransientHadoopTable(
            TEMPORARY_FOLDER, 2, 1, new String[] {"host3"}));
    assigner.onDiscoveredSplits(
        SplitHelpers.createSplitsFromTransientHadoopTable(
            TEMPORARY_FOLDER, 2, 1, new String[] {"host4"}));
    assertSnapshot(assigner, 4);
    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE, "host3");
    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE, "host4");
    assertGetNext(
        assigner, GetSplitResult.Status.AVAILABLE, "host4", Sets.newHashSet("host4"), "host3");
    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE, "host3");
    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE, "host1");
    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE, "unknown");
    assertSnapshot(assigner, 0);
  }

  @Test
  public void testMultipleHostnames() throws Exception {
    LocalitySplitAssigner assigner = new LocalitySplitAssigner();
    assigner.onDiscoveredSplits(
        SplitHelpers.createSplitsFromTransientHadoopTable(
            TEMPORARY_FOLDER, 1, 1, new String[] {"host1", "host2"}));
    assertSnapshot(assigner, 1);
    assertGetNext(
        assigner, GetSplitResult.Status.AVAILABLE, "host1", Sets.newHashSet("host1", "host2"));

    assigner.onDiscoveredSplits(
        SplitHelpers.createSplitsFromTransientHadoopTable(
            TEMPORARY_FOLDER, 1, 1, new String[] {"host1", "host3"}));
    assigner.onDiscoveredSplits(
        SplitHelpers.createSplitsFromTransientHadoopTable(
            TEMPORARY_FOLDER, 1, 1, new String[] {"host3", "host4"}));
    assertSnapshot(assigner, 2);
    assertGetNext(
        assigner, GetSplitResult.Status.AVAILABLE, "host1", Sets.newHashSet("host1", "host3"));
    assertGetNext(
        assigner, GetSplitResult.Status.AVAILABLE, "host4", Sets.newHashSet("host3", "host4"));

    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE, "host2");
    assertSnapshot(assigner, 0);
  }

  @Test
  public void testEmptyHostnames() throws Exception {
    LocalitySplitAssigner assigner = new LocalitySplitAssigner();
    assigner.onDiscoveredSplits(
        SplitHelpers.createSplitsFromTransientHadoopTable(
            TEMPORARY_FOLDER, 1, 1, new String[] {"host1", "host2"}));
    assigner.onDiscoveredSplits(
        SplitHelpers.createSplitsFromTransientHadoopTable(TEMPORARY_FOLDER, 1, 1, new String[] {}));

    assertSnapshot(assigner, 2);
    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE, "host1");
    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE, "host2", Sets.newHashSet());

    assigner.onDiscoveredSplits(
        SplitHelpers.createSplitsFromTransientHadoopTable(TEMPORARY_FOLDER, 1, 1, null));

    assertSnapshot(assigner, 1);
    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE, "host1", null);
    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE, "host1");
    assertSnapshot(assigner, 0);
  }

  @Test
  public void testFailedNullHost() {
    LocalitySplitAssigner assigner = new LocalitySplitAssigner();

    Assert.assertThrows(
        "hostname should not be null",
        IllegalArgumentException.class,
        () -> assigner.getNext(null));
  }
}
