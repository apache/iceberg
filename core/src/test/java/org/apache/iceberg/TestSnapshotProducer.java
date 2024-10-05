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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class TestSnapshotProducer {

  @Test
  public void testManifestFileGroupSize() {
    assertManifestWriterCount(
        4 /* worker pool size */,
        100 /* file count */,
        1 /* manifest writer count */,
        "Must use 1 writer if file count is small");

    assertManifestWriterCount(
        4 /* worker pool size */,
        SnapshotProducer.MIN_FILE_GROUP_SIZE /* file count */,
        1 /* manifest writer count */,
        "Must use 1 writer if file count matches min group size");

    assertManifestWriterCount(
        4 /* worker pool size */,
        SnapshotProducer.MIN_FILE_GROUP_SIZE + 1 /* file count */,
        1 /* manifest writer count */,
        "Must use 1 writer if file count is slightly above min group size");

    assertManifestWriterCount(
        4 /* worker pool size */,
        (int) (1.25 * SnapshotProducer.MIN_FILE_GROUP_SIZE) /* file count */,
        1 /* manifest writer count */,
        "Must use 1 writer when file count is < 1.5 * min group size");

    assertManifestWriterCount(
        4 /* worker pool size */,
        (int) (1.5 * SnapshotProducer.MIN_FILE_GROUP_SIZE) /* file count */,
        2 /* manifest writer count */,
        "Must use 2 writers when file count is >= 1.5 * min group size");

    assertManifestWriterCount(
        3 /* worker pool size */,
        100 * SnapshotProducer.MIN_FILE_GROUP_SIZE /* file count */,
        3 /* manifest writer count */,
        "Must limit parallelism to worker pool size when file count is large");

    assertManifestWriterCount(
        32 /* worker pool size */,
        5 * SnapshotProducer.MIN_FILE_GROUP_SIZE /* file count */,
        5 /* manifest writer count */,
        "Must limit parallelism to avoid tiny manifests");
  }

  private void assertManifestWriterCount(
      int workerPoolSize, int fileCount, int expectedManifestWriterCount, String errMsg) {
    int writerCount = SnapshotProducer.manifestWriterCount(workerPoolSize, fileCount);
    assertThat(writerCount).withFailMessage(errMsg).isEqualTo(expectedManifestWriterCount);
  }
}
