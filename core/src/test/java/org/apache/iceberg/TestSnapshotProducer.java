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
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;

public class TestSnapshotProducer extends TestBase {

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

  @TestTemplate
  void testCommitValidationFailsOnExistingWapIdInSnapshotHistory() {
    String stagedWapId = "12345";
    table
        .newFastAppend()
        .appendFile(FILE_A)
        .set(SnapshotSummary.STAGED_WAP_ID_PROP, stagedWapId)
        .commit();

    String validationExceptionMessage =
        String.format("Duplicate %s: %s", SnapshotSummary.STAGED_WAP_ID_PROP, stagedWapId);
    TestingSnapshotProducer producer =
        new TestingSnapshotProducer(table.ops())
            .validateWith(new WapIdValidator(stagedWapId, validationExceptionMessage));

    assertThatThrownBy(producer::commit)
        .isInstanceOf(ValidationException.class)
        .hasMessage(validationExceptionMessage);
  }

  @TestTemplate
  void testCommitValidationStartsFromConfiguredSnapshot() {
    String stagedWapId = "12345";
    table
        .newFastAppend()
        .appendFile(TestBase.FILE_A)
        .set(SnapshotSummary.STAGED_WAP_ID_PROP, stagedWapId)
        .commit();

    TestingSnapshotProducer producer =
        new TestingSnapshotProducer(table.ops())
            .validateFromSnapshot(table.currentSnapshot().snapshotId())
            .validateWith(new WapIdValidator(stagedWapId, "empty"));

    assertThatNoException().isThrownBy(producer::commit);
    assertThat(Iterables.size(table.snapshots())).isEqualTo(2);
  }

  private static class WapIdValidator implements Consumer<Snapshot> {
    private final String stagedWapId;
    private final String validationErrorMessage;

    private WapIdValidator(String stagedWapId, String validationErrorMessage) {
      this.stagedWapId = stagedWapId;
      this.validationErrorMessage = validationErrorMessage;
    }

    @Override
    public void accept(Snapshot snapshot) {
      if (stagedWapId.equals(snapshot.summary().get(SnapshotSummary.STAGED_WAP_ID_PROP))) {
        throw new ValidationException(validationErrorMessage);
      }
    }
  }

  private static class TestingSnapshotProducer extends SnapshotProducer<TestingSnapshotProducer> {
    private TestingSnapshotProducer(TableOperations ops) {
      super(ops);
    }

    @Override
    protected TestingSnapshotProducer self() {
      return this;
    }

    @Override
    protected void cleanUncommitted(Set<ManifestFile> committed) {}

    @Override
    protected String operation() {
      return "";
    }

    @Override
    protected List<ManifestFile> apply(TableMetadata metadataToUpdate, Snapshot snapshot) {
      return List.of();
    }

    @Override
    protected Map<String, String> summary() {
      return Map.of();
    }

    @Override
    public TestingSnapshotProducer set(String property, String value) {
      return null;
    }
  }

  private void assertManifestWriterCount(
      int workerPoolSize, int fileCount, int expectedManifestWriterCount, String errMsg) {
    int writerCount = SnapshotProducer.manifestWriterCount(workerPoolSize, fileCount);
    assertThat(writerCount).as(errMsg).isEqualTo(expectedManifestWriterCount);
  }
}
