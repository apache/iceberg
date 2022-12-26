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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestCustomValidations extends V2TableTestBase {

  private final Validation alwaysPassValidation =
      new Validation(currentTable -> true, "Always pass.");

  private final String alwaysFailMessage = "Always fail.";
  private final Validation alwaysFailValidation =
      new Validation(currentTable -> false, alwaysFailMessage);

  private final String watermarkKey = "watermark";

  private void setWatermarkProperty(Table table, int watermarkValue) {
    table.updateProperties().set(watermarkKey, Integer.toString(watermarkValue)).commit();
  }

  private final String watermarkFailMessagePattern =
      "Current watermark value not equal to expected value=%s";

  private Validation watermarkValidation(int expectedValue) {
    return new Validation(
        currentTable ->
            Objects.equals(
                currentTable.properties().get(watermarkKey), Integer.toString(expectedValue)),
        watermarkFailMessagePattern,
        expectedValue);
  }

  private final Validation illegalValidation =
      new Validation(
          currentTable -> {
            // illegal table modification inside validation predicate
            currentTable.updateProperties().set(watermarkKey, Integer.toString(0)).commit();
            return true;
          },
          "Predicate returned false.");

  @TestTemplate
  public void testCherryPickPassesValidation() {
    table.newAppend().appendFile(FILE_A).commit();
    table.newOverwrite().deleteFile(FILE_A).addFile(FILE_B).stageOnly().commit();
    long overwriteSnapshotId =
        Streams.stream(table.snapshots())
            .filter(snap -> DataOperations.OVERWRITE.equals(snap.operation()))
            .findFirst()
            .get()
            .snapshotId();
    validateTableFiles(table, FILE_A);

    new CherryPickOperation(table.name(), table.operations())
        .cherrypick(overwriteSnapshotId)
        .commitIf(ImmutableList.of(alwaysPassValidation));

    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(overwriteSnapshotId);
    validateTableFiles(table, FILE_B);
  }

  @TestTemplate
  public void testCherryPickFailsValidation() {
    table.newAppend().appendFile(FILE_A).commit();
    long firstSnapshotId = table.currentSnapshot().snapshotId();
    table.newOverwrite().deleteFile(FILE_A).addFile(FILE_B).stageOnly().commit();
    long overwriteSnapshotId =
        Streams.stream(table.snapshots())
            .filter(snap -> DataOperations.OVERWRITE.equals(snap.operation()))
            .findFirst()
            .get()
            .snapshotId();
    validateTableFiles(table, FILE_A);

    assertThatThrownBy(
            () ->
                new CherryPickOperation(table.name(), table.operations())
                    .cherrypick(overwriteSnapshotId)
                    .commitIf(ImmutableList.of(alwaysFailValidation)))
        .isInstanceOf(ValidationException.class)
        .hasMessage(alwaysFailMessage);

    assertThat(firstSnapshotId).isEqualTo(table.currentSnapshot().snapshotId());
    validateTableFiles(table, FILE_A);
  }

  @TestTemplate
  public void testCherryPickFailsValidationDueToConcurrentCommit() {
    table.newAppend().appendFile(FILE_A).commit();
    long firstSnapshotId = table.currentSnapshot().snapshotId();
    table.newOverwrite().deleteFile(FILE_A).addFile(FILE_B).stageOnly().commit();
    long overwriteSnapshotId =
        Streams.stream(table.snapshots())
            .filter(snap -> DataOperations.OVERWRITE.equals(snap.operation()))
            .findFirst()
            .get()
            .snapshotId();
    validateTableFiles(table, FILE_A);

    setWatermarkProperty(table, 0);

    CherryPickOperation pendingUpdate =
        new CherryPickOperation(table.name(), table.operations()).cherrypick(overwriteSnapshotId);

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(() -> pendingUpdate.commitIf(ImmutableList.of(watermarkValidation(0))))
        .isInstanceOf(ValidationException.class)
        .hasMessage(watermarkFailMessagePattern, 0);

    assertThat(firstSnapshotId).isEqualTo(table.currentSnapshot().snapshotId());
    validateTableFiles(table, FILE_A);
  }

  @TestTemplate
  public void testCherryPickFailsDueToIllegalTableModificationInsideValidation() {
    table.newAppend().appendFile(FILE_A).commit();
    long firstSnapshotId = table.currentSnapshot().snapshotId();
    table.newOverwrite().deleteFile(FILE_A).addFile(FILE_B).stageOnly().commit();
    long overwriteSnapshotId =
        Streams.stream(table.snapshots())
            .filter(snap -> DataOperations.OVERWRITE.equals(snap.operation()))
            .findFirst()
            .get()
            .snapshotId();
    validateTableFiles(table, FILE_A);

    assertThatThrownBy(
            () ->
                new CherryPickOperation(table.name(), table.operations())
                    .cherrypick(overwriteSnapshotId)
                    .commitIf(ImmutableList.of(illegalValidation)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    assertThat(firstSnapshotId).isEqualTo(table.currentSnapshot().snapshotId());
    validateTableFiles(table, FILE_A);
  }

  @TestTemplate
  public void testDeleteFilesPassesValidation() {
    table.newFastAppend().appendFile(FILE_A).commit();
    validateTableFiles(table, FILE_A);

    table.newDelete().deleteFile(FILE_A).commitIf(ImmutableList.of(alwaysPassValidation));

    validateTableFiles(table);
  }

  @TestTemplate
  public void testDeleteFilesFailsValidation() {
    table.newFastAppend().appendFile(FILE_A).commit();
    validateTableFiles(table, FILE_A);

    assertThatThrownBy(
            () ->
                table
                    .newDelete()
                    .deleteFile(FILE_A)
                    .commitIf(ImmutableList.of(alwaysFailValidation)))
        .isInstanceOf(ValidationException.class)
        .hasMessage(alwaysFailMessage);

    validateTableFiles(table, FILE_A);
  }

  @TestTemplate
  public void testDeleteFilesFailsValidationDueToConcurrentCommit() {
    table.newFastAppend().appendFile(FILE_A).commit();
    validateTableFiles(table, FILE_A);

    setWatermarkProperty(table, 0);

    PendingUpdate<?> pendingUpdate = table.newDelete().deleteFile(FILE_A);

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(() -> pendingUpdate.commitIf(ImmutableList.of(watermarkValidation(0))))
        .isInstanceOf(ValidationException.class)
        .hasMessage(watermarkFailMessagePattern, 0);

    validateTableFiles(table, FILE_A);
  }

  @TestTemplate
  public void testDeleteFilesFailsDueToIllegalTableModificationInsideValidation() {
    table.newFastAppend().appendFile(FILE_A).commit();
    validateTableFiles(table, FILE_A);

    assertThatThrownBy(
            () ->
                table.newDelete().deleteFile(FILE_A).commitIf(ImmutableList.of(illegalValidation)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    validateTableFiles(table, FILE_A);
  }

  @TestTemplate
  public void testExpireSnapshotsPassesValidation() {
    table.newAppend().appendFile(FILE_A).commit();
    Snapshot firstSnapshot = table.currentSnapshot();
    table.newAppend().appendFile(FILE_B).commit();
    Set<String> deletedFiles = Sets.newHashSet();

    table
        .expireSnapshots()
        .expireSnapshotId(firstSnapshot.snapshotId())
        .deleteWith(deletedFiles::add)
        .commitIf(ImmutableList.of(alwaysPassValidation));

    assertThat(deletedFiles)
        .as("Should remove the expired manifest list location")
        .containsExactly(firstSnapshot.manifestListLocation());
  }

  @TestTemplate
  public void testExpireSnapshotsFailsValidation() {
    table.newAppend().appendFile(FILE_A).commit();
    Snapshot firstSnapshot = table.currentSnapshot();
    table.newAppend().appendFile(FILE_B).commit();
    Set<String> deletedFiles = Sets.newHashSet();

    assertThatThrownBy(
            () ->
                table
                    .expireSnapshots()
                    .expireSnapshotId(firstSnapshot.snapshotId())
                    .deleteWith(deletedFiles::add)
                    .commitIf(ImmutableList.of(alwaysFailValidation)))
        .isInstanceOf(ValidationException.class)
        .hasMessage(alwaysFailMessage);

    assertThat(deletedFiles).isEmpty();
  }

  @TestTemplate
  public void testExpireSnapshotsFailsValidationDueToConcurrentCommit() {
    table.newAppend().appendFile(FILE_A).commit();
    Snapshot firstSnapshot = table.currentSnapshot();
    table.newAppend().appendFile(FILE_B).commit();
    Set<String> deletedFiles = Sets.newHashSet();

    setWatermarkProperty(table, 0);

    PendingUpdate<?> pendingUpdate =
        table
            .expireSnapshots()
            .expireSnapshotId(firstSnapshot.snapshotId())
            .deleteWith(deletedFiles::add);

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(() -> pendingUpdate.commitIf(ImmutableList.of(watermarkValidation(0))))
        .isInstanceOf(ValidationException.class)
        .hasMessage(watermarkFailMessagePattern, 0);

    assertThat(deletedFiles).isEmpty();
  }

  @TestTemplate
  public void testExpireSnapshotsFailsDueToIllegalTableModificationInsideValidation() {
    table.newAppend().appendFile(FILE_A).commit();
    Snapshot firstSnapshot = table.currentSnapshot();
    table.newAppend().appendFile(FILE_B).commit();
    Set<String> deletedFiles = Sets.newHashSet();

    assertThatThrownBy(
            () ->
                table
                    .expireSnapshots()
                    .expireSnapshotId(firstSnapshot.snapshotId())
                    .deleteWith(deletedFiles::add)
                    .commitIf(ImmutableList.of(illegalValidation)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    assertThat(deletedFiles).isEmpty();
  }

  @TestTemplate
  public void testFastAppendPassesValidation() {
    validateTableFiles(table);

    table.newFastAppend().appendFile(FILE_A).commitIf(ImmutableList.of(alwaysPassValidation));

    validateTableFiles(table, FILE_A);
  }

  @TestTemplate
  public void testFastAppendFailsValidation() {
    validateTableFiles(table);

    assertThatThrownBy(
            () ->
                table
                    .newFastAppend()
                    .appendFile(FILE_A)
                    .commitIf(ImmutableList.of(alwaysFailValidation)))
        .isInstanceOf(ValidationException.class)
        .hasMessage(alwaysFailMessage);

    validateTableFiles(table);
  }

  @TestTemplate
  public void testFastAppendFailsValidationDueToConcurrentCommit() {
    validateTableFiles(table);

    setWatermarkProperty(table, 0);

    PendingUpdate<?> pendingUpdate = table.newFastAppend().appendFile(FILE_A);

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(() -> pendingUpdate.commitIf(ImmutableList.of(watermarkValidation(0))))
        .isInstanceOf(ValidationException.class)
        .hasMessage(watermarkFailMessagePattern, 0);

    validateTableFiles(table);
  }

  @TestTemplate
  public void testFastAppendFailsDueToIllegalTableModificationInsideValidation() {
    validateTableFiles(table);

    assertThatThrownBy(
            () ->
                table
                    .newFastAppend()
                    .appendFile(FILE_A)
                    .commitIf(ImmutableList.of(illegalValidation)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    validateTableFiles(table);
  }

  @TestTemplate
  public void testManageSnapshotsPassesValidation() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    String tagName = "tag1";
    assertThat(table.refs().get(tagName)).isNull();

    table
        .manageSnapshots()
        .createTag(tagName, snapshotId)
        .commitIf(ImmutableList.of(alwaysPassValidation));

    assertThat(table.refs().get(tagName))
        .isNotNull()
        .isEqualTo(SnapshotRef.tagBuilder(snapshotId).build());
  }

  @TestTemplate
  public void testManageSnapshotsFailsValidation() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    String tagName = "tag1";
    assertThat(table.refs().get(tagName)).isNull();

    assertThatThrownBy(
            () ->
                table
                    .manageSnapshots()
                    .createTag(tagName, snapshotId)
                    .commitIf(ImmutableList.of(alwaysFailValidation)))
        .isInstanceOf(ValidationException.class)
        .hasMessage(alwaysFailMessage);

    assertThat(table.refs().get(tagName)).isNull();
  }

  @TestTemplate
  public void testManageSnapshotsFailsDueToConcurrentCommit() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    String tagName = "tag1";
    assertThat(table.refs().get(tagName)).isNull();

    setWatermarkProperty(table, 0);

    ManageSnapshots pendingUpdate = table.manageSnapshots().createTag(tagName, snapshotId);

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(() -> pendingUpdate.commitIf(ImmutableList.of(watermarkValidation(0))))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Table metadata refresh is required");

    assertThat(table.refs().get(tagName)).isNull();
  }

  @TestTemplate
  public void testManageSnapshotsFailsDueToIllegalTableModificationInsideValidation() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    String tagName = "tag1";
    assertThat(table.refs().get(tagName)).isNull();

    assertThatThrownBy(
            () ->
                table
                    .manageSnapshots()
                    .createTag(tagName, snapshotId)
                    .commitIf(ImmutableList.of(illegalValidation)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    assertThat(table.refs().get(tagName)).isNull();
  }

  @TestTemplate
  public void testMergeAppendPassesValidation() {
    validateTableFiles(table);

    table.newAppend().appendFile(FILE_A).commitIf(ImmutableList.of(alwaysPassValidation));

    validateTableFiles(table, FILE_A);
  }

  @TestTemplate
  public void testMergeAppendFailsValidation() {
    validateTableFiles(table);

    assertThatThrownBy(
            () ->
                table
                    .newAppend()
                    .appendFile(FILE_A)
                    .commitIf(ImmutableList.of(alwaysFailValidation)))
        .isInstanceOf(ValidationException.class)
        .hasMessage(alwaysFailMessage);

    validateTableFiles(table);
  }

  @TestTemplate
  public void testMergeAppendFailsValidationDueToConcurrentCommit() {
    validateTableFiles(table);

    setWatermarkProperty(table, 0);

    AppendFiles pendingUpdate = table.newAppend().appendFile(FILE_A);

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(() -> pendingUpdate.commitIf(ImmutableList.of(watermarkValidation(0))))
        .isInstanceOf(ValidationException.class)
        .hasMessage(watermarkFailMessagePattern, 0);

    validateTableFiles(table);
  }

  @TestTemplate
  public void testMergeAppendFailsDueToIllegalTableModificationInsideValidation() {
    validateTableFiles(table);

    assertThatThrownBy(
            () ->
                table.newAppend().appendFile(FILE_A).commitIf(ImmutableList.of(illegalValidation)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    validateTableFiles(table);
  }

  @TestTemplate
  public void testOverwritePassesValidation() {
    table.newFastAppend().appendFile(FILE_A).commit();
    validateTableFiles(table, FILE_A);

    table
        .newOverwrite()
        .overwriteByRowFilter(Expressions.alwaysTrue())
        .addFile(FILE_B)
        .commitIf(ImmutableList.of(alwaysPassValidation));

    validateTableFiles(table, FILE_B);
  }

  @TestTemplate
  public void testOverwriteFailsValidation() {
    table.newFastAppend().appendFile(FILE_A).commit();
    validateTableFiles(table, FILE_A);

    assertThatThrownBy(
            () ->
                table
                    .newOverwrite()
                    .overwriteByRowFilter(Expressions.alwaysTrue())
                    .addFile(FILE_B)
                    .commitIf(ImmutableList.of(alwaysFailValidation)))
        .isInstanceOf(ValidationException.class)
        .hasMessage(alwaysFailMessage);

    validateTableFiles(table, FILE_A);
  }

  @TestTemplate
  public void testOverwriteFailsValidationDueToConcurrentCommit() {
    table.newFastAppend().appendFile(FILE_A).commit();
    validateTableFiles(table, FILE_A);

    setWatermarkProperty(table, 0);

    OverwriteFiles pendingUpdate =
        table.newOverwrite().overwriteByRowFilter(Expressions.alwaysTrue()).addFile(FILE_B);

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(() -> pendingUpdate.commitIf(ImmutableList.of(watermarkValidation(0))))
        .isInstanceOf(ValidationException.class)
        .hasMessage(watermarkFailMessagePattern, 0);

    validateTableFiles(table, FILE_A);
  }

  @TestTemplate
  public void testOverwriteFailsDueToIllegalTableModificationInsideValidation() {
    table.newFastAppend().appendFile(FILE_A).commit();
    validateTableFiles(table, FILE_A);

    assertThatThrownBy(
            () ->
                table
                    .newOverwrite()
                    .overwriteByRowFilter(Expressions.alwaysTrue())
                    .addFile(FILE_B)
                    .commitIf(ImmutableList.of(illegalValidation)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    validateTableFiles(table, FILE_A);
  }

  @TestTemplate
  public void testReplacePartitionsPassesValidation() {
    validateTableFiles(table);

    table.newReplacePartitions().addFile(FILE_A).commitIf(ImmutableList.of(alwaysPassValidation));

    validateTableFiles(table, FILE_A);
  }

  @TestTemplate
  public void testReplacePartitionsFailsValidation() {
    validateTableFiles(table);

    assertThatThrownBy(
            () ->
                table
                    .newReplacePartitions()
                    .addFile(FILE_A)
                    .commitIf(ImmutableList.of(alwaysFailValidation)))
        .isInstanceOf(ValidationException.class)
        .hasMessage(alwaysFailMessage);

    validateTableFiles(table);
  }

  @TestTemplate
  public void testReplacePartitionsFailsValidationDueToConcurrentCommit() {
    validateTableFiles(table);

    setWatermarkProperty(table, 0);

    ReplacePartitions pendingUpdate = table.newReplacePartitions().addFile(FILE_A).addFile(FILE_B);

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(() -> pendingUpdate.commitIf(ImmutableList.of(watermarkValidation(0))))
        .isInstanceOf(ValidationException.class)
        .hasMessage(watermarkFailMessagePattern, 0);

    validateTableFiles(table);
  }

  @TestTemplate
  public void testReplacePartitionsFailsDueToIllegalTableModificationInsideValidation() {
    validateTableFiles(table);

    assertThatThrownBy(
            () ->
                table
                    .newReplacePartitions()
                    .addFile(FILE_A)
                    .commitIf(ImmutableList.of(illegalValidation)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    validateTableFiles(table);
  }

  @TestTemplate
  public void testReplaceSortOrderPassesValidation() {
    assertThat(table.sortOrder()).isEqualTo(SortOrder.unsorted());

    table.replaceSortOrder().asc("data").commitIf(ImmutableList.of(alwaysPassValidation));

    assertThat(table.sortOrder())
        .as("Table should reflect new sort order")
        .isEqualTo(SortOrder.builderFor(table.schema()).asc("data").build());
  }

  @TestTemplate
  public void testReplaceSortOrderFailsValidation() {
    assertThat(table.sortOrder()).isEqualTo(SortOrder.unsorted());

    assertThatThrownBy(
            () ->
                table
                    .replaceSortOrder()
                    .asc("data")
                    .commitIf(ImmutableList.of(alwaysFailValidation)))
        .isInstanceOf(ValidationException.class)
        .hasMessage(alwaysFailMessage);

    assertThat(table.sortOrder()).isEqualTo(SortOrder.unsorted());
  }

  @TestTemplate
  public void testReplaceSortOrderFailsValidationDueToConcurrentCommit() {
    assertThat(table.sortOrder()).isEqualTo(SortOrder.unsorted());

    setWatermarkProperty(table, 0);

    ReplaceSortOrder pendingUpdate = table.replaceSortOrder().asc("data");

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(() -> pendingUpdate.commitIf(ImmutableList.of(watermarkValidation(0))))
        .isInstanceOf(ValidationException.class)
        .hasMessage(watermarkFailMessagePattern, 0);

    assertThat(table.sortOrder()).isEqualTo(SortOrder.unsorted());
  }

  @TestTemplate
  public void testReplaceSortOrderFailsDueToIllegalTableModificationInsideValidation() {
    assertThat(table.sortOrder()).isEqualTo(SortOrder.unsorted());

    assertThatThrownBy(
            () ->
                table.replaceSortOrder().asc("data").commitIf(ImmutableList.of(illegalValidation)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    assertThat(table.sortOrder()).isEqualTo(SortOrder.unsorted());
  }

  @TestTemplate
  public void testRewriteFilesPassesValidation() {
    table.newAppend().appendFile(FILE_A).commit();
    validateTableFiles(table, FILE_A);

    table
        .newRewrite()
        .rewriteFiles(ImmutableSet.of(FILE_A), ImmutableSet.of(FILE_B))
        .commitIf(ImmutableList.of(alwaysPassValidation));

    validateTableFiles(table, FILE_B);
  }

  @TestTemplate
  public void testRewriteFilesFailsValidation() {
    table.newAppend().appendFile(FILE_A).commit();
    validateTableFiles(table, FILE_A);

    assertThatThrownBy(
            () ->
                table
                    .newRewrite()
                    .rewriteFiles(ImmutableSet.of(FILE_A), ImmutableSet.of(FILE_B))
                    .commitIf(ImmutableList.of(alwaysFailValidation)))
        .isInstanceOf(ValidationException.class)
        .hasMessage(alwaysFailMessage);

    validateTableFiles(table, FILE_A);
  }

  @TestTemplate
  public void testRewriteFilesFailsValidationDueToConcurrentCommit() {
    table.newAppend().appendFile(FILE_A).commit();
    validateTableFiles(table, FILE_A);

    setWatermarkProperty(table, 0);

    RewriteFiles pendingUpdate =
        table.newRewrite().rewriteFiles(ImmutableSet.of(FILE_A), ImmutableSet.of(FILE_B));

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(() -> pendingUpdate.commitIf(ImmutableList.of(watermarkValidation(0))))
        .isInstanceOf(ValidationException.class)
        .hasMessage(watermarkFailMessagePattern, 0);

    validateTableFiles(table, FILE_A);
  }

  @TestTemplate
  public void testRewriteFilesFailsDueToIllegalTableModificationInsideValidation() {
    table.newAppend().appendFile(FILE_A).commit();
    validateTableFiles(table, FILE_A);

    assertThatThrownBy(
            () ->
                table
                    .newRewrite()
                    .rewriteFiles(ImmutableSet.of(FILE_A), ImmutableSet.of(FILE_B))
                    .commitIf(ImmutableList.of(illegalValidation)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    validateTableFiles(table, FILE_A);
  }

  @TestTemplate
  public void testRewriteManifestsPassesValidation() {
    table.newAppend().appendFile(FILE_A).commit();
    table.newAppend().appendFile(FILE_B).commit();
    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(2);

    table
        .rewriteManifests()
        .clusterBy(dataFile -> "")
        .commitIf(ImmutableList.of(alwaysPassValidation));

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(1);
  }

  @TestTemplate
  public void testRewriteManifestsFailsValidation() {
    table.newAppend().appendFile(FILE_A).commit();
    table.newAppend().appendFile(FILE_B).commit();
    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(2);

    assertThatThrownBy(
            () ->
                table
                    .rewriteManifests()
                    .clusterBy(dataFile -> "")
                    .commitIf(ImmutableList.of(alwaysFailValidation)))
        .isInstanceOf(ValidationException.class)
        .hasMessage(alwaysFailMessage);

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(2);
  }

  @TestTemplate
  public void testRewriteManifestsFailsValidationDueToConcurrentCommit() {
    table.newAppend().appendFile(FILE_A).commit();
    table.newAppend().appendFile(FILE_B).commit();
    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(2);

    setWatermarkProperty(table, 0);

    RewriteManifests pendingUpdate = table.rewriteManifests().clusterBy(dataFile -> "");

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(() -> pendingUpdate.commitIf(ImmutableList.of(watermarkValidation(0))))
        .isInstanceOf(ValidationException.class)
        .hasMessage(watermarkFailMessagePattern, 0);

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(2);
  }

  @TestTemplate
  public void testRewriteManifestsFailsDueToIllegalTableModificationInsideValidation() {
    table.newAppend().appendFile(FILE_A).commit();
    table.newAppend().appendFile(FILE_B).commit();
    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(2);

    assertThatThrownBy(
            () ->
                table
                    .rewriteManifests()
                    .clusterBy(dataFile -> "")
                    .commitIf(ImmutableList.of(illegalValidation)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(2);
  }

  @TestTemplate
  public void testRowDeltaPassesValidation() {
    validateTableFiles(table);

    table.newRowDelta().addRows(FILE_A).commitIf(ImmutableList.of(alwaysPassValidation));

    validateTableFiles(table, FILE_A);
  }

  @TestTemplate
  public void testRowDeltaFailsValidation() {
    validateTableFiles(table);

    assertThatThrownBy(
            () ->
                table
                    .newRowDelta()
                    .addRows(FILE_A)
                    .commitIf(ImmutableList.of(alwaysFailValidation)))
        .isInstanceOf(ValidationException.class)
        .hasMessage(alwaysFailMessage);

    validateTableFiles(table);
  }

  @TestTemplate
  public void testRowDeltaFailsValidationDueToConcurrentCommit() {
    validateTableFiles(table);

    setWatermarkProperty(table, 0);

    RowDelta pendingUpdate = table.newRowDelta().addRows(FILE_A);

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(() -> pendingUpdate.commitIf(ImmutableList.of(watermarkValidation(0))))
        .isInstanceOf(ValidationException.class)
        .hasMessage(watermarkFailMessagePattern, 0);

    validateTableFiles(table);
  }

  @TestTemplate
  public void testRowDeltaFailsDueToIllegalTableModificationInsideValidation() {
    validateTableFiles(table);

    assertThatThrownBy(
            () -> table.newRowDelta().addRows(FILE_A).commitIf(ImmutableList.of(illegalValidation)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    validateTableFiles(table);
  }

  @TestTemplate
  public void testSetSnapshotOperationPassesValidation() {
    table.newAppend().appendFile(FILE_A).commit();
    long firstSnapshotId = table.currentSnapshot().snapshotId();
    table.newAppend().appendFile(FILE_B).commit();
    validateTableFiles(table, FILE_A, FILE_B);

    new SetSnapshotOperation(table.operations())
        .setCurrentSnapshot(firstSnapshotId)
        .commitIf(ImmutableList.of(alwaysPassValidation));

    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(firstSnapshotId);
    validateTableFiles(table, FILE_A);
  }

  @TestTemplate
  public void testSetSnapshotOperationFailsValidation() {
    table.newAppend().appendFile(FILE_A).commit();
    long firstSnapshotId = table.currentSnapshot().snapshotId();
    table.newAppend().appendFile(FILE_B).commit();
    long secondSnapshotId = table.currentSnapshot().snapshotId();
    validateTableFiles(table, FILE_A, FILE_B);

    assertThatThrownBy(
            () ->
                new SetSnapshotOperation(table.operations())
                    .setCurrentSnapshot(firstSnapshotId)
                    .commitIf(ImmutableList.of(alwaysFailValidation)))
        .isInstanceOf(ValidationException.class)
        .hasMessage(alwaysFailMessage);

    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(secondSnapshotId);
    validateTableFiles(table, FILE_A, FILE_B);
  }

  @TestTemplate
  public void testSetSnapshotOperationFailsValidationDueToConcurrentCommit() {
    table.newAppend().appendFile(FILE_A).commit();
    long firstSnapshotId = table.currentSnapshot().snapshotId();
    table.newAppend().appendFile(FILE_B).commit();
    long secondSnapshotId = table.currentSnapshot().snapshotId();
    validateTableFiles(table, FILE_A, FILE_B);

    setWatermarkProperty(table, 0);

    SetSnapshotOperation pendingUpdate =
        new SetSnapshotOperation(table.operations()).setCurrentSnapshot(firstSnapshotId);

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(() -> pendingUpdate.commitIf(ImmutableList.of(watermarkValidation(0))))
        .isInstanceOf(ValidationException.class)
        .hasMessage(watermarkFailMessagePattern, 0);

    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(secondSnapshotId);
    validateTableFiles(table, FILE_A, FILE_B);
  }

  @TestTemplate
  public void testSetSnapshotOperationFailsDueToIllegalTableModificationInsideValidation() {
    table.newAppend().appendFile(FILE_A).commit();
    long firstSnapshotId = table.currentSnapshot().snapshotId();
    table.newAppend().appendFile(FILE_B).commit();
    long secondSnapshotId = table.currentSnapshot().snapshotId();
    validateTableFiles(table, FILE_A, FILE_B);

    assertThatThrownBy(
            () ->
                new SetSnapshotOperation(table.operations())
                    .setCurrentSnapshot(firstSnapshotId)
                    .commitIf(ImmutableList.of(illegalValidation)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(secondSnapshotId);
    validateTableFiles(table, FILE_A, FILE_B);
  }

  @TestTemplate
  public void testUpdateSnapshotReferencesOperationPassesValidation() {
    table.newAppend().appendFile(FILE_A).commit();
    long firstSnapshotId = table.currentSnapshot().snapshotId();
    String branchName = "feature-develop";
    assertThat(table.ops().refresh().ref(branchName)).isNull();

    new UpdateSnapshotReferencesOperation(table.operations())
        .createBranch(branchName, firstSnapshotId)
        .commitIf(ImmutableList.of(alwaysPassValidation));

    assertThat(table.ops().refresh().ref(branchName))
        .isEqualTo(SnapshotRef.branchBuilder(firstSnapshotId).build());
  }

  @TestTemplate
  public void testUpdateSnapshotReferencesOperationFailsValidation() {
    table.newAppend().appendFile(FILE_A).commit();
    long firstSnapshotId = table.currentSnapshot().snapshotId();
    String branchName = "feature-develop";
    assertThat(table.ops().refresh().ref(branchName)).isNull();

    assertThatThrownBy(
            () ->
                new UpdateSnapshotReferencesOperation(table.operations())
                    .createBranch(branchName, firstSnapshotId)
                    .commitIf(ImmutableList.of(alwaysFailValidation)))
        .isInstanceOf(ValidationException.class)
        .hasMessage(alwaysFailMessage);

    assertThat(table.ops().refresh().ref(branchName)).isNull();
  }

  @TestTemplate
  public void testUpdateSnapshotReferencesOperationFailsDueToConcurrentCommit() {
    table.newAppend().appendFile(FILE_A).commit();
    long firstSnapshotId = table.currentSnapshot().snapshotId();
    String branchName = "feature-develop";
    assertThat(table.ops().refresh().ref(branchName)).isNull();

    setWatermarkProperty(table, 0);

    UpdateSnapshotReferencesOperation pendingUpdate =
        new UpdateSnapshotReferencesOperation(table.operations())
            .createBranch(branchName, firstSnapshotId);

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(() -> pendingUpdate.commitIf(ImmutableList.of(watermarkValidation(0))))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Cannot commit changes based on stale metadata");

    assertThat(table.ops().refresh().ref(branchName)).isNull();
  }

  @TestTemplate
  public void
      testUpdateSnapshotReferencesOperationFailsDueToIllegalTableModificationInsideValidation() {
    table.newAppend().appendFile(FILE_A).commit();
    long firstSnapshotId = table.currentSnapshot().snapshotId();
    String branchName = "feature-develop";
    assertThat(table.ops().refresh().ref(branchName)).isNull();

    assertThatThrownBy(
            () ->
                new UpdateSnapshotReferencesOperation(table.operations())
                    .createBranch(branchName, firstSnapshotId)
                    .commitIf(ImmutableList.of(illegalValidation)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    assertThat(table.ops().refresh().ref(branchName)).isNull();
  }

  private static GenericStatisticsFile genericStatisticsFile(Snapshot currentSnapshot) {
    return new GenericStatisticsFile(
        currentSnapshot.snapshotId(),
        "/some/statistics/file.puffin",
        100,
        42,
        ImmutableList.of(
            new GenericBlobMetadata(
                "stats-type",
                currentSnapshot.snapshotId(),
                currentSnapshot.sequenceNumber(),
                ImmutableList.of(1, 2),
                ImmutableMap.of("a-property", "some-property-value"))));
  }

  @TestTemplate
  public void testUpdateStatisticsPassesValidation() {
    table.newFastAppend().commit();
    Snapshot currentSnapshot = table.currentSnapshot();
    assertThat(table.statisticsFiles()).isEmpty();

    GenericStatisticsFile statisticsFile = genericStatisticsFile(currentSnapshot);
    table
        .updateStatistics()
        .setStatistics(currentSnapshot.snapshotId(), statisticsFile)
        .commitIf(ImmutableList.of(alwaysPassValidation));

    assertThat(table.statisticsFiles())
        .as("Table should have statistics files")
        .containsExactly(statisticsFile);
  }

  @TestTemplate
  public void testUpdateStatisticsFailsValidation() {
    table.newFastAppend().commit();
    Snapshot currentSnapshot = table.currentSnapshot();
    assertThat(table.statisticsFiles()).isEmpty();

    assertThatThrownBy(
            () ->
                table
                    .updateStatistics()
                    .setStatistics(
                        currentSnapshot.snapshotId(), genericStatisticsFile(currentSnapshot))
                    .commitIf(ImmutableList.of(alwaysFailValidation)))
        .isInstanceOf(ValidationException.class)
        .hasMessage(alwaysFailMessage);

    assertThat(table.statisticsFiles()).isEmpty();
  }

  @TestTemplate
  public void testUpdateStatisticsFailsValidationDueToConcurrentCommit() {
    table.newFastAppend().commit();
    Snapshot currentSnapshot = table.currentSnapshot();
    assertThat(table.statisticsFiles()).isEmpty();

    setWatermarkProperty(table, 0);

    UpdateStatistics pendingUpdate =
        table
            .updateStatistics()
            .setStatistics(currentSnapshot.snapshotId(), genericStatisticsFile(currentSnapshot));

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(() -> pendingUpdate.commitIf(ImmutableList.of(watermarkValidation(0))))
        .isInstanceOf(ValidationException.class)
        .hasMessage(watermarkFailMessagePattern, 0);

    assertThat(table.statisticsFiles()).isEmpty();
  }

  @TestTemplate
  public void testUpdateStatisticsFailsDueToIllegalTableModificationInsideValidation() {
    table.newFastAppend().commit();
    Snapshot currentSnapshot = table.currentSnapshot();
    assertThat(table.statisticsFiles()).isEmpty();

    assertThatThrownBy(
            () ->
                table
                    .updateStatistics()
                    .setStatistics(
                        currentSnapshot.snapshotId(), genericStatisticsFile(currentSnapshot))
                    .commitIf(ImmutableList.of(illegalValidation)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    assertThat(table.statisticsFiles()).isEmpty();
  }

  @TestTemplate
  public void testUpdateLocationPassesValidation(@TempDir File tempDir) {
    String newLocation = tempDir.getAbsolutePath();
    assertThat(table.location()).isNotEqualTo(newLocation);

    table
        .updateLocation()
        .setLocation(newLocation)
        .commitIf(ImmutableList.of(alwaysPassValidation));

    assertThat(table.location()).isEqualTo(newLocation);
  }

  @TestTemplate
  public void testUpdateLocationFailsValidation(@TempDir File tempDir) {
    String originalLocation = table.location();
    String newLocation = tempDir.getAbsolutePath();
    assertThat(originalLocation).isNotEqualTo(newLocation);

    assertThatThrownBy(
            () ->
                table
                    .updateLocation()
                    .setLocation(newLocation)
                    .commitIf(ImmutableList.of(alwaysFailValidation)))
        .isInstanceOf(ValidationException.class)
        .hasMessage(alwaysFailMessage);

    assertThat(table.location()).isEqualTo(originalLocation);
  }

  @TestTemplate
  public void testUpdateLocationFailsValidationDueToConcurrentCommit(@TempDir File tempDir) {
    String originalLocation = table.location();
    String newLocation = tempDir.getAbsolutePath();
    assertThat(originalLocation).isNotEqualTo(newLocation);

    setWatermarkProperty(table, 0);

    UpdateLocation pendingUpdate = table.updateLocation().setLocation(newLocation);

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(() -> pendingUpdate.commitIf(ImmutableList.of(watermarkValidation(0))))
        .isInstanceOf(ValidationException.class)
        .hasMessage(watermarkFailMessagePattern, 0);

    assertThat(table.location()).isEqualTo(originalLocation);
  }

  @TestTemplate
  public void testUpdateLocationFailsDueToIllegalTableModificationInsideValidation(
      @TempDir File tempDir) {
    String originalLocation = table.location();
    String newLocation = tempDir.getAbsolutePath();
    assertThat(originalLocation).isNotEqualTo(newLocation);

    assertThatThrownBy(
            () ->
                table
                    .updateLocation()
                    .setLocation(newLocation)
                    .commitIf(ImmutableList.of(illegalValidation)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    assertThat(table.location()).isEqualTo(originalLocation);
  }

  @TestTemplate
  public void testUpdatePropertiesPassesValidation() {
    String key = "newKey";
    String value = "newValue";
    assertThat(table.properties().get(key)).isNull();

    table.updateProperties().set(key, value).commitIf(ImmutableList.of(alwaysPassValidation));

    assertThat(table.properties().get(key)).isEqualTo(value);
  }

  @TestTemplate
  public void testUpdatePropertiesFailsValidation() {
    String key = "newKey";
    String value = "newValue";
    assertThat(table.properties().get(key)).isNull();

    assertThatThrownBy(
            () ->
                table
                    .updateProperties()
                    .set(key, value)
                    .commitIf(ImmutableList.of(alwaysFailValidation)))
        .isInstanceOf(ValidationException.class)
        .hasMessage(alwaysFailMessage);

    assertThat(table.properties().get(key)).isNull();
  }

  @TestTemplate
  public void testUpdatePropertiesFailsValidationDueToConcurrentCommit() {
    String key = "newKey";
    String value = "newValue";
    assertThat(table.properties().get(key)).isNull();

    setWatermarkProperty(table, 0);

    UpdateProperties pendingUpdate = table.updateProperties().set(key, value);

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(() -> pendingUpdate.commitIf(ImmutableList.of(watermarkValidation(0))))
        .isInstanceOf(ValidationException.class)
        .hasMessage(watermarkFailMessagePattern, 0);

    assertThat(table.properties().get(key)).isNull();
  }

  @TestTemplate
  public void testUpdatePropertiesFailsDueToIllegalTableModificationInsideValidation() {
    String key = "newKey";
    String value = "newValue";
    assertThat(table.properties().get(key)).isNull();

    assertThatThrownBy(
            () ->
                table
                    .updateProperties()
                    .set(key, value)
                    .commitIf(ImmutableList.of(illegalValidation)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    assertThat(table.properties().get(key)).isNull();
  }

  private static final Schema ORIGINAL_SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), required(2, "data", Types.StringType.get()));

  @TestTemplate
  public void testUpdateSchemaPassesValidation() {
    assertThat(table.schema().sameSchema(ORIGINAL_SCHEMA)).isTrue();

    table
        .updateSchema()
        .addColumn("bool", Types.BooleanType.get())
        .commitIf(ImmutableList.of(alwaysPassValidation));

    assertThat(
            table
                .schema()
                .sameSchema(
                    new Schema(
                        required(1, "id", Types.IntegerType.get()),
                        required(2, "data", Types.StringType.get()),
                        optional(3, "bool", Types.BooleanType.get()))))
        .as("Should include new bucket")
        .isTrue();
  }

  @TestTemplate
  public void testUpdateSchemaFailsValidation() {
    assertThat(table.schema().sameSchema(ORIGINAL_SCHEMA)).isTrue();

    assertThatThrownBy(
            () ->
                table
                    .updateSchema()
                    .addColumn("bool", Types.BooleanType.get())
                    .commitIf(ImmutableList.of(alwaysFailValidation)))
        .isInstanceOf(ValidationException.class)
        .hasMessage(alwaysFailMessage);

    assertThat(table.schema().sameSchema(ORIGINAL_SCHEMA)).isTrue();
  }

  @TestTemplate
  public void testUpdateSchemaFailsDueToConcurrentCommit() {
    assertThat(table.schema().sameSchema(ORIGINAL_SCHEMA)).isTrue();

    setWatermarkProperty(table, 0);

    UpdateSchema pendingUpdate = table.updateSchema().addColumn("bool", Types.BooleanType.get());

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(() -> pendingUpdate.commitIf(ImmutableList.of(watermarkValidation(0))))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Cannot commit changes based on stale metadata");

    assertThat(table.schema().sameSchema(ORIGINAL_SCHEMA)).isTrue();
  }

  @TestTemplate
  public void testUpdateSchemaFailsDueToIllegalTableModificationInsideValidation() {
    assertThat(table.schema().sameSchema(ORIGINAL_SCHEMA)).isTrue();

    assertThatThrownBy(
            () ->
                table
                    .updateSchema()
                    .addColumn("bool", Types.BooleanType.get())
                    .commitIf(ImmutableList.of(illegalValidation)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    assertThat(table.schema().sameSchema(ORIGINAL_SCHEMA)).isTrue();
  }

  private static final PartitionSpec ORIGINAL_SPEC =
      PartitionSpec.builderFor(ORIGINAL_SCHEMA)
          .bucket("data", BUCKETS_NUMBER, "data_bucket")
          .withSpecId(0)
          .build();

  @TestTemplate
  public void testUpdateSpecPassesValidation() {
    assertThat(table.spec()).isEqualTo(ORIGINAL_SPEC);

    table
        .updateSpec()
        .addField("id_bucket", Expressions.bucket("id", BUCKETS_NUMBER))
        .commitIf(ImmutableList.of(alwaysPassValidation));

    assertThat(table.spec())
        .as("Should include new bucket")
        .isEqualTo(
            PartitionSpec.builderFor(table.schema())
                .bucket("data", BUCKETS_NUMBER, "data_bucket")
                .bucket("id", BUCKETS_NUMBER, "id_bucket")
                .withSpecId(1)
                .build());
  }

  @TestTemplate
  public void testUpdateSpecFailsValidation() {
    assertThat(table.spec()).isEqualTo(ORIGINAL_SPEC);

    assertThatThrownBy(
            () ->
                table
                    .updateSpec()
                    .addField("id_bucket", Expressions.bucket("id", BUCKETS_NUMBER))
                    .commitIf(ImmutableList.of(alwaysFailValidation)))
        .isInstanceOf(ValidationException.class)
        .hasMessage(alwaysFailMessage);

    assertThat(table.spec()).isEqualTo(ORIGINAL_SPEC);
  }

  @TestTemplate
  public void testUpdateSpecFailsDueToConcurrentCommit() {
    assertThat(table.spec()).isEqualTo(ORIGINAL_SPEC);

    setWatermarkProperty(table, 0);

    UpdatePartitionSpec pendingUpdate =
        table.updateSpec().addField("id_bucket", Expressions.bucket("id", BUCKETS_NUMBER));

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(() -> pendingUpdate.commitIf(ImmutableList.of(watermarkValidation(0))))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Cannot commit changes based on stale metadata");

    assertThat(table.spec()).isEqualTo(ORIGINAL_SPEC);
  }

  @TestTemplate
  public void testUpdateSpecFailsDueToIllegalTableModificationInsideValidation() {
    assertThat(table.spec()).isEqualTo(ORIGINAL_SPEC);

    assertThatThrownBy(
            () ->
                table
                    .updateSpec()
                    .addField("id_bucket", Expressions.bucket("id", BUCKETS_NUMBER))
                    .commitIf(ImmutableList.of(illegalValidation)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    assertThat(table.spec()).isEqualTo(ORIGINAL_SPEC);
  }
}
