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

    CherryPickOperation cherrypick =
        new CherryPickOperation(table.name(), table.operations()).cherrypick(overwriteSnapshotId);
    cherrypick.validate(ImmutableList.of(alwaysPassValidation));
    cherrypick.commit();

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
            () -> {
              CherryPickOperation cherrypick =
                  new CherryPickOperation(table.name(), table.operations())
                      .cherrypick(overwriteSnapshotId);
              cherrypick.validate(ImmutableList.of(alwaysFailValidation));
              cherrypick.commit();
            })
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
    pendingUpdate.validate(ImmutableList.of(watermarkValidation(0)));

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(pendingUpdate::commit)
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
            () -> {
              CherryPickOperation cherrypick =
                  new CherryPickOperation(table.name(), table.operations())
                      .cherrypick(overwriteSnapshotId);
              cherrypick.validate(ImmutableList.of(illegalValidation));
              cherrypick.commit();
            })
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    assertThat(firstSnapshotId).isEqualTo(table.currentSnapshot().snapshotId());
    validateTableFiles(table, FILE_A);
  }

  @TestTemplate
  public void testDeleteFilesPassesValidation() {
    table.newFastAppend().appendFile(FILE_A).commit();
    validateTableFiles(table, FILE_A);

    DeleteFiles deleteFiles = table.newDelete().deleteFile(FILE_A);
    deleteFiles.validate(ImmutableList.of(alwaysPassValidation));
    deleteFiles.commit();

    validateTableFiles(table);
  }

  @TestTemplate
  public void testDeleteFilesFailsValidation() {
    table.newFastAppend().appendFile(FILE_A).commit();
    validateTableFiles(table, FILE_A);

    assertThatThrownBy(
            () -> {
              DeleteFiles deleteFiles = table.newDelete().deleteFile(FILE_A);
              deleteFiles.validate(ImmutableList.of(alwaysFailValidation));
              deleteFiles.commit();
            })
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
    pendingUpdate.validate(ImmutableList.of(watermarkValidation(0)));

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(pendingUpdate::commit)
        .isInstanceOf(ValidationException.class)
        .hasMessage(watermarkFailMessagePattern, 0);

    validateTableFiles(table, FILE_A);
  }

  @TestTemplate
  public void testDeleteFilesFailsDueToIllegalTableModificationInsideValidation() {
    table.newFastAppend().appendFile(FILE_A).commit();
    validateTableFiles(table, FILE_A);

    assertThatThrownBy(
            () -> {
              DeleteFiles deleteFiles = table.newDelete().deleteFile(FILE_A);
              deleteFiles.validate(ImmutableList.of(illegalValidation));
              deleteFiles.commit();
            })
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

    ExpireSnapshots expireSnapshots =
        table
            .expireSnapshots()
            .expireSnapshotId(firstSnapshot.snapshotId())
            .deleteWith(deletedFiles::add);
    expireSnapshots.validate(ImmutableList.of(alwaysPassValidation));
    expireSnapshots.commit();

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
            () -> {
              ExpireSnapshots expireSnapshots =
                  table
                      .expireSnapshots()
                      .expireSnapshotId(firstSnapshot.snapshotId())
                      .deleteWith(deletedFiles::add);
              expireSnapshots.validate(ImmutableList.of(alwaysFailValidation));
              expireSnapshots.commit();
            })
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
    pendingUpdate.validate(ImmutableList.of(watermarkValidation(0)));

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(pendingUpdate::commit)
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
            () -> {
              ExpireSnapshots expireSnapshots =
                  table
                      .expireSnapshots()
                      .expireSnapshotId(firstSnapshot.snapshotId())
                      .deleteWith(deletedFiles::add);
              expireSnapshots.validate(ImmutableList.of(illegalValidation));
              expireSnapshots.commit();
            })
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    assertThat(deletedFiles).isEmpty();
  }

  @TestTemplate
  public void testFastAppendPassesValidation() {
    validateTableFiles(table);

    AppendFiles appendFiles = table.newFastAppend().appendFile(FILE_A);
    appendFiles.validate(ImmutableList.of(alwaysPassValidation));
    appendFiles.commit();

    validateTableFiles(table, FILE_A);
  }

  @TestTemplate
  public void testFastAppendFailsValidation() {
    validateTableFiles(table);

    assertThatThrownBy(
            () -> {
              AppendFiles appendFiles = table.newFastAppend().appendFile(FILE_A);
              appendFiles.validate(ImmutableList.of(alwaysFailValidation));
              appendFiles.commit();
            })
        .isInstanceOf(ValidationException.class)
        .hasMessage(alwaysFailMessage);

    validateTableFiles(table);
  }

  @TestTemplate
  public void testFastAppendFailsValidationDueToConcurrentCommit() {
    validateTableFiles(table);

    setWatermarkProperty(table, 0);

    PendingUpdate<?> pendingUpdate = table.newFastAppend().appendFile(FILE_A);
    pendingUpdate.validate(ImmutableList.of(watermarkValidation(0)));

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(pendingUpdate::commit)
        .isInstanceOf(ValidationException.class)
        .hasMessage(watermarkFailMessagePattern, 0);

    validateTableFiles(table);
  }

  @TestTemplate
  public void testFastAppendFailsDueToIllegalTableModificationInsideValidation() {
    validateTableFiles(table);

    assertThatThrownBy(
            () -> {
              AppendFiles appendFiles = table.newFastAppend().appendFile(FILE_A);
              appendFiles.validate(ImmutableList.of(illegalValidation));
              appendFiles.commit();
            })
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

    ManageSnapshots manageSnapshots = table.manageSnapshots().createTag(tagName, snapshotId);
    manageSnapshots.validate(ImmutableList.of(alwaysPassValidation));
    manageSnapshots.commit();

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
            () -> {
              ManageSnapshots manageSnapshots =
                  table.manageSnapshots().createTag(tagName, snapshotId);
              manageSnapshots.validate(ImmutableList.of(alwaysFailValidation));
              manageSnapshots.commit();
            })
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
    pendingUpdate.validate(ImmutableList.of(watermarkValidation(0)));

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(pendingUpdate::commit)
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
            () -> {
              ManageSnapshots manageSnapshots =
                  table.manageSnapshots().createTag(tagName, snapshotId);
              manageSnapshots.validate(ImmutableList.of(illegalValidation));
              manageSnapshots.commit();
            })
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    assertThat(table.refs().get(tagName)).isNull();
  }

  @TestTemplate
  public void testMergeAppendPassesValidation() {
    validateTableFiles(table);

    AppendFiles appendFiles = table.newAppend().appendFile(FILE_A);
    appendFiles.validate(ImmutableList.of(alwaysPassValidation));
    appendFiles.commit();

    validateTableFiles(table, FILE_A);
  }

  @TestTemplate
  public void testMergeAppendFailsValidation() {
    validateTableFiles(table);

    assertThatThrownBy(
            () -> {
              AppendFiles appendFiles = table.newAppend().appendFile(FILE_A);
              appendFiles.validate(ImmutableList.of(alwaysFailValidation));
              appendFiles.commit();
            })
        .isInstanceOf(ValidationException.class)
        .hasMessage(alwaysFailMessage);

    validateTableFiles(table);
  }

  @TestTemplate
  public void testMergeAppendFailsValidationDueToConcurrentCommit() {
    validateTableFiles(table);

    setWatermarkProperty(table, 0);

    AppendFiles pendingUpdate = table.newAppend().appendFile(FILE_A);
    pendingUpdate.validate(ImmutableList.of(watermarkValidation(0)));

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(pendingUpdate::commit)
        .isInstanceOf(ValidationException.class)
        .hasMessage(watermarkFailMessagePattern, 0);

    validateTableFiles(table);
  }

  @TestTemplate
  public void testMergeAppendFailsDueToIllegalTableModificationInsideValidation() {
    validateTableFiles(table);

    assertThatThrownBy(
            () -> {
              AppendFiles appendFiles = table.newAppend().appendFile(FILE_A);
              appendFiles.validate(ImmutableList.of(illegalValidation));
              appendFiles.commit();
            })
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    validateTableFiles(table);
  }

  @TestTemplate
  public void testOverwritePassesValidation() {
    table.newFastAppend().appendFile(FILE_A).commit();
    validateTableFiles(table, FILE_A);

    OverwriteFiles overwriteFiles =
        table.newOverwrite().overwriteByRowFilter(Expressions.alwaysTrue()).addFile(FILE_B);
    overwriteFiles.validate(ImmutableList.of(alwaysPassValidation));
    overwriteFiles.commit();

    validateTableFiles(table, FILE_B);
  }

  @TestTemplate
  public void testOverwriteFailsValidation() {
    table.newFastAppend().appendFile(FILE_A).commit();
    validateTableFiles(table, FILE_A);

    assertThatThrownBy(
            () -> {
              OverwriteFiles overwriteFiles =
                  table
                      .newOverwrite()
                      .overwriteByRowFilter(Expressions.alwaysTrue())
                      .addFile(FILE_B);
              overwriteFiles.validate(ImmutableList.of(alwaysFailValidation));
              overwriteFiles.commit();
            })
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
    pendingUpdate.validate(ImmutableList.of(watermarkValidation(0)));

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(pendingUpdate::commit)
        .isInstanceOf(ValidationException.class)
        .hasMessage(watermarkFailMessagePattern, 0);

    validateTableFiles(table, FILE_A);
  }

  @TestTemplate
  public void testOverwriteFailsDueToIllegalTableModificationInsideValidation() {
    table.newFastAppend().appendFile(FILE_A).commit();
    validateTableFiles(table, FILE_A);

    assertThatThrownBy(
            () -> {
              OverwriteFiles overwriteFiles =
                  table
                      .newOverwrite()
                      .overwriteByRowFilter(Expressions.alwaysTrue())
                      .addFile(FILE_B);
              overwriteFiles.validate(ImmutableList.of(illegalValidation));
              overwriteFiles.commit();
            })
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    validateTableFiles(table, FILE_A);
  }

  @TestTemplate
  public void testReplacePartitionsPassesValidation() {
    validateTableFiles(table);

    ReplacePartitions replacePartitions = table.newReplacePartitions().addFile(FILE_A);
    replacePartitions.validate(ImmutableList.of(alwaysPassValidation));
    replacePartitions.commit();

    validateTableFiles(table, FILE_A);
  }

  @TestTemplate
  public void testReplacePartitionsFailsValidation() {
    validateTableFiles(table);

    assertThatThrownBy(
            () -> {
              ReplacePartitions replacePartitions = table.newReplacePartitions().addFile(FILE_A);
              replacePartitions.validate(ImmutableList.of(alwaysFailValidation));
              replacePartitions.commit();
            })
        .isInstanceOf(ValidationException.class)
        .hasMessage(alwaysFailMessage);

    validateTableFiles(table);
  }

  @TestTemplate
  public void testReplacePartitionsFailsValidationDueToConcurrentCommit() {
    validateTableFiles(table);

    setWatermarkProperty(table, 0);

    ReplacePartitions pendingUpdate = table.newReplacePartitions().addFile(FILE_A).addFile(FILE_B);
    pendingUpdate.validate(ImmutableList.of(watermarkValidation(0)));

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(pendingUpdate::commit)
        .isInstanceOf(ValidationException.class)
        .hasMessage(watermarkFailMessagePattern, 0);

    validateTableFiles(table);
  }

  @TestTemplate
  public void testReplacePartitionsFailsDueToIllegalTableModificationInsideValidation() {
    validateTableFiles(table);

    assertThatThrownBy(
            () -> {
              ReplacePartitions replacePartitions = table.newReplacePartitions().addFile(FILE_A);
              replacePartitions.validate(ImmutableList.of(illegalValidation));
              replacePartitions.commit();
            })
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    validateTableFiles(table);
  }

  @TestTemplate
  public void testReplaceSortOrderPassesValidation() {
    assertThat(table.sortOrder()).isEqualTo(SortOrder.unsorted());

    ReplaceSortOrder replaceSortOrder = table.replaceSortOrder().asc("data");
    replaceSortOrder.validate(ImmutableList.of(alwaysPassValidation));
    replaceSortOrder.commit();

    assertThat(table.sortOrder())
        .as("Table should reflect new sort order")
        .isEqualTo(SortOrder.builderFor(table.schema()).asc("data").build());
  }

  @TestTemplate
  public void testReplaceSortOrderFailsValidation() {
    assertThat(table.sortOrder()).isEqualTo(SortOrder.unsorted());

    assertThatThrownBy(
            () -> {
              ReplaceSortOrder replaceSortOrder = table.replaceSortOrder().asc("data");
              replaceSortOrder.validate(ImmutableList.of(alwaysFailValidation));
              replaceSortOrder.commit();
            })
        .isInstanceOf(ValidationException.class)
        .hasMessage(alwaysFailMessage);

    assertThat(table.sortOrder()).isEqualTo(SortOrder.unsorted());
  }

  @TestTemplate
  public void testReplaceSortOrderFailsValidationDueToConcurrentCommit() {
    assertThat(table.sortOrder()).isEqualTo(SortOrder.unsorted());

    setWatermarkProperty(table, 0);

    ReplaceSortOrder pendingUpdate = table.replaceSortOrder().asc("data");
    pendingUpdate.validate(ImmutableList.of(watermarkValidation(0)));

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(pendingUpdate::commit)
        .isInstanceOf(ValidationException.class)
        .hasMessage(watermarkFailMessagePattern, 0);

    assertThat(table.sortOrder()).isEqualTo(SortOrder.unsorted());
  }

  @TestTemplate
  public void testReplaceSortOrderFailsDueToIllegalTableModificationInsideValidation() {
    assertThat(table.sortOrder()).isEqualTo(SortOrder.unsorted());

    assertThatThrownBy(
            () -> {
              ReplaceSortOrder replaceSortOrder = table.replaceSortOrder().asc("data");
              replaceSortOrder.validate(ImmutableList.of(illegalValidation));
              replaceSortOrder.commit();
            })
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    assertThat(table.sortOrder()).isEqualTo(SortOrder.unsorted());
  }

  @TestTemplate
  public void testRewriteFilesPassesValidation() {
    table.newAppend().appendFile(FILE_A).commit();
    validateTableFiles(table, FILE_A);

    RewriteFiles rewriteFiles =
        table.newRewrite().rewriteFiles(ImmutableSet.of(FILE_A), ImmutableSet.of(FILE_B));
    rewriteFiles.validate(ImmutableList.of(alwaysPassValidation));
    rewriteFiles.commit();

    validateTableFiles(table, FILE_B);
  }

  @TestTemplate
  public void testRewriteFilesFailsValidation() {
    table.newAppend().appendFile(FILE_A).commit();
    validateTableFiles(table, FILE_A);

    assertThatThrownBy(
            () -> {
              RewriteFiles rewriteFiles =
                  table.newRewrite().rewriteFiles(ImmutableSet.of(FILE_A), ImmutableSet.of(FILE_B));
              rewriteFiles.validate(ImmutableList.of(alwaysFailValidation));
              rewriteFiles.commit();
            })
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
    pendingUpdate.validate(ImmutableList.of(watermarkValidation(0)));

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(pendingUpdate::commit)
        .isInstanceOf(ValidationException.class)
        .hasMessage(watermarkFailMessagePattern, 0);

    validateTableFiles(table, FILE_A);
  }

  @TestTemplate
  public void testRewriteFilesFailsDueToIllegalTableModificationInsideValidation() {
    table.newAppend().appendFile(FILE_A).commit();
    validateTableFiles(table, FILE_A);

    assertThatThrownBy(
            () -> {
              RewriteFiles rewriteFiles =
                  table.newRewrite().rewriteFiles(ImmutableSet.of(FILE_A), ImmutableSet.of(FILE_B));
              rewriteFiles.validate(ImmutableList.of(illegalValidation));
              rewriteFiles.commit();
            })
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    validateTableFiles(table, FILE_A);
  }

  @TestTemplate
  public void testRewriteManifestsPassesValidation() {
    table.newAppend().appendFile(FILE_A).commit();
    table.newAppend().appendFile(FILE_B).commit();
    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(2);

    RewriteManifests rewriteManifests = table.rewriteManifests().clusterBy(dataFile -> "");
    rewriteManifests.validate(ImmutableList.of(alwaysPassValidation));
    rewriteManifests.commit();

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(1);
  }

  @TestTemplate
  public void testRewriteManifestsFailsValidation() {
    table.newAppend().appendFile(FILE_A).commit();
    table.newAppend().appendFile(FILE_B).commit();
    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(2);

    assertThatThrownBy(
            () -> {
              RewriteManifests rewriteManifests =
                  table.rewriteManifests().clusterBy(dataFile -> "");
              rewriteManifests.validate(ImmutableList.of(alwaysFailValidation));
              rewriteManifests.commit();
            })
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
    pendingUpdate.validate(ImmutableList.of(watermarkValidation(0)));

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(pendingUpdate::commit)
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
            () -> {
              RewriteManifests rewriteManifests =
                  table.rewriteManifests().clusterBy(dataFile -> "");
              rewriteManifests.validate(ImmutableList.of(illegalValidation));
              rewriteManifests.commit();
            })
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(2);
  }

  @TestTemplate
  public void testRowDeltaPassesValidation() {
    validateTableFiles(table);

    RowDelta rowDelta = table.newRowDelta().addRows(FILE_A);
    rowDelta.validate(ImmutableList.of(alwaysPassValidation));
    rowDelta.commit();

    validateTableFiles(table, FILE_A);
  }

  @TestTemplate
  public void testRowDeltaFailsValidation() {
    validateTableFiles(table);

    assertThatThrownBy(
            () -> {
              RowDelta rowDelta = table.newRowDelta().addRows(FILE_A);
              rowDelta.validate(ImmutableList.of(alwaysFailValidation));
              rowDelta.commit();
            })
        .isInstanceOf(ValidationException.class)
        .hasMessage(alwaysFailMessage);

    validateTableFiles(table);
  }

  @TestTemplate
  public void testRowDeltaFailsValidationDueToConcurrentCommit() {
    validateTableFiles(table);

    setWatermarkProperty(table, 0);

    RowDelta pendingUpdate = table.newRowDelta().addRows(FILE_A);
    pendingUpdate.validate(ImmutableList.of(watermarkValidation(0)));

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(pendingUpdate::commit)
        .isInstanceOf(ValidationException.class)
        .hasMessage(watermarkFailMessagePattern, 0);

    validateTableFiles(table);
  }

  @TestTemplate
  public void testRowDeltaFailsDueToIllegalTableModificationInsideValidation() {
    validateTableFiles(table);

    assertThatThrownBy(
            () -> {
              RowDelta rowDelta = table.newRowDelta().addRows(FILE_A);
              rowDelta.validate(ImmutableList.of(illegalValidation));
              rowDelta.commit();
            })
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

    SetSnapshotOperation setSnapshotOperation =
        new SetSnapshotOperation(table.operations()).setCurrentSnapshot(firstSnapshotId);
    setSnapshotOperation.validate(ImmutableList.of(alwaysPassValidation));
    setSnapshotOperation.commit();

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
            () -> {
              SetSnapshotOperation setSnapshotOperation =
                  new SetSnapshotOperation(table.operations()).setCurrentSnapshot(firstSnapshotId);
              setSnapshotOperation.validate(ImmutableList.of(alwaysFailValidation));
              setSnapshotOperation.commit();
            })
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
    pendingUpdate.validate(ImmutableList.of(watermarkValidation(0)));

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(pendingUpdate::commit)
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
            () -> {
              SetSnapshotOperation setSnapshotOperation =
                  new SetSnapshotOperation(table.operations()).setCurrentSnapshot(firstSnapshotId);
              setSnapshotOperation.validate(ImmutableList.of(illegalValidation));
              setSnapshotOperation.commit();
            })
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

    UpdateSnapshotReferencesOperation updateSnapshotReferencesOperation =
        new UpdateSnapshotReferencesOperation(table.operations())
            .createBranch(branchName, firstSnapshotId);
    updateSnapshotReferencesOperation.validate(ImmutableList.of(alwaysPassValidation));
    updateSnapshotReferencesOperation.commit();

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
            () -> {
              UpdateSnapshotReferencesOperation updateSnapshotReferencesOperation =
                  new UpdateSnapshotReferencesOperation(table.operations())
                      .createBranch(branchName, firstSnapshotId);
              updateSnapshotReferencesOperation.validate(ImmutableList.of(alwaysFailValidation));
              updateSnapshotReferencesOperation.commit();
            })
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
    pendingUpdate.validate(ImmutableList.of(watermarkValidation(0)));

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(pendingUpdate::commit)
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
            () -> {
              UpdateSnapshotReferencesOperation updateSnapshotReferencesOperation =
                  new UpdateSnapshotReferencesOperation(table.operations())
                      .createBranch(branchName, firstSnapshotId);
              updateSnapshotReferencesOperation.validate(ImmutableList.of(illegalValidation));
              updateSnapshotReferencesOperation.commit();
            })
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
    UpdateStatistics updateStatistics =
        table.updateStatistics().setStatistics(currentSnapshot.snapshotId(), statisticsFile);
    updateStatistics.validate(ImmutableList.of(alwaysPassValidation));
    updateStatistics.commit();

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
            () -> {
              UpdateStatistics updateStatistics =
                  table
                      .updateStatistics()
                      .setStatistics(
                          currentSnapshot.snapshotId(), genericStatisticsFile(currentSnapshot));
              updateStatistics.validate(ImmutableList.of(alwaysFailValidation));
              updateStatistics.commit();
            })
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
    pendingUpdate.validate(ImmutableList.of(watermarkValidation(0)));

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(pendingUpdate::commit)
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
            () -> {
              UpdateStatistics updateStatistics =
                  table
                      .updateStatistics()
                      .setStatistics(
                          currentSnapshot.snapshotId(), genericStatisticsFile(currentSnapshot));
              updateStatistics.validate(ImmutableList.of(illegalValidation));
              updateStatistics.commit();
            })
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    assertThat(table.statisticsFiles()).isEmpty();
  }

  @TestTemplate
  public void testUpdateLocationPassesValidation(@TempDir File tempDir) {
    String newLocation = tempDir.getAbsolutePath();
    assertThat(table.location()).isNotEqualTo(newLocation);

    UpdateLocation updateLocation = table.updateLocation().setLocation(newLocation);
    updateLocation.validate(ImmutableList.of(alwaysPassValidation));
    updateLocation.commit();

    assertThat(table.location()).isEqualTo(newLocation);
  }

  @TestTemplate
  public void testUpdateLocationFailsValidation(@TempDir File tempDir) {
    String originalLocation = table.location();
    String newLocation = tempDir.getAbsolutePath();
    assertThat(originalLocation).isNotEqualTo(newLocation);

    assertThatThrownBy(
            () -> {
              UpdateLocation updateLocation = table.updateLocation().setLocation(newLocation);
              updateLocation.validate(ImmutableList.of(alwaysFailValidation));
              updateLocation.commit();
            })
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
    pendingUpdate.validate(ImmutableList.of(watermarkValidation(0)));

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(pendingUpdate::commit)
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
            () -> {
              UpdateLocation updateLocation = table.updateLocation().setLocation(newLocation);
              updateLocation.validate(ImmutableList.of(illegalValidation));
              updateLocation.commit();
            })
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    assertThat(table.location()).isEqualTo(originalLocation);
  }

  @TestTemplate
  public void testUpdatePropertiesPassesValidation() {
    String key = "newKey";
    String value = "newValue";
    assertThat(table.properties().get(key)).isNull();

    UpdateProperties updateProperties = table.updateProperties().set(key, value);
    updateProperties.validate(ImmutableList.of(alwaysPassValidation));
    updateProperties.commit();

    assertThat(table.properties().get(key)).isEqualTo(value);
  }

  @TestTemplate
  public void testUpdatePropertiesFailsValidation() {
    String key = "newKey";
    String value = "newValue";
    assertThat(table.properties().get(key)).isNull();

    assertThatThrownBy(
            () -> {
              UpdateProperties updateProperties = table.updateProperties().set(key, value);
              updateProperties.validate(ImmutableList.of(alwaysFailValidation));
              updateProperties.commit();
            })
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
    pendingUpdate.validate(ImmutableList.of(watermarkValidation(0)));

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(pendingUpdate::commit)
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
            () -> {
              UpdateProperties updateProperties = table.updateProperties().set(key, value);
              updateProperties.validate(ImmutableList.of(illegalValidation));
              updateProperties.commit();
            })
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

    UpdateSchema updateSchema = table.updateSchema().addColumn("bool", Types.BooleanType.get());
    updateSchema.validate(ImmutableList.of(alwaysPassValidation));
    updateSchema.commit();

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
            () -> {
              UpdateSchema updateSchema =
                  table.updateSchema().addColumn("bool", Types.BooleanType.get());
              updateSchema.validate(ImmutableList.of(alwaysFailValidation));
              updateSchema.commit();
            })
        .isInstanceOf(ValidationException.class)
        .hasMessage(alwaysFailMessage);

    assertThat(table.schema().sameSchema(ORIGINAL_SCHEMA)).isTrue();
  }

  @TestTemplate
  public void testUpdateSchemaFailsDueToConcurrentCommit() {
    assertThat(table.schema().sameSchema(ORIGINAL_SCHEMA)).isTrue();

    setWatermarkProperty(table, 0);

    UpdateSchema pendingUpdate = table.updateSchema().addColumn("bool", Types.BooleanType.get());
    pendingUpdate.validate(ImmutableList.of(watermarkValidation(0)));

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(pendingUpdate::commit)
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Cannot commit changes based on stale metadata");

    assertThat(table.schema().sameSchema(ORIGINAL_SCHEMA)).isTrue();
  }

  @TestTemplate
  public void testUpdateSchemaFailsDueToIllegalTableModificationInsideValidation() {
    assertThat(table.schema().sameSchema(ORIGINAL_SCHEMA)).isTrue();

    assertThatThrownBy(
            () -> {
              UpdateSchema updateSchema =
                  table.updateSchema().addColumn("bool", Types.BooleanType.get());
              updateSchema.validate(ImmutableList.of(illegalValidation));
              updateSchema.commit();
            })
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

    UpdatePartitionSpec updatePartitionSpec =
        table.updateSpec().addField("id_bucket", Expressions.bucket("id", BUCKETS_NUMBER));
    updatePartitionSpec.validate(ImmutableList.of(alwaysPassValidation));
    updatePartitionSpec.commit();

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
            () -> {
              UpdatePartitionSpec updatePartitionSpec =
                  table
                      .updateSpec()
                      .addField("id_bucket", Expressions.bucket("id", BUCKETS_NUMBER));
              updatePartitionSpec.validate(ImmutableList.of(alwaysFailValidation));
              updatePartitionSpec.commit();
            })
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
    pendingUpdate.validate(ImmutableList.of(watermarkValidation(0)));

    // concurrent update to the table which advances our watermark value before we're able to commit
    setWatermarkProperty(table, 1);

    assertThatThrownBy(pendingUpdate::commit)
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Cannot commit changes based on stale metadata");

    assertThat(table.spec()).isEqualTo(ORIGINAL_SPEC);
  }

  @TestTemplate
  public void testUpdateSpecFailsDueToIllegalTableModificationInsideValidation() {
    assertThat(table.spec()).isEqualTo(ORIGINAL_SPEC);

    assertThatThrownBy(
            () -> {
              UpdatePartitionSpec updatePartitionSpec =
                  table
                      .updateSpec()
                      .addField("id_bucket", Expressions.bucket("id", BUCKETS_NUMBER));
              updatePartitionSpec.validate(ImmutableList.of(illegalValidation));
              updatePartitionSpec.commit();
            })
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");

    assertThat(table.spec()).isEqualTo(ORIGINAL_SPEC);
  }
}
