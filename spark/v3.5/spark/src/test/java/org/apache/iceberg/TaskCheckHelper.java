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

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public final class TaskCheckHelper {
  private TaskCheckHelper() {}

  public static void assertEquals(
      ScanTaskGroup<FileScanTask> expected, ScanTaskGroup<FileScanTask> actual) {
    List<FileScanTask> expectedTasks = getFileScanTasksInFilePathOrder(expected);
    List<FileScanTask> actualTasks = getFileScanTasksInFilePathOrder(actual);

    assertThat(actualTasks)
        .as("The number of file scan tasks should match")
        .hasSameSizeAs(expectedTasks);

    for (int i = 0; i < expectedTasks.size(); i++) {
      FileScanTask expectedTask = expectedTasks.get(i);
      FileScanTask actualTask = actualTasks.get(i);
      assertEquals(expectedTask, actualTask);
    }
  }

  public static void assertEquals(FileScanTask expected, FileScanTask actual) {
    assertEquals(expected.file(), actual.file());

    // PartitionSpec implements its own equals method
    assertThat(actual.spec()).as("PartitionSpec doesn't match").isEqualTo(expected.spec());

    assertThat(actual.start()).as("starting position doesn't match").isEqualTo(expected.start());

    assertThat(actual.start())
        .as("the number of bytes to scan doesn't match")
        .isEqualTo(expected.start());

    // simplify comparison on residual expression via comparing toString
    assertThat(actual.residual().toString())
        .as("Residual expression doesn't match")
        .isEqualTo(expected.residual().toString());
  }

  public static void assertEquals(DataFile expected, DataFile actual) {
    assertThat(actual.path())
        .as("Should match the serialized record path")
        .isEqualTo(expected.path());
    assertThat(actual.format())
        .as("Should match the serialized record format")
        .isEqualTo(expected.format());
    assertThat(actual.partition().get(0, Object.class))
        .as("Should match the serialized record partition")
        .isEqualTo(expected.partition().get(0, Object.class));
    assertThat(actual.recordCount())
        .as("Should match the serialized record count")
        .isEqualTo(expected.recordCount());
    assertThat(actual.fileSizeInBytes())
        .as("Should match the serialized record size")
        .isEqualTo(expected.fileSizeInBytes());
    assertThat(actual.valueCounts())
        .as("Should match the serialized record value counts")
        .isEqualTo(expected.valueCounts());
    assertThat(actual.nullValueCounts())
        .as("Should match the serialized record null value counts")
        .isEqualTo(expected.nullValueCounts());
    assertThat(actual.lowerBounds())
        .as("Should match the serialized record lower bounds")
        .isEqualTo(expected.lowerBounds());
    assertThat(actual.upperBounds())
        .as("Should match the serialized record upper bounds")
        .isEqualTo(expected.upperBounds());
    assertThat(actual.keyMetadata())
        .as("Should match the serialized record key metadata")
        .isEqualTo(expected.keyMetadata());
    assertThat(actual.splitOffsets())
        .as("Should match the serialized record offsets")
        .isEqualTo(expected.splitOffsets());
    assertThat(actual.keyMetadata())
        .as("Should match the serialized record offsets")
        .isEqualTo(expected.keyMetadata());
  }

  private static List<FileScanTask> getFileScanTasksInFilePathOrder(
      ScanTaskGroup<FileScanTask> taskGroup) {
    return taskGroup.tasks().stream()
        // use file path + start position to differentiate the tasks
        .sorted(Comparator.comparing(o -> o.file().path().toString() + "##" + o.start()))
        .collect(Collectors.toList());
  }
}
