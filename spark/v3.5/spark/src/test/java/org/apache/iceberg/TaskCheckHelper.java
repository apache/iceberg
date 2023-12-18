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

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Assert;

public final class TaskCheckHelper {
  private TaskCheckHelper() {}

  public static void assertEquals(
      ScanTaskGroup<FileScanTask> expected, ScanTaskGroup<FileScanTask> actual) {
    List<FileScanTask> expectedTasks = getFileScanTasksInFilePathOrder(expected);
    List<FileScanTask> actualTasks = getFileScanTasksInFilePathOrder(actual);

    Assert.assertEquals(
        "The number of file scan tasks should match", expectedTasks.size(), actualTasks.size());

    for (int i = 0; i < expectedTasks.size(); i++) {
      FileScanTask expectedTask = expectedTasks.get(i);
      FileScanTask actualTask = actualTasks.get(i);
      assertEquals(expectedTask, actualTask);
    }
  }

  public static void assertEquals(FileScanTask expected, FileScanTask actual) {
    assertEquals(expected.file(), actual.file());

    // PartitionSpec implements its own equals method
    Assert.assertEquals("PartitionSpec doesn't match", expected.spec(), actual.spec());

    Assert.assertEquals("starting position doesn't match", expected.start(), actual.start());

    Assert.assertEquals(
        "the number of bytes to scan doesn't match", expected.start(), actual.start());

    // simplify comparison on residual expression via comparing toString
    Assert.assertEquals(
        "Residual expression doesn't match",
        expected.residual().toString(),
        actual.residual().toString());
  }

  public static void assertEquals(DataFile expected, DataFile actual) {
    Assert.assertEquals("Should match the serialized record path", expected.path(), actual.path());
    Assert.assertEquals(
        "Should match the serialized record format", expected.format(), actual.format());
    Assert.assertEquals(
        "Should match the serialized record partition",
        expected.partition().get(0, Object.class),
        actual.partition().get(0, Object.class));
    Assert.assertEquals(
        "Should match the serialized record count", expected.recordCount(), actual.recordCount());
    Assert.assertEquals(
        "Should match the serialized record size",
        expected.fileSizeInBytes(),
        actual.fileSizeInBytes());
    Assert.assertEquals(
        "Should match the serialized record value counts",
        expected.valueCounts(),
        actual.valueCounts());
    Assert.assertEquals(
        "Should match the serialized record null value counts",
        expected.nullValueCounts(),
        actual.nullValueCounts());
    Assert.assertEquals(
        "Should match the serialized record lower bounds",
        expected.lowerBounds(),
        actual.lowerBounds());
    Assert.assertEquals(
        "Should match the serialized record upper bounds",
        expected.upperBounds(),
        actual.upperBounds());
    Assert.assertEquals(
        "Should match the serialized record key metadata",
        expected.keyMetadata(),
        actual.keyMetadata());
    Assert.assertEquals(
        "Should match the serialized record offsets",
        expected.splitOffsets(),
        actual.splitOffsets());
    Assert.assertEquals(
        "Should match the serialized record offsets", expected.keyMetadata(), actual.keyMetadata());
  }

  private static List<FileScanTask> getFileScanTasksInFilePathOrder(
      ScanTaskGroup<FileScanTask> taskGroup) {
    return taskGroup.tasks().stream()
        // use file path + start position to differentiate the tasks
        .sorted(Comparator.comparing(o -> o.file().path().toString() + "##" + o.start()))
        .collect(Collectors.toList());
  }
}
