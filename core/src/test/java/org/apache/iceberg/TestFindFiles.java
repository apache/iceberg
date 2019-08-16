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

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Set;
import org.apache.iceberg.expressions.Expressions;
import org.junit.Assert;
import org.junit.Test;

public class TestFindFiles extends TableTestBase {
  @Test
  public void testBasicBehavior() {
    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Iterable<DataFile> files = FindFiles.in(table).collect();

    Assert.assertEquals(pathSet(FILE_A, FILE_B), pathSet(files));
  }

  @Test
  public void testWithMetadataMatching() {
    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .commit();

    Iterable<DataFile> files = FindFiles.in(table)
        .withMetadataMatching(Expressions.startsWith("file_path", "/path/to/data-a"))
        .collect();

    Assert.assertEquals(pathSet(FILE_A), pathSet(files));
  }

  @Test
  public void testInPartition() {
    table.newAppend()
        .appendFile(FILE_A) // bucket 0
        .appendFile(FILE_B) // bucket 1
        .appendFile(FILE_C) // bucket 2
        .appendFile(FILE_D) // bucket 3
        .commit();

    Iterable<DataFile> files = FindFiles.in(table)
        .inPartition(table.spec(), StaticDataTask.Row.of(1))
        .inPartition(table.spec(), StaticDataTask.Row.of(2))
        .collect();

    Assert.assertEquals(pathSet(FILE_B, FILE_C), pathSet(files));
  }

  @Test
  public void testInPartitions() {
    table.newAppend()
        .appendFile(FILE_A) // bucket 0
        .appendFile(FILE_B) // bucket 1
        .appendFile(FILE_C) // bucket 2
        .appendFile(FILE_D) // bucket 3
        .commit();

    Iterable<DataFile> files = FindFiles.in(table)
        .inPartitions(table.spec(), StaticDataTask.Row.of(1), StaticDataTask.Row.of(2))
        .collect();

    Assert.assertEquals(pathSet(FILE_B, FILE_C), pathSet(files));
  }

  @Test
  public void testAsOfTimestamp() {
    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    table.newAppend()
        .appendFile(FILE_B)
        .commit();

    long timestamp = System.currentTimeMillis();

    table.newAppend()
        .appendFile(FILE_C)
        .commit();

    table.newAppend()
        .appendFile(FILE_D)
        .commit();

    Iterable<DataFile> files = FindFiles.in(table).asOfTime(timestamp).collect();

    Assert.assertEquals(pathSet(FILE_A, FILE_B), pathSet(files));
  }

  @Test
  public void testSnapshotId() {
    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    table.newAppend()
        .appendFile(FILE_C)
        .commit();

    long snapshotId = table.currentSnapshot().snapshotId();

    table.newAppend()
        .appendFile(FILE_D)
        .commit();

    Iterable<DataFile> files = FindFiles.in(table).inSnapshot(snapshotId).collect();

    Assert.assertEquals(pathSet(FILE_A, FILE_B, FILE_C), pathSet(files));
  }

  @Test
  public void testCaseSensitivity() {
    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .commit();

    Iterable<DataFile> files = FindFiles.in(table)
        .caseInsensitive()
        .withMetadataMatching(Expressions.startsWith("FILE_PATH", "/path/to/data-a"))
        .collect();

    Assert.assertEquals(pathSet(FILE_A), pathSet(files));
  }

  private Set<String> pathSet(DataFile... files) {
    return Sets.newHashSet(Iterables.transform(Arrays.asList(files), file -> file.path().toString()));
  }

  private Set<String> pathSet(Iterable<DataFile> files) {
    return Sets.newHashSet(Iterables.transform(files, file -> file.path().toString()));
  }
}
