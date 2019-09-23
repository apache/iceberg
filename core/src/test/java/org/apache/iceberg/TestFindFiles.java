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
        .appendFile(fileA)
        .appendFile(fileB)
        .commit();

    Iterable<DataFile> files = FindFiles.in(table).collect();

    Assert.assertEquals(pathSet(fileA, fileB), pathSet(files));
  }

  @Test
  public void testWithMetadataMatching() {
    table.newAppend()
        .appendFile(fileA)
        .appendFile(fileB)
        .appendFile(fileC)
        .appendFile(fileD)
        .commit();

    Iterable<DataFile> files = FindFiles.in(table)
        .withMetadataMatching(Expressions.startsWith("file_path", fileA.path().toString()))
        .collect();

    Assert.assertTrue(pathSet(files).size() == 1);
  }

  @Test
  public void testInPartition() {
    table.newAppend()
        .appendFile(fileA) // bucket 0
        .appendFile(fileB) // bucket 1
        .appendFile(fileC) // bucket 2
        .appendFile(fileD) // bucket 3
        .commit();

    Iterable<DataFile> files = FindFiles.in(table)
        .inPartition(table.spec(), StaticDataTask.Row.of(1))
        .inPartition(table.spec(), StaticDataTask.Row.of(2))
        .collect();

    Assert.assertEquals(pathSet(fileB, fileC), pathSet(files));
  }

  @Test
  public void testInPartitions() {
    table.newAppend()
        .appendFile(fileA) // bucket 0
        .appendFile(fileB) // bucket 1
        .appendFile(fileC) // bucket 2
        .appendFile(fileD) // bucket 3
        .commit();

    Iterable<DataFile> files = FindFiles.in(table)
        .inPartitions(table.spec(), StaticDataTask.Row.of(1), StaticDataTask.Row.of(2))
        .collect();

    Assert.assertEquals(pathSet(fileB, fileC), pathSet(files));
  }

  @Test
  public void testAsOfTimestamp() {
    table.newAppend()
        .appendFile(fileA)
        .commit();

    table.newAppend()
        .appendFile(fileB)
        .commit();

    long timestamp = System.currentTimeMillis();

    table.newAppend()
        .appendFile(fileC)
        .commit();

    table.newAppend()
        .appendFile(fileD)
        .commit();

    Iterable<DataFile> files = FindFiles.in(table).asOfTime(timestamp).collect();

    Assert.assertEquals(pathSet(fileA, fileB), pathSet(files));
  }

  @Test
  public void testSnapshotId() {
    table.newAppend()
        .appendFile(fileA)
        .appendFile(fileB)
        .commit();

    table.newAppend()
        .appendFile(fileC)
        .commit();

    long snapshotId = table.currentSnapshot().snapshotId();

    table.newAppend()
        .appendFile(fileD)
        .commit();

    Iterable<DataFile> files = FindFiles.in(table).inSnapshot(snapshotId).collect();

    Assert.assertEquals(pathSet(fileA, fileB, fileC), pathSet(files));
  }

  @Test
  public void testCaseSensitivity() {
    table.newAppend()
        .appendFile(fileA)
        .appendFile(fileB)
        .appendFile(fileC)
        .appendFile(fileD)
        .commit();

    Iterable<DataFile> files = FindFiles.in(table)
        .caseInsensitive()
        .withMetadataMatching(Expressions.startsWith("FILE_PATH", fileA.path().toString()))
        .collect();

    Assert.assertTrue(pathSet(files).size() == 1);
  }

  private Set<String> pathSet(DataFile... files) {
    return Sets.newHashSet(Iterables.transform(Arrays.asList(files), file -> file.path().toString()));
  }

  private Set<String> pathSet(Iterable<DataFile> files) {
    return Sets.newHashSet(Iterables.transform(files, file -> file.path().toString()));
  }
}
