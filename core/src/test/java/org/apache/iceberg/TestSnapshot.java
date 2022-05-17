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

import java.io.File;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Tasks;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestSnapshot extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestSnapshot(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testAppendFilesFromTable() {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    // collect data files from deserialization
    Iterable<DataFile> filesToAdd = table.currentSnapshot().addedFiles();

    table.newDelete().deleteFile(FILE_A).deleteFile(FILE_B).commit();

    Snapshot oldSnapshot = table.currentSnapshot();

    AppendFiles fastAppend = table.newFastAppend();
    for (DataFile file : filesToAdd) {
      fastAppend.appendFile(file);
    }

    Snapshot newSnapshot = fastAppend.apply();
    validateSnapshot(oldSnapshot, newSnapshot, FILE_A, FILE_B);
  }

  @Test
  public void testAppendFoundFiles() {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Iterable<DataFile> filesToAdd = FindFiles.in(table)
        .inPartition(table.spec(), StaticDataTask.Row.of(0))
        .inPartition(table.spec(), StaticDataTask.Row.of(1))
        .collect();

    table.newDelete().deleteFile(FILE_A).deleteFile(FILE_B).commit();

    Snapshot oldSnapshot = table.currentSnapshot();

    AppendFiles fastAppend = table.newFastAppend();
    for (DataFile file : filesToAdd) {
      fastAppend.appendFile(file);
    }

    Snapshot newSnapshot = fastAppend.apply();
    validateSnapshot(oldSnapshot, newSnapshot, FILE_A, FILE_B);
  }

  @Test
  public void testPerThreadLatestSnapshot() throws Exception {
    File dir = temp.newFolder();
    dir.delete();
    int threadsCount = 3;
    int numberOfCommitedFilesPerThread = 1;

    String fileName = UUID.randomUUID().toString();
    DataFile file = DataFiles.builder(table.spec())
        .withPath(FileFormat.PARQUET.addExtension(fileName))
        .withRecordCount(2)
        .withFileSizeInBytes(0)
        .build();
    ExecutorService executorService = Executors.newFixedThreadPool(threadsCount);

    AtomicInteger barrier = new AtomicInteger(0);
    Set<Long> syncedSet = ConcurrentHashMap.newKeySet();
    Tasks
        .range(threadsCount)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(executorService)
        .run(index -> {
          for (int numCommittedFiles = 0; numCommittedFiles < numberOfCommitedFilesPerThread; numCommittedFiles++) {
            while (barrier.get() < numCommittedFiles * threadsCount) {
              try {
                Thread.sleep(10);
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            }
            table.newFastAppend().appendFile(file).commit();
            syncedSet.add(SnapshotProducer.getLatestSnapshotIdInThread());
            barrier.incrementAndGet();
          }
        });

    table.refresh();
    assertEquals(threadsCount * numberOfCommitedFilesPerThread,
        Lists.newArrayList(table.snapshots()).size());
    // check the number of non-identical values in syncedList which should always equal to thread number
    assertEquals(threadsCount, syncedSet.size());
  }
}
