/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.util;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static junit.framework.TestCase.assertEquals;

@RunWith(Parameterized.class)
public class TestCommitMetadata extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  static final DataFile FILE_A = DataFiles.builder(SPEC)
      .withPath("/path/to/data-a.parquet")
      .withFileSizeInBytes(10)
      .withPartitionPath("data_bucket=0") // easy way to set partition data for now
      .withRecordCount(1)
      .build();

  public TestCommitMetadata(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testSetCommitMetadataConcurrently() throws IOException {
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
            Map<String, String> properties = Maps.newHashMap();
            properties.put("writer-thread", String.valueOf(Thread.currentThread().getId()));
            CommitMetadata.withCommitProperties(properties, () -> {
              table.newFastAppend().appendFile(file).commit();
              return 0;
            });
            barrier.incrementAndGet();
          }
        });
    table.refresh();
    assertEquals(threadsCount * numberOfCommitedFilesPerThread, Lists.newArrayList(table.snapshots()).size());
    for (Snapshot snapshot : table.snapshots()) {
      System.out.println(snapshot.summary().get("writer-thread"));
    }
  }
}
