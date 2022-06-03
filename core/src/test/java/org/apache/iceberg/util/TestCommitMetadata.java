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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTableTestBase;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestCommitMetadata {

  static final Schema TABLE_SCHEMA = new Schema(
      required(1, "id", Types.IntegerType.get(), "unique ID"),
      required(2, "data", Types.StringType.get())
  );

  // Partition spec used to create tables
  static final PartitionSpec SPEC = PartitionSpec.builderFor(TABLE_SCHEMA)
      .bucket("data", 16)
      .build();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testSetCommitMetadataConcurrently() throws IOException {
    File dir = temp.newFolder();
    dir.delete();
    int threadsCount = 3;
    Table table = new HadoopTables(new Configuration()).create(TABLE_SCHEMA, SPEC,
        ImmutableMap.of(COMMIT_NUM_RETRIES, String.valueOf(threadsCount)), dir.toURI().toString());

    String fileName = UUID.randomUUID().toString();
    DataFile file = DataFiles.builder(table.spec())
        .withPath(FileFormat.PARQUET.addExtension(fileName))
        .withRecordCount(2)
        .withFileSizeInBytes(0)
        .build();
    ExecutorService executorService = Executors.newFixedThreadPool(threadsCount, new ThreadFactory() {

      private AtomicInteger currentThreadCount = new AtomicInteger(0);

      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, "thread-" + currentThreadCount.getAndIncrement());
      }
    });

    Tasks
        .range(threadsCount)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(executorService)
        .run(index -> {
            Map<String, String> properties = Maps.newHashMap();
              properties.put("writer-thread", String.valueOf(Thread.currentThread().getName()));
              try {
                CommitMetadata.withCommitProperties(properties, () -> {
                  table.newFastAppend().appendFile(file).commit();
                  return 0;
                });
              } catch (Exception e) {
                e.printStackTrace();
              }
            }
        );
    table.refresh();
    assertEquals(threadsCount, Lists.newArrayList(table.snapshots()).size());
    Set<String> threadNames = new HashSet<>();
    for (Snapshot snapshot : table.snapshots()) {
      threadNames.add(snapshot.summary().get("writer-thread"));
    }
    assertTrue(threadNames.contains("thread-0"));
    assertTrue(threadNames.contains("thread-1"));
    assertTrue(threadNames.contains("thread-2"));
  }
}
