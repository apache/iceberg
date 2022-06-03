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

package org.apache.iceberg.spark.extensions;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.CommitMetadata;
import org.apache.iceberg.util.Tasks;
import org.junit.After;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;

public class TestCommitMetadataWithSQL extends SparkRowLevelOperationsTestBase {

  public TestCommitMetadataWithSQL(String catalogName, String implementation, Map<String, String> config,
                                   String fileFormat, boolean vectorized, String distributionMode) {
    super(catalogName, implementation, config, fileFormat, vectorized, distributionMode);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS source");
  }

  @Override
  protected Map<String, String> extraTableProperties() {
    return ImmutableMap.of(
        TableProperties.FORMAT_VERSION, "2",
        TableProperties.MERGE_MODE, "merge-on-read",
        TableProperties.COMMIT_NUM_RETRIES, "4"
    );
  }

  @Test
  public void testExtraSnapshotMetadataWithSQL() throws IOException {
    createAndInitTable("id BIGINT, dep STRING");
    sql("ALTER TABLE %s ADD PARTITION FIELD id", tableName);
    // add a data file to the 'software' partition
    append(tableName, "{ \"id\": 0, \"dep\": \"software\" }");

    int threadsCount = 3;
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
                  createOrReplaceView("source",
                      "{ \"id\": 1, \"dep\": \"finance\" }\n" +
                          "{ \"id\": 2, \"dep\": \"hardware\" }");
                  sql("MERGE INTO %s target USING source on target.id = source.id" +
                      " WHEN MATCHED THEN UPDATE SET target.dep='product'", tableName);
                  return 0;
                });
              } catch (Exception e) {
                e.printStackTrace();
              }
            }
        );
    Table table = validationCatalog.loadTable(tableIdent);
    Set<String> threadNames = new HashSet<>();
    for (Snapshot snapshot : table.snapshots()) {
      threadNames.add(snapshot.summary().get("writer-thread"));
    }
    assertTrue(threadNames.contains("thread-0"));
    assertTrue(threadNames.contains("thread-1"));
    assertTrue(threadNames.contains("thread-2"));
  }
}
