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

package org.apache.iceberg.spark;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileRewriteCoordinator {

  private static final Logger LOG = LoggerFactory.getLogger(FileRewriteCoordinator.class);
  private static final FileRewriteCoordinator INSTANCE = new FileRewriteCoordinator();

  private final Map<String, Lock> lockMap = Maps.newConcurrentMap();
  private final Map<Pair<String, String>, Set<DataFile>> resultMap = Maps.newConcurrentMap();

  private FileRewriteCoordinator() {
  }

  public static FileRewriteCoordinator get() {
    return INSTANCE;
  }

  public void stageRewrite(Table table, String fileSetID, Set<DataFile> newDataFiles) {
    Preconditions.checkArgument(newDataFiles != null && newDataFiles.size() > 0, "Cannot stage null or empty files");
    LOG.info("Staging results of rewriting file set {} for table {}", fileSetID, table);
    Pair<String, String> id = toID(table, fileSetID);
    resultMap.put(id, newDataFiles);
  }

  public void commitRewrite(Table table, String fileSetID) {
    commitRewrite(table, ImmutableSet.of(fileSetID));
  }

  public void commitRewrite(Table table, Set<String> fileSetIDs) {
    LOG.info("Committing rewrite of file sets {} for table {}", fileSetIDs, table);

    Set<DataFile> rewrittenDataFiles = fetchRewrittenDataFiles(table, fileSetIDs);
    Set<DataFile> newDataFiles = fetchNewDataFiles(table, fileSetIDs);

    String tableUUID = tableUUID(table);
    Lock tableLock = lockMap.computeIfAbsent(tableUUID, uuid -> new ReentrantLock());

    try {
      LOG.info("Locking table {} to commit rewrite of file sets {}", table, fileSetIDs);
      tableLock.lock();

      table.newRewrite()
          .rewriteFiles(rewrittenDataFiles, newDataFiles)
          .commit();

      LOG.info("Successfully committed rewrite of file sets {} for table {}", fileSetIDs, table);

    } finally {
      LOG.info("Unlocking table {}", table);
      tableLock.unlock();
    }
  }

  private Set<DataFile> fetchRewrittenDataFiles(Table table, Set<String> fileSetIDs) {
    FileScanTaskSetManager taskSetManager = FileScanTaskSetManager.get();

    Set<DataFile> rewrittenDataFiles = Sets.newHashSet();

    for (String fileSetID : fileSetIDs) {
      List<FileScanTask> tasks = taskSetManager.fetchTasks(table, fileSetID);
      ValidationException.check(tasks != null,
          "Task set manager has no tasks for table %s with id %s",
          table, fileSetID);

      for (FileScanTask task : tasks) {
        DataFile dataFile = task.file();
        rewrittenDataFiles.add(dataFile);
      }
    }

    return rewrittenDataFiles;
  }

  private Set<DataFile> fetchNewDataFiles(Table table, Set<String> fileSetIDs) {
    List<Set<DataFile>> results = Lists.newArrayList();

    for (String fileSetID : fileSetIDs) {
      Pair<String, String> id = toID(table, fileSetID);
      Set<DataFile> result = resultMap.get(id);
      ValidationException.check(result != null,
          "No results for compacting file set %s in table %s",
          fileSetID, table);

      results.add(result);
    }

    Set<DataFile> newDataFiles = results.get(0);
    for (int index = 1; index < results.size(); index++) {
      newDataFiles = Sets.union(newDataFiles, results.get(index));
    }

    return newDataFiles;
  }

  public void abortRewrite(Table table, String fileSetID) {
    LOG.info("Aborting compaction of file set {} for table {}", fileSetID, table);
    Pair<String, String> id = toID(table, fileSetID);
    Set<DataFile> dataFiles = resultMap.remove(id);
    if (dataFiles != null) {
      LOG.info("Deleting {} uncommitted data files", dataFiles.size());
      deleteFiles(table.io(), dataFiles);
    }
  }

  public void abortRewrite(Table table, Set<String> fileSetIDs) {
    for (String fileSetID : fileSetIDs) {
      abortRewrite(table, fileSetID);
    }
  }

  private void deleteFiles(FileIO io, Iterable<DataFile> dataFiles) {
    Tasks.foreach(dataFiles)
        .noRetry()
        .suppressFailureWhenFinished()
        .onFailure((dataFile, exc) -> LOG.warn("Failed to delete: {}", dataFile.path(), exc))
        .run(dataFile -> io.deleteFile(dataFile.path().toString()));
  }

  private Pair<String, String> toID(Table table, String setID) {
    String tableUUID = tableUUID(table);
    return Pair.of(tableUUID, setID);
  }

  private String tableUUID(Table table) {
    TableOperations ops = ((HasTableOperations) table).operations();
    return ops.current().uuid();
  }
}
