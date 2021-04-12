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
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Pair;

public class DataCompactionJobCoordinator {

  private static final Map<Pair<String, String>, List<FileScanTask>> TASKS = Maps.newHashMap();
  private static final Map<Pair<String, String>, List<DataFile>> NEW_DATA_FILES = Maps.newHashMap();

  private DataCompactionJobCoordinator() {
  }

  public static String stageJob(Table table, List<FileScanTask> tasks) {
    String jobID = UUID.randomUUID().toString();
    Pair<String, String> id = toID(table, jobID);
    TASKS.put(id, tasks);
    return jobID;
  }

  public static List<FileScanTask> getTasks(Table table, String jobID) {
    Pair<String, String> id = toID(table, jobID);
    return TASKS.get(id);
  }

  public static void commitJob(Table table, String jobID, List<DataFile> newDataFiles) {
    Pair<String, String> id = toID(table, jobID);
    NEW_DATA_FILES.put(id, newDataFiles);
  }

  public static JobResult getJobResult(Table table, String jobID) {
    Pair<String, String> id = toID(table, jobID);
    List<FileScanTask> tasks = TASKS.get(id);
    List<DataFile> newDataFiles = NEW_DATA_FILES.get(id);
    return new JobResult(tasks, newDataFiles);
  }

  public static void invalidateJob(Table table, String jobID) {
    Pair<String, String> id = toID(table, jobID);
    TASKS.remove(id);
    NEW_DATA_FILES.remove(id);
  }

  private static Pair<String, String> toID(Table table, String jobID) {
    TableOperations ops = ((HasTableOperations) table).operations();
    String tableUUID = ops.current().uuid();
    return Pair.of(tableUUID, jobID);
  }

  public static class JobResult {
    private final List<FileScanTask> tasks;
    private final List<DataFile> newDataFiles;
    private volatile List<DataFile> rewrittenDataFiles;

    public JobResult(List<FileScanTask> tasks, List<DataFile> newDataFiles) {
      this.tasks = tasks;
      this.newDataFiles = newDataFiles;
    }

    public List<DataFile> newDataFiles() {
      return newDataFiles;
    }

    public List<DataFile> rewrittenDataFiles() {
      if (rewrittenDataFiles == null) {
        synchronized (this) {
          if (rewrittenDataFiles == null) {
            this.rewrittenDataFiles = toDataFiles(tasks);
          }
        }
      }

      return rewrittenDataFiles;
    }

    private List<DataFile> toDataFiles(List<FileScanTask> fileScanTasks) {
      return fileScanTasks.stream().map(FileScanTask::file).collect(Collectors.toList());
    }
  }
}
