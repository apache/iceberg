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

package org.apache.iceberg.mr.hive;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An Iceberg table committer for adding data files to the Iceberg tables.
 * Currently independent of the Hive ACID transactions.
 */
public final class HiveIcebergOutputCommitter extends OutputCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergOutputCommitter.class);

  @Override
  public void setupJob(JobContext jobContext) {
    // do nothing.
  }

  @Override
  public void setupTask(TaskAttemptContext taskAttemptContext) {
    // do nothing.
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext context) {
    // We need to commit if this is the last phase of a MapReduce process
    return TaskType.REDUCE.equals(context.getTaskAttemptID().getTaskID().getTaskType()) ||
        context.getJobConf().getNumReduceTasks() == 0;
  }

  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    TaskAttemptID attemptID = context.getTaskAttemptID();
    String commitFileLocation = LocationHelper.generateToCommitFileLocation(context.getJobConf(), attemptID);
    HiveIcebergRecordWriter writer = HiveIcebergOutputFormat.writers.remove(attemptID);

    Set<ClosedFileData> closedFiles = Collections.emptySet();
    if (writer != null) {
      closedFiles = writer.closedFileData();
    }

    // Create the committed file for the task
    createToCommitFile(closedFiles, commitFileLocation, new HadoopFileIO(context.getJobConf()));
  }

  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    // Clean up writer data from the local store
    HiveIcebergRecordWriter writer = HiveIcebergOutputFormat.writers.remove(context.getTaskAttemptID());

    // Remove files if it was not done already
    writer.close(true);
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    JobConf conf = jobContext.getJobConf();
    // If there are reducers, then every reducer will generate a result file.
    // If this is a map only task, then every mapper will generate a result file.
    int expectedFiles = conf.getNumReduceTasks() != 0 ? conf.getNumReduceTasks() : conf.getNumMapTasks();
    Table table = Catalogs.loadTable(conf);

    ExecutorService executor = null;
    try {
      // Creating executor service for parallel handling of file reads
      executor = Executors.newFixedThreadPool(
          conf.getInt(InputFormatConfig.COMMIT_THREAD_POOL_SIZE, InputFormatConfig.COMMIT_THREAD_POOL_SIZE_DEFAULT),
          new ThreadFactoryBuilder()
              .setDaemon(false)
              .setPriority(Thread.NORM_PRIORITY)
              .setNameFormat("iceberg-commit-pool-%d")
              .build());

      Set<DataFile> dataFiles = new ConcurrentHashMap<>().newKeySet();

      // Reading the committed files. The assumption here is that the taskIds are generated in sequential order
      // starting from 0.
      Tasks.range(expectedFiles)
          .executeWith(executor)
          .retry(3)
          .run(taskId -> {
            String taskFileName = LocationHelper.generateToCommitFileLocation(conf, jobContext.getJobID(), taskId);
            Set<ClosedFileData> closedFiles = readToCommitFile(taskFileName, table.io());

            // If the data is not empty add to the table
            if (!closedFiles.isEmpty()) {
              closedFiles.forEach(file -> {
                DataFiles.Builder builder = DataFiles.builder(table.spec())
                    .withPath(file.fileName())
                    .withFormat(file.fileFormat())
                    .withFileSizeInBytes(file.length())
                    .withPartition(file.partitionKey())
                    .withMetrics(file.metrics());
                dataFiles.add(builder.build());
              });
            }
          });

      if (dataFiles.size() > 0) {
        // Appending data files to the table
        AppendFiles append = table.newAppend();
        Set<String> addedFiles = new HashSet<>(dataFiles.size());
        dataFiles.forEach(dataFile -> {
          append.appendFile(dataFile);
          addedFiles.add(dataFile.path().toString());
        });
        append.commit();
        LOG.info("Iceberg write is committed for {} with files {}", table, addedFiles);
      } else {
        LOG.info("Iceberg write is committed for {} with no new files", table);
      }

      // Calling super to cleanupJob if something more is needed
      cleanupJob(jobContext);

    } finally {
      if (executor != null) {
        executor.shutdown();
      }
    }
  }

  @Override
  public void abortJob(JobContext context, int status) throws IOException {
    // Remove the result directory for the failed job
    Tasks.foreach(LocationHelper.generateJobLocation(context.getJobConf(), context.getJobID()))
        .retry(3)
        .suppressFailureWhenFinished()
        .onFailure((file, exc) -> LOG.debug("Failed on to remove directory {} on abort job", file, exc))
        .run(file -> {
          Path toDelete = new Path(file);
          FileSystem fs = Util.getFs(toDelete, context.getJobConf());
          try {
            fs.delete(toDelete, true /* recursive */);
          } catch (IOException e) {
            throw new RuntimeIOException(e, "Failed to delete job directory: %s", file);
          }
        });
    cleanupJob(context);
  }

  private static void createToCommitFile(Set<ClosedFileData> closedFiles, String location, FileIO io)
      throws IOException {

    OutputFile commitFile = io.newOutputFile(location);
    ObjectOutputStream oos = new ObjectOutputStream(commitFile.createOrOverwrite());
    oos.writeObject(closedFiles);
    oos.close();
    LOG.debug("Iceberg committed file is created {}", commitFile);
  }

  private static Set<ClosedFileData> readToCommitFile(String toCommitFileLocation, FileIO io) {
    try (ObjectInputStream ois = new ObjectInputStream(io.newInputFile(toCommitFileLocation).newStream())) {
      return (Set<ClosedFileData>) ois.readObject();
    } catch (ClassNotFoundException | IOException e) {
      throw new NotFoundException("Can not read or parse committed file: %s", toCommitFileLocation);
    }
  }
}
