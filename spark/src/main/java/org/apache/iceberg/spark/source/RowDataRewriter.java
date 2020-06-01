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

package org.apache.iceberg.spark.source;

import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.streaming.MicroBatchExecution;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowDataRewriter implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(RowDataRewriter.class);

  private final transient JavaSparkContext sparkContext;
  private final Broadcast<FileIO> fileIO;
  private final Broadcast<EncryptionManager> encryptionManager;
  private final String tableSchema;
  private final Writer.WriterFactory writerFactory;
  private final boolean caseSensitive;

  public RowDataRewriter(Table table, JavaSparkContext sparkContext, PartitionSpec spec, boolean caseSensitive,
                         Broadcast<FileIO> fileIO, Broadcast<EncryptionManager> encryptionManager,
                         long targetDataFileSizeInBytes) {
    this.sparkContext = sparkContext;
    this.fileIO = fileIO;
    this.encryptionManager = encryptionManager;

    this.caseSensitive = caseSensitive;
    this.tableSchema = SchemaParser.toJson(table.schema());

    String formatString = table.properties().getOrDefault(
        TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    FileFormat fileFormat = FileFormat.valueOf(formatString.toUpperCase(Locale.ENGLISH));
    this.writerFactory = new Writer.WriterFactory(spec, fileFormat, table.locationProvider(), table.properties(),
        fileIO, encryptionManager, targetDataFileSizeInBytes, table.schema(), SparkSchemaUtil.convert(table.schema()));
  }

  public List<DataFile> rewriteDataForTasks(CloseableIterable<CombinedScanTask> tasks) {
    List<CombinedScanTask> taskList = Lists.newArrayList(tasks);
    int parallelism = taskList.size();

    JavaRDD<CombinedScanTask> taskRDD = sparkContext.parallelize(taskList, parallelism);
    JavaRDD<Writer.TaskCommit> taskCommitRDD = taskRDD.map(task -> rewriteDataForTask(task));

    return taskCommitRDD.collect().stream()
        .flatMap(taskCommit -> Arrays.stream(taskCommit.files()))
        .collect(Collectors.toList());
  }

  private Writer.TaskCommit rewriteDataForTask(CombinedScanTask task) throws Exception {
    TaskContext context = TaskContext.get();

    RowDataReader dataReader = new RowDataReader(task, SchemaParser.fromJson(tableSchema),
        SchemaParser.fromJson(tableSchema), fileIO.value(), encryptionManager.value(), caseSensitive);

    int partitionId = context.partitionId();
    long taskId = context.taskAttemptId();
    long epochId = Long.parseLong(
        Optional.ofNullable(context.getLocalProperty(MicroBatchExecution.BATCH_ID_KEY())).orElse("0"));
    DataWriter<InternalRow> dataWriter = writerFactory.createDataWriter(partitionId, taskId, epochId);

    Throwable originalThrowable = null;
    try {
      while (dataReader.next()) {
        InternalRow row = dataReader.get();
        dataWriter.write(row);
      }

      dataReader.close();
      return (Writer.TaskCommit) dataWriter.commit();

    } catch (Throwable t) {
      originalThrowable = t;
      try {
        LOG.error("Aborting task", originalThrowable);
        context.markTaskFailed(originalThrowable);

        LOG.error("Aborting commit for partition {} (task {}, attempt {}, stage {}.{})",
            partitionId, taskId, context.attemptNumber(), context.stageId(), context.stageAttemptNumber());
        dataReader.close();
        dataWriter.abort();
        LOG.error("Aborted commit for partition {} (task {}, attempt {}, stage {}.{})",
            partitionId, taskId, context.taskAttemptId(), context.stageId(), context.stageAttemptNumber());

      } catch (Throwable inner) {
        if (originalThrowable != inner) {
          originalThrowable.addSuppressed(inner);
          LOG.warn("Suppressing exception in catch: {}", inner.getMessage(), inner);
        }

        // Wrap throwable in an Exception
        throw new Exception(originalThrowable);
      }
    }

    return new Writer.TaskCommit();
  }
}
