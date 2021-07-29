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

import java.io.Serializable;
import java.util.Collection;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.SortedPosDeleteWriter;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EqualityDeleteRewriter implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(EqualityDeleteRewriter.class);
  private final PartitionSpec spec;
  private final Schema schema;
  private final Broadcast<FileIO> io;
  private final Broadcast<EncryptionManager> encryptionManager;
  private final LocationProvider locations;
  private final boolean caseSensitive;
  private final FileFormat format;
  private final Table table;


  public EqualityDeleteRewriter(Table table, boolean caseSensitive,
                                Broadcast<FileIO> io, Broadcast<EncryptionManager> encryptionManager) {
    this.table = table;
    this.spec = table.spec();
    this.schema = table.schema();
    this.io = io;
    this.encryptionManager = encryptionManager;
    this.locations = table.locationProvider();
    this.caseSensitive = caseSensitive;
    String formatString = table.properties().getOrDefault(
        TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    this.format = FileFormat.valueOf(formatString.toUpperCase(Locale.ENGLISH));
  }

  public Set<DeleteFile> toPosDeletes(JavaRDD<Pair<StructLike, CombinedScanTask>> taskRDD) {
    JavaRDD<Set<DeleteFile>> dataFilesRDD = taskRDD.map(this::toPosDeletes);

    return dataFilesRDD.collect().stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toSet());
  }

  public Set<DeleteFile> toPosDeletes(Pair<StructLike, CombinedScanTask> task) throws Exception {
    TaskContext context = TaskContext.get();
    int partitionId = context.partitionId();
    long taskId = context.taskAttemptId();

    Schema metaSchema = new Schema(MetadataColumns.FILE_PATH, MetadataColumns.ROW_POSITION);
    Schema expectedSchema = TypeUtil.join(metaSchema, schema);

    DeleteRowReader deleteRowReader = new DeleteRowReader(task.second(), table, expectedSchema, caseSensitive,
        FileContent.EQUALITY_DELETES);

    StructType structType = SparkSchemaUtil.convert(schema);
    SparkAppenderFactory appenderFactory = SparkAppenderFactory.builderFor(table, schema, structType)
        .spec(spec)
        .build();

    OutputFileFactory fileFactory = new OutputFileFactory(
        spec, format, locations, io.value(), encryptionManager.value(), partitionId, taskId);

    SortedPosDeleteWriter<InternalRow> posDeleteWriter =
        new SortedPosDeleteWriter<>(appenderFactory, fileFactory, format, task.first());

    try {
      while (deleteRowReader.next()) {
        InternalRow row = deleteRowReader.get();
        posDeleteWriter.delete(row.getString(0), row.getLong(1));
      }

      deleteRowReader.close();
      deleteRowReader = null;

      return Sets.newHashSet(posDeleteWriter.complete());

    } catch (Throwable originalThrowable) {
      try {
        LOG.error("Aborting task", originalThrowable);
        context.markTaskFailed(originalThrowable);

        LOG.error("Aborting commit for partition {} (task {}, attempt {}, stage {}.{})",
            partitionId, taskId, context.attemptNumber(), context.stageId(), context.stageAttemptNumber());
        if (deleteRowReader != null) {
          deleteRowReader.close();
        }

        // clean up files created by this writer
        Tasks.foreach(Iterables.concat(posDeleteWriter.complete()))
            .throwFailureWhenFinished()
            .noRetry()
            .run(file -> io.value().deleteFile(file.path().toString()));

        LOG.error("Aborted commit for partition {} (task {}, attempt {}, stage {}.{})",
            partitionId, taskId, context.taskAttemptId(), context.stageId(), context.stageAttemptNumber());

      } catch (Throwable inner) {
        if (originalThrowable != inner) {
          originalThrowable.addSuppressed(inner);
          LOG.warn("Suppressing exception in catch: {}", inner.getMessage(), inner);
        }
      }

      if (originalThrowable instanceof Exception) {
        throw originalThrowable;
      } else {
        throw new RuntimeException(originalThrowable);
      }
    }
  }
}
