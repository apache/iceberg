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
package org.apache.iceberg.mr.mapreduce;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataTableScan;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.SystemConfigs;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.data.GenericDeleteFilter;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.SerializationUtil;
import org.apache.iceberg.util.ThreadPools;

/**
 * Generic Mrv2 InputFormat API for Iceberg.
 *
 * @param <T> T is the in memory data model which can either be Pig tuples, Hive rows. Default is
 *     Iceberg records
 */
public class IcebergInputFormat<T> extends InputFormat<Void, T> {
  /**
   * Configures the {@code Job} to use the {@code IcebergInputFormat} and returns a helper to add
   * further configuration.
   *
   * @param job the {@code Job} to configure
   */
  public static InputFormatConfig.ConfigBuilder configure(Job job) {
    job.setInputFormatClass(IcebergInputFormat.class);
    return new InputFormatConfig.ConfigBuilder(job.getConfiguration());
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) {
    Configuration conf = context.getConfiguration();
    Table table = Catalogs.loadTable(conf);
    final ExecutorService workerPool =
        ThreadPools.newFixedThreadPool(
            "iceberg-plan-worker-pool",
            conf.getInt(
                SystemConfigs.WORKER_THREAD_POOL_SIZE.propertyKey(),
                ThreadPools.WORKER_THREAD_POOL_SIZE));
    try {
      return planInputSplits(table, conf, workerPool);
    } finally {
      workerPool.shutdown();
    }
  }

  private List<InputSplit> planInputSplits(
      Table table, Configuration conf, ExecutorService workerPool) {
    TableScan scan =
        table
            .newScan()
            .caseSensitive(
                conf.getBoolean(
                    InputFormatConfig.CASE_SENSITIVE, InputFormatConfig.CASE_SENSITIVE_DEFAULT));
    long snapshotId = conf.getLong(InputFormatConfig.SNAPSHOT_ID, -1);
    if (snapshotId != -1) {
      scan = scan.useSnapshot(snapshotId);
    }
    long asOfTime = conf.getLong(InputFormatConfig.AS_OF_TIMESTAMP, -1);
    if (asOfTime != -1) {
      scan = scan.asOfTime(asOfTime);
    }
    long splitSize = conf.getLong(InputFormatConfig.SPLIT_SIZE, 0);
    if (splitSize > 0) {
      scan = scan.option(TableProperties.SPLIT_SIZE, String.valueOf(splitSize));
    }
    String schemaStr = conf.get(InputFormatConfig.READ_SCHEMA);
    if (schemaStr != null) {
      scan.project(SchemaParser.fromJson(schemaStr));
    }
    String[] selectedColumns = conf.getStrings(InputFormatConfig.SELECTED_COLUMNS);
    if (selectedColumns != null) {
      scan.select(selectedColumns);
    }

    // TODO add a filter parser to get rid of Serialization
    Expression filter =
        SerializationUtil.deserializeFromBase64(conf.get(InputFormatConfig.FILTER_EXPRESSION));
    if (filter != null) {
      scan = scan.filter(filter);
    }

    List<InputSplit> splits = Lists.newArrayList();
    scan = scan.planWith(workerPool);
    try (CloseableIterable<CombinedScanTask> tasksIterable = scan.planTasks()) {
      Table serializableTable = SerializableTable.copyOf(table);
      tasksIterable.forEach(
          task -> {
            splits.add(new IcebergSplit(serializableTable, conf, task));
          });
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to close table scan: %s", scan), e);
    }

    // if enabled, do not serialize FileIO hadoop config to decrease split size
    // However, do not skip serialization for metatable queries, because some metadata tasks cache
    // the IO object and we
    // wouldn't be able to inject the config into these tasks on the deserializer-side, unlike for
    // standard queries
    if (scan instanceof DataTableScan) {
      checkAndSkipIoConfigSerialization(conf, table);
    }

    return splits;
  }

  /**
   * If enabled, it ensures that the FileIO's hadoop configuration will not be serialized. This
   * might be desirable for decreasing the overall size of serialized table objects.
   *
   * <p>Note: Skipping FileIO config serialization in this fashion might in turn necessitate calling
   * {@link #checkAndSetIoConfig(Configuration, Table)} on the deserializer-side to enable
   * subsequent use of the FileIO.
   *
   * @param config Configuration to set for FileIO in a transient manner, if enabled
   * @param table The Iceberg table object
   */
  private void checkAndSkipIoConfigSerialization(Configuration config, Table table) {
    if (table != null
        && config.getBoolean(
            InputFormatConfig.CONFIG_SERIALIZATION_DISABLED,
            InputFormatConfig.CONFIG_SERIALIZATION_DISABLED_DEFAULT)
        && table.io() instanceof HadoopConfigurable) {
      ((HadoopConfigurable) table.io())
          .serializeConfWith(conf -> new NonSerializingConfig(config)::get);
    }
  }

  /**
   * If enabled, it populates the FileIO's hadoop configuration with the input config object. This
   * might be necessary when the table object was serialized without the FileIO config.
   *
   * @param config Configuration to set for FileIO, if enabled
   * @param table The Iceberg table object
   */
  private static void checkAndSetIoConfig(Configuration config, Table table) {
    if (table != null
        && config.getBoolean(
            InputFormatConfig.CONFIG_SERIALIZATION_DISABLED,
            InputFormatConfig.CONFIG_SERIALIZATION_DISABLED_DEFAULT)
        && table.io() instanceof HadoopConfigurable) {
      ((HadoopConfigurable) table.io()).setConf(config);
    }
  }

  @Override
  public RecordReader<Void, T> createRecordReader(InputSplit split, TaskAttemptContext context) {
    return new IcebergRecordReader<>();
  }

  private static class NonSerializingConfig implements Serializable {

    private final transient Configuration conf;

    NonSerializingConfig(Configuration conf) {
      this.conf = conf;
    }

    public Configuration get() {
      if (conf == null) {
        throw new IllegalStateException(
            "Configuration was not serialized on purpose but was not set manually either");
      }

      return conf;
    }
  }

  private static final class IcebergRecordReader<T> extends RecordReader<Void, T> {

    private TaskAttemptContext context;
    private Schema tableSchema;
    private Schema expectedSchema;
    private String nameMapping;
    private boolean reuseContainers;
    private boolean caseSensitive;
    private Iterator<FileScanTask> tasks;
    private T current;
    private CloseableIterator<T> currentIterator;
    private FileIO io;
    private EncryptionManager encryptionManager;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext newContext) {
      Configuration conf = newContext.getConfiguration();
      // For now IcebergInputFormat does its own split planning and does not accept FileSplit
      // instances
      CombinedScanTask task = ((IcebergSplit) split).task();
      this.context = newContext;
      Table table = ((IcebergSplit) split).table();
      checkAndSetIoConfig(conf, table);
      this.io = table.io();
      this.encryptionManager = table.encryption();
      this.tasks = task.files().iterator();
      this.tableSchema = InputFormatConfig.tableSchema(conf);
      this.nameMapping = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
      this.caseSensitive =
          conf.getBoolean(
              InputFormatConfig.CASE_SENSITIVE, InputFormatConfig.CASE_SENSITIVE_DEFAULT);
      this.expectedSchema = readSchema(conf, tableSchema, caseSensitive);
      this.reuseContainers = conf.getBoolean(InputFormatConfig.REUSE_CONTAINERS, false);
      this.currentIterator = open(tasks.next(), expectedSchema).iterator();
    }

    @Override
    public boolean nextKeyValue() throws IOException {
      while (true) {
        if (currentIterator.hasNext()) {
          current = currentIterator.next();
          return true;
        } else if (tasks.hasNext()) {
          currentIterator.close();
          currentIterator = open(tasks.next(), expectedSchema).iterator();
        } else {
          currentIterator.close();
          return false;
        }
      }
    }

    @Override
    public Void getCurrentKey() {
      return null;
    }

    @Override
    public T getCurrentValue() {
      return current;
    }

    @Override
    public float getProgress() {
      // TODO: We could give a more accurate progress based on records read from the file.
      // Context.getProgress does not
      // have enough information to give an accurate progress value. This isn't that easy, since we
      // don't know how much
      // of the input split has been processed and we are pushing filters into Parquet and ORC. But
      // we do know when a
      // file is opened and could count the number of rows returned, so we can estimate. And we
      // could also add a row
      // count to the readers so that we can get an accurate count of rows that have been either
      // returned or filtered
      // out.
      return context.getProgress();
    }

    @Override
    public void close() throws IOException {
      currentIterator.close();
    }

    private CloseableIterable<T> openTask(FileScanTask currentTask, Schema readSchema) {
      DataFile file = currentTask.file();
      InputFile inputFile =
          encryptionManager.decrypt(
              EncryptedFiles.encryptedInput(io.newInputFile(file.location()), file.keyMetadata()));

      ReadBuilder readBuilder =
          FormatModelRegistry.readBuilder(file.format(), Record.class, inputFile);

      if (reuseContainers) {
        readBuilder = readBuilder.reuseContainers();
      }

      if (nameMapping != null) {
        readBuilder = readBuilder.withNameMapping(NameMappingParser.fromJson(nameMapping));
      }

      return applyResidualFiltering(
          (CloseableIterable<T>)
              readBuilder
                  .project(readSchema)
                  .split(currentTask.start(), currentTask.length())
                  .caseSensitive(caseSensitive)
                  .filter(currentTask.residual())
                  .build(),
          currentTask.residual(),
          readSchema);
    }

    @SuppressWarnings("unchecked")
    private CloseableIterable<T> open(FileScanTask currentTask, Schema readSchema) {
      DeleteFilter deletes = new GenericDeleteFilter(io, currentTask, tableSchema, readSchema);
      Schema requiredSchema = deletes.requiredSchema();
      return deletes.filter(openTask(currentTask, requiredSchema));
    }

    private CloseableIterable<T> applyResidualFiltering(
        CloseableIterable<T> iter, Expression residual, Schema readSchema) {
      boolean applyResidual =
          !context.getConfiguration().getBoolean(InputFormatConfig.SKIP_RESIDUAL_FILTERING, false);

      if (applyResidual && residual != null && residual != Expressions.alwaysTrue()) {
        // Date and timestamp values are not the correct type for Evaluator.
        // Wrapping to return the expected type.
        InternalRecordWrapper wrapper = new InternalRecordWrapper(readSchema.asStruct());
        Evaluator filter = new Evaluator(readSchema.asStruct(), residual, caseSensitive);
        return CloseableIterable.filter(
            iter, record -> filter.eval(wrapper.wrap((StructLike) record)));
      } else {
        return iter;
      }
    }

    private static Schema readSchema(
        Configuration conf, Schema tableSchema, boolean caseSensitive) {
      Schema readSchema = InputFormatConfig.readSchema(conf);

      if (readSchema != null) {
        return readSchema;
      }

      String[] selectedColumns = InputFormatConfig.selectedColumns(conf);
      if (selectedColumns == null) {
        return tableSchema;
      }

      return caseSensitive
          ? tableSchema.select(selectedColumns)
          : tableSchema.caseInsensitiveSelect(selectedColumns);
    }
  }
}
