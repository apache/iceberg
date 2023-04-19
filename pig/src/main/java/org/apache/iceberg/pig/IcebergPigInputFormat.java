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
package org.apache.iceberg.pig;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.SerializationUtil;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.impl.util.ObjectSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergPigInputFormat<T> extends InputFormat<Void, T> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergPigInputFormat.class);

  static final String ICEBERG_SCHEMA = "iceberg.schema";
  static final String ICEBERG_PROJECTED_FIELDS = "iceberg.projected.fields";
  static final String ICEBERG_FILTER_EXPRESSION = "iceberg.filter.expression";

  private final Table table;
  private final String signature;
  private List<InputSplit> splits;

  IcebergPigInputFormat(Table table, String signature) {
    this.table = table;
    this.signature = signature;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    if (splits != null) {
      LOG.info("Returning cached splits: {}", splits.size());
      return splits;
    }

    splits = Lists.newArrayList();

    TableScan scan = table.newScan();

    // Apply Filters
    Expression filterExpression =
        (Expression)
            ObjectSerializer.deserialize(
                context.getConfiguration().get(scope(ICEBERG_FILTER_EXPRESSION)));
    LOG.info("[{}]: iceberg filter expressions: {}", signature, filterExpression);

    if (filterExpression != null) {
      LOG.info("Filter Expression: {}", filterExpression);
      scan = scan.filter(filterExpression);
    }

    // Wrap in Splits
    try (CloseableIterable<CombinedScanTask> tasks = scan.planTasks()) {
      tasks.forEach(scanTask -> splits.add(new IcebergSplit(scanTask)));
    }

    return splits;
  }

  @Override
  public RecordReader<Void, T> createRecordReader(InputSplit split, TaskAttemptContext context) {
    return new IcebergRecordReader<>();
  }

  private static class IcebergSplit extends InputSplit implements Writable {
    private static final String[] ANYWHERE = new String[] {"*"};

    private CombinedScanTask task;

    IcebergSplit(CombinedScanTask task) {
      this.task = task;
    }

    @Override
    public long getLength() {
      return task.files().stream().mapToLong(FileScanTask::length).sum();
    }

    @Override
    public String[] getLocations() {
      return ANYWHERE;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      byte[] data = SerializationUtil.serializeToBytes(this.task);
      out.writeInt(data.length);
      out.write(data);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      byte[] data = new byte[in.readInt()];
      in.readFully(data);

      this.task = SerializationUtil.deserializeFromBytes(data);
    }
  }

  private String scope(String key) {
    return key + '.' + signature;
  }

  public class IcebergRecordReader<T> extends RecordReader<Void, T> {
    private TaskAttemptContext context;

    private Iterator<FileScanTask> tasks;

    private CloseableIterable reader;
    private Iterator<T> recordIterator;
    private T currentRecord;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext initContext) throws IOException {
      this.context = initContext;

      CombinedScanTask task = ((IcebergSplit) split).task;
      this.tasks = task.files().iterator();

      advance();
    }

    @SuppressWarnings("unchecked")
    private boolean advance() throws IOException {
      if (reader != null) {
        reader.close();
      }

      if (!tasks.hasNext()) {
        return false;
      }

      FileScanTask currentTask = tasks.next();

      Schema tableSchema =
          (Schema)
              ObjectSerializer.deserialize(context.getConfiguration().get(scope(ICEBERG_SCHEMA)));
      LOG.debug("[{}]: Task table schema: {}", signature, tableSchema);

      List<String> projectedFields =
          (List<String>)
              ObjectSerializer.deserialize(
                  context.getConfiguration().get(scope(ICEBERG_PROJECTED_FIELDS)));
      LOG.debug("[{}]: Task projected fields: {}", signature, projectedFields);

      Schema projectedSchema =
          projectedFields != null ? SchemaUtil.project(tableSchema, projectedFields) : tableSchema;

      PartitionSpec spec = currentTask.asFileScanTask().spec();
      DataFile file = currentTask.file();
      InputFile inputFile = HadoopInputFile.fromLocation(file.path(), context.getConfiguration());

      Set<Integer> idColumns = spec.identitySourceIds();

      // schema needed for the projection and filtering
      boolean hasJoinedPartitionColumns = !idColumns.isEmpty();

      switch (file.format()) {
        case PARQUET:
          Map<Integer, Object> partitionValueMap = Maps.newHashMap();

          if (hasJoinedPartitionColumns) {

            Schema readSchema = TypeUtil.selectNot(projectedSchema, idColumns);
            Schema projectedPartitionSchema = TypeUtil.select(projectedSchema, idColumns);

            Map<String, Integer> partitionSpecFieldIndexMap = Maps.newHashMap();
            for (int i = 0; i < spec.fields().size(); i++) {
              partitionSpecFieldIndexMap.put(spec.fields().get(i).name(), i);
            }

            for (Types.NestedField field : projectedPartitionSchema.columns()) {
              int partitionIndex = partitionSpecFieldIndexMap.get(field.name());

              Object partitionValue = file.partition().get(partitionIndex, Object.class);
              partitionValueMap.put(
                  field.fieldId(), convertPartitionValue(field.type(), partitionValue));
            }

            reader =
                Parquet.read(inputFile)
                    .project(readSchema)
                    .split(currentTask.start(), currentTask.length())
                    .filter(currentTask.residual())
                    .createReaderFunc(
                        fileSchema ->
                            PigParquetReader.buildReader(
                                fileSchema, projectedSchema, partitionValueMap))
                    .build();
          } else {
            reader =
                Parquet.read(inputFile)
                    .project(projectedSchema)
                    .split(currentTask.start(), currentTask.length())
                    .filter(currentTask.residual())
                    .createReaderFunc(
                        fileSchema ->
                            PigParquetReader.buildReader(
                                fileSchema, projectedSchema, partitionValueMap))
                    .build();
          }

          recordIterator = reader.iterator();

          break;
        default:
          throw new UnsupportedOperationException("Unsupported file format: " + file.format());
      }

      return true;
    }

    private Object convertPartitionValue(Type type, Object value) {
      if (type.typeId() == Types.BinaryType.get().typeId()) {
        return new DataByteArray(ByteBuffers.toByteArray((ByteBuffer) value));
      }

      return value;
    }

    @Override
    public boolean nextKeyValue() throws IOException {
      if (recordIterator.hasNext()) {
        currentRecord = recordIterator.next();
        return true;
      }

      while (advance()) {
        if (recordIterator.hasNext()) {
          currentRecord = recordIterator.next();
          return true;
        }
      }

      return false;
    }

    @Override
    public Void getCurrentKey() {
      return null;
    }

    @Override
    public T getCurrentValue() {
      return currentRecord;
    }

    @Override
    public float getProgress() {
      return 0;
    }

    @Override
    public void close() {}
  }
}
