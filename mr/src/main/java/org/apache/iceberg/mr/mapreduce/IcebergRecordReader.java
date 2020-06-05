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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mr.IcebergMRConfig;
import org.apache.iceberg.mr.InMemoryDataModel;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

public class IcebergRecordReader<T> extends RecordReader<Void, T> {
  private TaskAttemptContext context;
  private Schema tableSchema;
  private Schema expectedSchema;
  private boolean reuseContainers;
  private boolean caseSensitive;
  private InMemoryDataModel inMemoryDataModel;
  private Map<String, Integer> namesToPos;
  private Iterator<FileScanTask> tasks;
  private T currentRow;
  private CloseableIterator<T> currentIterator;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext newContext) {
    Configuration conf = newContext.getConfiguration();
    // For now IcebergInputFormat does its own split planning and does not accept FileSplit instances
    CombinedScanTask task = ((IcebergSplit) split).getTask();
    this.context = newContext;
    this.tasks = task.files().iterator();
    this.tableSchema = IcebergMRConfig.schema(conf);
    Schema projection = IcebergMRConfig.projection(conf);
    this.expectedSchema = projection != null ? projection : tableSchema;
    this.namesToPos = buildNameToPos(expectedSchema);
    this.reuseContainers = IcebergMRConfig.reuseContainers(conf);
    this.caseSensitive = IcebergMRConfig.caseSensitive(conf);
    this.inMemoryDataModel = IcebergMRConfig.inMemoryDataModel(conf);
    this.currentIterator = open(tasks.next());
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    while (true) {
      if (currentIterator.hasNext()) {
        currentRow = currentIterator.next();
        return true;
      } else if (tasks.hasNext()) {
        currentIterator.close();
        currentIterator = open(tasks.next());
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
    return currentRow;
  }

  @Override
  public float getProgress() {
    // TODO: We could give a more accurate progress based on records read from the file. Context.getProgress does not
    // have enough information to give an accurate progress value. This isn't that easy, since we don't know how much
    // of the input split has been processed and we are pushing filters into Parquet and ORC. But we do know when a
    // file is opened and could count the number of rows returned, so we can estimate. And we could also add a row
    // count to the readers so that we can get an accurate count of rows that have been either returned or filtered
    // out.
    return context.getProgress();
  }

  @Override
  public void close() throws IOException {
    currentIterator.close();
  }

  private static Map<String, Integer> buildNameToPos(Schema expectedSchema) {
    Map<String, Integer> nameToPos = Maps.newHashMap();
    for (int pos = 0; pos < expectedSchema.asStruct().fields().size(); pos++) {
      Types.NestedField field = expectedSchema.asStruct().fields().get(pos);
      nameToPos.put(field.name(), pos);
    }
    return nameToPos;
  }

  private CloseableIterator<T> open(FileScanTask currentTask) {
    DataFile file = currentTask.file();
    // schema of rows returned by readers
    PartitionSpec spec = currentTask.spec();
    Set<Integer> idColumns =  Sets.intersection(spec.identitySourceIds(), TypeUtil.getProjectedIds(expectedSchema));
    boolean hasJoinedPartitionColumns = !idColumns.isEmpty();

    CloseableIterable<T> iterable;
    if (hasJoinedPartitionColumns) {
      Schema readDataSchema = TypeUtil.selectNot(expectedSchema, idColumns);
      Schema identityPartitionSchema = TypeUtil.select(expectedSchema, idColumns);
      iterable = CloseableIterable.transform(open(currentTask, readDataSchema),
          row -> withIdentityPartitionColumns(row, identityPartitionSchema, spec, file.partition()));
    } else {
      iterable = open(currentTask, expectedSchema);
    }

    return iterable.iterator();
  }

  private CloseableIterable<T> open(FileScanTask currentTask, Schema readSchema) {
    DataFile file = currentTask.file();
    // TODO we should make use of FileIO to create inputFile
    InputFile inputFile = HadoopInputFile.fromLocation(file.path(), context.getConfiguration());
    CloseableIterable<T> iterable;
    switch (file.format()) {
      case AVRO:
        iterable = newAvroIterable(inputFile, currentTask, readSchema);
        break;
      case ORC:
        iterable = newOrcIterable(inputFile, currentTask, readSchema);
        break;
      case PARQUET:
        iterable = newParquetIterable(inputFile, currentTask, readSchema);
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("Cannot read %s file: %s", file.format().name(), file.path()));
    }

    return iterable;
  }

  @SuppressWarnings("unchecked")
  private T withIdentityPartitionColumns(
      T row, Schema identityPartitionSchema, PartitionSpec spec, StructLike partition) {
    switch (inMemoryDataModel) {
      case PIG:
      case HIVE:
        throw new UnsupportedOperationException(
            "Adding partition columns to Pig and Hive data model are not supported yet");
      case GENERIC:
        return (T) withIdentityPartitionColumns((Record) row, identityPartitionSchema, spec, partition);
    }
    return row;
  }

  private Record withIdentityPartitionColumns(
      Record record, Schema identityPartitionSchema, PartitionSpec spec, StructLike partitionTuple) {
    List<PartitionField> partitionFields = spec.fields();
    List<Types.NestedField> identityColumns = identityPartitionSchema.columns();
    GenericRecord row = GenericRecord.create(expectedSchema.asStruct());
    namesToPos.forEach((name, pos) -> {
      Object field = record.getField(name);
      if (field != null) {
        row.set(pos, field);
      }

      // if the current name, pos points to an identity partition column, we set the
      // column at pos correctly by reading the corresponding value from partitionTuple`
      for (int i = 0; i < identityColumns.size(); i++) {
        Types.NestedField identityColumn = identityColumns.get(i);
        for (int j = 0; j < partitionFields.size(); j++) {
          PartitionField partitionField = partitionFields.get(j);
          if (name.equals(identityColumn.name()) &&
              identityColumn.fieldId() == partitionField.sourceId() &&
              "identity".equals(partitionField.transform().toString())) {
            row.set(pos, partitionTuple.get(j, spec.javaClasses()[j]));
          }
        }
      }
    });

    return row;
  }

  private CloseableIterable<T> applyResidualFiltering(CloseableIterable<T> iter, Expression residual,
                                                      Schema readSchema) {
    boolean applyResidual = IcebergMRConfig.applyResidualFiltering(context.getConfiguration());

    if (applyResidual && residual != null && residual != Expressions.alwaysTrue()) {
      Evaluator filter = new Evaluator(readSchema.asStruct(), residual, caseSensitive);
      return CloseableIterable.filter(iter, record -> filter.eval((StructLike) record));
    } else {
      return iter;
    }
  }

  private CloseableIterable<T> newAvroIterable(InputFile inputFile, FileScanTask task, Schema readSchema) {
    Avro.ReadBuilder avroReadBuilder = Avro.read(inputFile)
        .project(readSchema)
        .split(task.start(), task.length());
    if (reuseContainers) {
      avroReadBuilder.reuseContainers();
    }

    switch (inMemoryDataModel) {
      case PIG:
      case HIVE:
        //TODO implement value readers for Pig and Hive
        throw new UnsupportedOperationException("Avro support not yet supported for Pig and Hive");
      case GENERIC:
        avroReadBuilder.createReaderFunc(DataReader::create);
    }
    return applyResidualFiltering(avroReadBuilder.build(), task.residual(), readSchema);
  }

  private CloseableIterable<T> newParquetIterable(InputFile inputFile, FileScanTask task, Schema readSchema) {
    Parquet.ReadBuilder parquetReadBuilder = Parquet.read(inputFile)
        .project(readSchema)
        .filter(task.residual())
        .caseSensitive(caseSensitive)
        .split(task.start(), task.length());
    if (reuseContainers) {
      parquetReadBuilder.reuseContainers();
    }

    switch (inMemoryDataModel) {
      case PIG:
      case HIVE:
        //TODO implement value readers for Pig and Hive
        throw new UnsupportedOperationException("Parquet support not yet supported for Pig and Hive");
      case GENERIC:
        parquetReadBuilder.createReaderFunc(
            fileSchema -> GenericParquetReaders.buildReader(readSchema, fileSchema));
    }
    return applyResidualFiltering(parquetReadBuilder.build(), task.residual(), readSchema);
  }

  private CloseableIterable<T> newOrcIterable(InputFile inputFile, FileScanTask task, Schema readSchema) {
    ORC.ReadBuilder orcReadBuilder = ORC.read(inputFile)
        .project(readSchema)
        .filter(task.residual())
        .caseSensitive(caseSensitive)
        .split(task.start(), task.length());
    // ORC does not support reuse containers yet
    switch (inMemoryDataModel) {
      case PIG:
      case HIVE:
        //TODO: implement value readers for Pig and Hive
        throw new UnsupportedOperationException("ORC support not yet supported for Pig and Hive");
      case GENERIC:
        orcReadBuilder.createReaderFunc(fileSchema -> GenericOrcReader.buildReader(readSchema, fileSchema));
    }

    return applyResidualFiltering(orcReadBuilder.build(), task.residual(), readSchema);
  }
}
