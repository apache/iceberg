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

package org.apache.iceberg.data;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.Accessor;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.deletes.Deletes;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimap;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.iceberg.util.ProjectStructLike;
import org.apache.iceberg.util.StructLikeSet;

class GenericReader implements Serializable {
  private static final Schema POS_DELETE_SCHEMA = new Schema(
      MetadataColumns.DELETE_FILE_PATH,
      MetadataColumns.DELETE_FILE_POS);

  private final FileIO io;
  private final Schema projection;
  private final boolean caseSensitive;
  private final boolean reuseContainers;

  GenericReader(TableScan scan, boolean reuseContainers) {
    this.io = scan.table().io();
    this.projection = scan.schema();
    this.caseSensitive = scan.isCaseSensitive();
    this.reuseContainers = reuseContainers;
  }

  CloseableIterator<Record> open(CloseableIterable<CombinedScanTask> tasks) {
    Iterable<FileScanTask> fileTasks = Iterables.concat(Iterables.transform(tasks, CombinedScanTask::files));
    return CloseableIterable.concat(Iterables.transform(fileTasks, this::open)).iterator();
  }

  public CloseableIterable<Record> open(CombinedScanTask task) {
    return new CombinedTaskIterable(task);
  }

  public CloseableIterable<Record> open(FileScanTask task) {
    List<DeleteFile> posDeletes = Lists.newArrayList();
    List<DeleteFile> eqDeletes = Lists.newArrayList();
    for (DeleteFile delete : task.deletes()) {
      switch (delete.content()) {
        case POSITION_DELETES:
          posDeletes.add(delete);
          break;
        case EQUALITY_DELETES:
          eqDeletes.add(delete);
          break;
        default:
          throw new UnsupportedOperationException("Unknown delete file content: " + delete.content());
      }
    }

    Schema fileProjection = fileProjection(!posDeletes.isEmpty());

    CloseableIterable<Record> records = openFile(task, fileProjection);
    records = applyPosDeletes(records, fileProjection, task.file().path(), posDeletes, task.file());
    records = applyEqDeletes(records, fileProjection, eqDeletes, task.file());
    records = applyResidual(records, fileProjection, task.residual());

    return records;
  }

  private Schema fileProjection(boolean hasPosDeletes) {
    if (hasPosDeletes) {
      List<Types.NestedField> columns = Lists.newArrayList(projection.columns());
      columns.add(MetadataColumns.ROW_POSITION);
      return new Schema(columns);
    }

    return projection;
  }

  private CloseableIterable<Record> applyResidual(CloseableIterable<Record> records, Schema recordSchema,
                                                  Expression residual) {
    if (residual != null && residual != Expressions.alwaysTrue()) {
      InternalRecordWrapper wrapper = new InternalRecordWrapper(recordSchema.asStruct());
      Evaluator filter = new Evaluator(recordSchema.asStruct(), residual, caseSensitive);
      return CloseableIterable.filter(records, record -> filter.eval(wrapper.wrap(record)));
    }

    return records;
  }

  private CloseableIterable<Record> applyEqDeletes(CloseableIterable<Record> records, Schema recordSchema,
                                                   List<DeleteFile> eqDeletes, DataFile dataFile) {
    if (eqDeletes.isEmpty()) {
      return records;
    }

    Multimap<Set<Integer>, DeleteFile> filesByDeleteIds = Multimaps.newMultimap(Maps.newHashMap(), Lists::newArrayList);
    for (DeleteFile delete : eqDeletes) {
      filesByDeleteIds.put(Sets.newHashSet(delete.equalityFieldIds()), delete);
    }

    CloseableIterable<Record> filteredRecords = records;
    for (Map.Entry<Set<Integer>, Collection<DeleteFile>> entry : filesByDeleteIds.asMap().entrySet()) {
      Set<Integer> ids = entry.getKey();
      Iterable<DeleteFile> deletes = entry.getValue();

      Schema deleteSchema = TypeUtil.select(recordSchema, ids);
      int[] orderedIds = deleteSchema.columns().stream().mapToInt(Types.NestedField::fieldId).toArray();

      // a wrapper to translate from generic objects to internal representations
      InternalRecordWrapper asStructLike = new InternalRecordWrapper(recordSchema.asStruct());

      // a projection to select and reorder fields of the file schema to match the delete rows
      ProjectStructLike projectRow = ProjectStructLike.of(recordSchema, orderedIds);

      Iterable<CloseableIterable<Record>> deleteRecords = Iterables.transform(deletes,
          delete -> openDeletes(delete, dataFile, deleteSchema));
      StructLikeSet deleteSet = Deletes.toEqualitySet(
          // copy the delete records because they will be held in a set
          CloseableIterable.transform(CloseableIterable.concat(deleteRecords), Record::copy),
          deleteSchema.asStruct());

      filteredRecords = Deletes.filter(filteredRecords,
          record -> projectRow.wrap(asStructLike.wrap(record)), deleteSet);
    }

    return filteredRecords;
  }

  private CloseableIterable<Record> applyPosDeletes(CloseableIterable<Record> records, Schema recordSchema,
                                                    CharSequence file, List<DeleteFile> posDeletes, DataFile dataFile) {
    if (posDeletes.isEmpty()) {
      return records;
    }

    Accessor<StructLike> posAccessor = recordSchema.accessorForField(MetadataColumns.ROW_POSITION.fieldId());
    Function<Record, Long> posGetter = record -> (Long) posAccessor.get(record);
    List<CloseableIterable<StructLike>> deletes = Lists.transform(posDeletes,
        delete -> openPosDeletes(delete, dataFile));

    // if there are fewer deletes than a reasonable number to keep in memory, use a set
    if (posDeletes.stream().mapToLong(DeleteFile::recordCount).sum() < 100_000L) {
      return Deletes.filter(records, posGetter, Deletes.toPositionSet(file, CloseableIterable.concat(deletes)));
    }

    return Deletes.streamingFilter(records, posGetter, Deletes.deletePositions(file, deletes));
  }

  private CloseableIterable<StructLike> openPosDeletes(DeleteFile file, DataFile dataFile) {
    return openDeletes(file, dataFile, POS_DELETE_SCHEMA);
  }

  private <T> CloseableIterable<T> openDeletes(DeleteFile deleteFile, DataFile dataFile, Schema deleteSchema) {
    InputFile input = io.newInputFile(deleteFile.path().toString());
    switch (deleteFile.format()) {
      case AVRO:
        return Avro.read(input)
            .project(deleteSchema)
            .reuseContainers()
            .createReaderFunc(DataReader::create)
            .build();

      case PARQUET:
        Parquet.ReadBuilder builder = Parquet.read(input)
            .project(deleteSchema)
            .reuseContainers()
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(deleteSchema, fileSchema));

        if (deleteFile.content() == FileContent.POSITION_DELETES) {
          builder.filter(Expressions.equal(MetadataColumns.DELETE_FILE_PATH.name(), dataFile.path()));
        }

        return builder.build();

      case ORC:
      default:
        throw new UnsupportedOperationException(String.format(
            "Cannot read %s file: %s", deleteFile.format().name(), deleteFile.path()));
    }
  }

  private CloseableIterable<Record> openFile(FileScanTask task, Schema fileProjection) {
    InputFile input = io.newInputFile(task.file().path().toString());
    Map<Integer, ?> partition = PartitionUtil.constantsMap(task, IdentityPartitionConverters::convertConstant);

    switch (task.file().format()) {
      case AVRO:
        Avro.ReadBuilder avro = Avro.read(input)
            .project(fileProjection)
            .createReaderFunc(
                avroSchema -> DataReader.create(fileProjection, avroSchema, partition))
            .split(task.start(), task.length());

        if (reuseContainers) {
          avro.reuseContainers();
        }

        return avro.build();

      case PARQUET:
        Parquet.ReadBuilder parquet = Parquet.read(input)
            .project(fileProjection)
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(fileProjection, fileSchema, partition))
            .split(task.start(), task.length())
            .filter(task.residual());

        if (reuseContainers) {
          parquet.reuseContainers();
        }

        return parquet.build();

      case ORC:
        Schema projectionWithoutConstantAndMetadataFields = TypeUtil.selectNot(fileProjection,
            Sets.union(partition.keySet(), MetadataColumns.metadataFieldIds()));
        ORC.ReadBuilder orc = ORC.read(input)
            .project(projectionWithoutConstantAndMetadataFields)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(fileProjection, fileSchema, partition))
            .split(task.start(), task.length())
            .filter(task.residual());

        return orc.build();

      default:
        throw new UnsupportedOperationException(String.format("Cannot read %s file: %s",
            task.file().format().name(), task.file().path()));
    }
  }

  private class CombinedTaskIterable extends CloseableGroup implements CloseableIterable<Record> {
    private final CombinedScanTask task;

    private CombinedTaskIterable(CombinedScanTask task) {
      this.task = task;
    }

    @Override
    public CloseableIterator<Record> iterator() {
      CloseableIterator<Record> iter = CloseableIterable.concat(
          Iterables.transform(task.files(), GenericReader.this::open)).iterator();
      addCloseable(iter);
      return iter;
    }
  }
}
