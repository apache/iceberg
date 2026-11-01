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
package org.apache.iceberg.flink.sink;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.data.RowDataProjection;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitioningDVWriter;
import org.apache.iceberg.io.RollingEqualityDeleteWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PartitionMap;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.iceberg.util.StructLikeUtil;
import org.apache.iceberg.util.Tasks;

abstract class BaseDeltaTaskWriter extends BaseTaskWriter<RowData> {

  private final Schema schema;
  private final Schema deleteSchema;
  private final RowDataWrapper wrapper;
  private final RowDataWrapper keyWrapper;
  private final RowDataProjection keyProjection;
  private final boolean upsert;
  private final FileWriterFactory<RowData> fileWriterFactory;
  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final long targetFileSize;
  private final List<Pair<PartitionSpec, PartitionKey>> otherSpecPartitionKeys =
      Lists.newArrayList();
  private final PartitionMap<RollingEqualityDeleteWriter<RowData>> otherSpecDeleteWriters;
  private final List<DeleteFile> otherSpecDeleteFiles = Lists.newArrayList();
  private final StructLikeSet insertedKeys;

  BaseDeltaTaskWriter(
      PartitionSpec spec,
      FileFormat format,
      FileWriterFactory<RowData> fileWriterFactory,
      OutputFileFactory fileFactory,
      FileIO io,
      long targetFileSize,
      Schema schema,
      RowType flinkSchema,
      Set<Integer> equalityFieldIds,
      boolean upsert,
      boolean useDv,
      Map<Integer, PartitionSpec> tableSpecs) {
    super(spec, format, fileWriterFactory, fileFactory, io, targetFileSize, useDv);
    this.schema = schema;
    this.deleteSchema = TypeUtil.select(schema, Sets.newHashSet(equalityFieldIds));
    this.wrapper = new RowDataWrapper(flinkSchema, schema.asStruct());
    this.keyWrapper =
        new RowDataWrapper(FlinkSchemaUtil.convert(deleteSchema), deleteSchema.asStruct());
    this.keyProjection =
        RowDataProjection.create(flinkSchema, schema.asStruct(), deleteSchema.asStruct());
    this.upsert = upsert;
    this.fileWriterFactory = fileWriterFactory;
    this.fileFactory = fileFactory;
    this.io = io;
    this.targetFileSize = targetFileSize;
    this.insertedKeys = StructLikeSet.create(deleteSchema.asStruct());
    this.otherSpecDeleteWriters = PartitionMap.create(tableSpecs);
    boolean keyOnlyPartitioning =
        partitionedByKey(spec, equalityFieldIds)
            && tableSpecs.values().stream()
                .allMatch(other -> partitionedByKey(other, equalityFieldIds));
    if (spec.isUnpartitioned() || !keyOnlyPartitioning) {
      return;
    }

    for (PartitionSpec otherSpec : tableSpecs.values()) {
      boolean sameKeyTypes =
          otherSpec.fields().stream()
              .allMatch(
                  field ->
                      Objects.equals(
                          otherSpec.schema().findType(field.sourceId()),
                          deleteSchema.findType(field.sourceId())));
      if (otherSpec.specId() != spec.specId() && sameKeyTypes) {
        otherSpecPartitionKeys.add(Pair.of(otherSpec, new PartitionKey(otherSpec, deleteSchema)));
      }
    }
  }

  private static boolean partitionedByKey(PartitionSpec spec, Set<Integer> equalityFieldIds) {
    return spec.fields().stream().allMatch(field -> equalityFieldIds.contains(field.sourceId()));
  }

  abstract RowDataDeltaWriter route(RowData row);

  RowDataWrapper wrapper() {
    return wrapper;
  }

  @Override
  public void write(RowData row) throws IOException {
    RowDataDeltaWriter writer = route(row);

    switch (row.getRowKind()) {
      case INSERT:
      case UPDATE_AFTER:
        if (upsert) {
          RowData key = keyProjection.wrap(row);
          writer.deleteKey(key);
          deleteFromOtherSpecs(row, key);
        }
        writer.write(row);
        trackInsertedKey(row);
        break;

      case UPDATE_BEFORE:
        if (upsert) {
          break; // UPDATE_BEFORE is not necessary for UPSERT, we do nothing to prevent delete one
          // row twice
        }
        writer.delete(row);
        deleteFromOtherSpecs(row, row);
        break;
      case DELETE:
        if (upsert) {
          RowData key = keyProjection.wrap(row);
          writer.deleteKey(key);
          deleteFromOtherSpecs(row, key);
        } else {
          writer.delete(row);
          deleteFromOtherSpecs(row, row);
        }
        break;

      default:
        throw new UnsupportedOperationException("Unknown row kind: " + row.getRowKind());
    }
  }

  private boolean deletesOtherSpecs() {
    return !otherSpecPartitionKeys.isEmpty();
  }

  private void trackInsertedKey(RowData row) {
    if (deletesOtherSpecs()) {
      insertedKeys.add(StructLikeUtil.copy(keyWrapper.wrap(keyProjection.wrap(row))));
    }
  }

  private void deleteFromOtherSpecs(RowData row, RowData deleteRow) throws IOException {
    StructLike key = keyWrapper.wrap(keyProjection.wrap(row));
    if (!deletesOtherSpecs() || insertedKeys.remove(key)) {
      return;
    }

    for (Pair<PartitionSpec, PartitionKey> specAndKey : otherSpecPartitionKeys) {
      PartitionSpec otherSpec = specAndKey.first();
      PartitionKey partitionKey = specAndKey.second();
      partitionKey.partition(key);
      RollingEqualityDeleteWriter<RowData> writer =
          otherSpecDeleteWriters.get(otherSpec.specId(), partitionKey);
      if (writer == null) {
        PartitionKey copiedKey = partitionKey.copy();
        writer =
            new RollingEqualityDeleteWriter<>(
                fileWriterFactory,
                fileFactory,
                io,
                targetFileSize,
                otherSpec,
                otherSpec.isUnpartitioned() ? null : copiedKey);
        otherSpecDeleteWriters.put(otherSpec.specId(), copiedKey, writer);
      }

      writer.write(deleteRow);
    }
  }

  private void closeOtherSpecDeleteWriters() throws IOException {
    try {
      Tasks.foreach(Lists.newArrayList(otherSpecDeleteWriters.values()))
          .throwFailureWhenFinished()
          .noRetry()
          .run(
              writer -> {
                writer.close();
                otherSpecDeleteFiles.addAll(writer.result().deleteFiles());
              },
              IOException.class);
    } catch (IOException | RuntimeException e) {
      setFailure(e);
      throw e;
    } finally {
      otherSpecDeleteWriters.clear();
    }
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } catch (IOException | RuntimeException e) {
      try {
        closeOtherSpecDeleteWriters();
      } catch (IOException | RuntimeException suppressed) {
        e.addSuppressed(suppressed);
      }

      throw e;
    }

    closeOtherSpecDeleteWriters();
  }

  @Override
  public WriteResult complete() throws IOException {
    WriteResult result = super.complete();
    return WriteResult.builder().add(result).addDeleteFiles(otherSpecDeleteFiles).build();
  }

  @Override
  public void abort() throws IOException {
    try {
      super.abort();
    } finally {
      Tasks.foreach(otherSpecDeleteFiles)
          .throwFailureWhenFinished()
          .noRetry()
          .run(file -> io.deleteFile(file.location()));
    }
  }

  protected class RowDataDeltaWriter extends BaseEqualityDeltaWriter {
    RowDataDeltaWriter(PartitionKey partition, PartitioningDVWriter<RowData> dvFileWriter) {
      super(partition, schema, deleteSchema, DeleteGranularity.FILE, dvFileWriter);
    }

    @Override
    protected StructLike asStructLike(RowData data) {
      return wrapper.wrap(data);
    }

    @Override
    protected StructLike asStructLikeKey(RowData data) {
      return keyWrapper.wrap(data);
    }
  }
}
