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
package org.apache.iceberg.flink.source;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.AddedRowsScanTask;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeletedDataFileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedInputFile;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.FlinkSourceFilter;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.data.FlinkOrcReader;
import org.apache.iceberg.flink.data.FlinkParquetReaders;
import org.apache.iceberg.flink.data.FlinkPlannedAvroReader;
import org.apache.iceberg.flink.data.RowDataProjection;
import org.apache.iceberg.flink.data.RowDataUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PartitionUtil;

/**
 * Reader for ChangelogScanTask that reads data files and converts them to RowData with appropriate
 * RowKind.
 *
 * <p>The reader handles different changelog operations:
 *
 * <ul>
 *   <li>INSERT: Sets RowKind.INSERT (+I)
 *   <li>DELETE: Sets RowKind.DELETE (-D)
 *   <li>UPDATE_BEFORE: Sets RowKind.UPDATE_BEFORE (-U)
 *   <li>UPDATE_AFTER: Sets RowKind.UPDATE_AFTER (+U)
 * </ul>
 */
@Internal
public class RowDataChangelogScanTaskReader
    implements ChangelogDataIterator.ChangelogScanTaskReader<RowData> {

  private final Schema tableSchema;
  private final Schema projectedSchema;
  private final String nameMapping;
  private final boolean caseSensitive;
  private final FlinkSourceFilter rowFilter;
  private final FileIO io;
  private final EncryptionManager encryption;

  public RowDataChangelogScanTaskReader(
      Schema tableSchema,
      Schema projectedSchema,
      String nameMapping,
      boolean caseSensitive,
      List<Expression> filters,
      FileIO io,
      EncryptionManager encryption) {
    this.tableSchema = tableSchema;
    this.projectedSchema = projectedSchema;
    this.nameMapping = nameMapping;
    this.caseSensitive = caseSensitive;
    this.io = io;
    this.encryption = encryption;

    if (filters != null && !filters.isEmpty()) {
      Expression combinedExpression =
          filters.stream().reduce(Expressions.alwaysTrue(), Expressions::and);
      this.rowFilter =
          new FlinkSourceFilter(this.projectedSchema, combinedExpression, this.caseSensitive);
    } else {
      this.rowFilter = null;
    }
  }

  @Override
  public CloseableIterator<RowData> open(
      ContentScanTask<DataFile> task,
      ChangelogOperation operation,
      int changeOrdinal,
      long commitSnapshotId) {

    List<DeleteFile> deleteFiles = getDeleteFiles(task);
    Map<String, InputFile> inputFiles = decryptInputFiles(task, deleteFiles);

    Map<Integer, ?> idToConstant = PartitionUtil.constantsMap(task, RowDataUtil::convertConstant);
    ChangelogDeleteFilter deletes =
        new ChangelogDeleteFilter(task, deleteFiles, tableSchema, projectedSchema, inputFiles);
    CloseableIterable<RowData> iterable =
        deletes.filter(newIterable(task, deletes.requiredSchema(), idToConstant, inputFiles));

    // Project the RowData to remove the extra meta columns
    if (!projectedSchema.sameSchema(deletes.requiredSchema())) {
      RowDataProjection rowDataProjection =
          RowDataProjection.create(
              deletes.requiredRowType(),
              deletes.requiredSchema().asStruct(),
              projectedSchema.asStruct());
      iterable = CloseableIterable.transform(iterable, rowDataProjection::wrap);
    }

    // Set RowKind based on changelog operation
    RowKind rowKind = toRowKind(operation);
    iterable = CloseableIterable.transform(iterable, row -> withRowKind(row, rowKind));

    return iterable.iterator();
  }

  private Map<String, InputFile> decryptInputFiles(
      ContentScanTask<DataFile> task, List<DeleteFile> deleteFiles) {
    // Collect all files that need to be decrypted
    Stream<ContentFile<?>> allFiles =
        Stream.concat(Stream.of(task.file()), deleteFiles.stream());

    Stream<EncryptedInputFile> encryptedFiles =
        allFiles.map(
            file ->
                EncryptedFiles.encryptedInput(
                    io.newInputFile(file.location()), file.keyMetadata()));

    // Decrypt with batch call to avoid multiple RPCs to key server
    @SuppressWarnings("StreamToIterable")
    Iterable<InputFile> decryptedFiles = encryption.decrypt(encryptedFiles::iterator);

    Map<String, InputFile> inputFileMap = Maps.newHashMap();
    decryptedFiles.forEach(f -> inputFileMap.putIfAbsent(f.location(), f));
    return Collections.unmodifiableMap(inputFileMap);
  }

  private List<DeleteFile> getDeleteFiles(ContentScanTask<DataFile> task) {
    if (task instanceof AddedRowsScanTask) {
      return ((AddedRowsScanTask) task).deletes();
    } else if (task instanceof DeletedDataFileScanTask) {
      return ((DeletedDataFileScanTask) task).existingDeletes();
    }
    return ImmutableList.of();
  }

  private RowKind toRowKind(ChangelogOperation operation) {
    switch (operation) {
      case INSERT:
        return RowKind.INSERT;
      case DELETE:
        return RowKind.DELETE;
      case UPDATE_BEFORE:
        return RowKind.UPDATE_BEFORE;
      case UPDATE_AFTER:
        return RowKind.UPDATE_AFTER;
      default:
        throw new UnsupportedOperationException("Unknown changelog operation: " + operation);
    }
  }

  private RowData withRowKind(RowData row, RowKind rowKind) {
    row.setRowKind(rowKind);
    return row;
  }

  private CloseableIterable<RowData> newIterable(
      ContentScanTask<DataFile> task,
      Schema schema,
      Map<Integer, ?> idToConstant,
      Map<String, InputFile> inputFiles) {
    CloseableIterable<RowData> iter;
    switch (task.file().format()) {
      case PARQUET:
        iter = newParquetIterable(task, schema, idToConstant, inputFiles);
        break;

      case AVRO:
        iter = newAvroIterable(task, schema, idToConstant, inputFiles);
        break;

      case ORC:
        iter = newOrcIterable(task, schema, idToConstant, inputFiles);
        break;

      default:
        throw new UnsupportedOperationException(
            "Cannot read unknown format: " + task.file().format());
    }

    if (rowFilter != null) {
      return CloseableIterable.filter(iter, rowFilter::filter);
    }
    return iter;
  }

  private CloseableIterable<RowData> newAvroIterable(
      ContentScanTask<DataFile> task,
      Schema schema,
      Map<Integer, ?> idToConstant,
      Map<String, InputFile> inputFiles) {
    Avro.ReadBuilder builder =
        Avro.read(inputFiles.get(task.file().location()))
            .reuseContainers()
            .project(schema)
            .split(task.start(), task.length())
            .createReaderFunc(readSchema -> FlinkPlannedAvroReader.create(schema, idToConstant));

    if (nameMapping != null) {
      builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
    }

    return builder.build();
  }

  private CloseableIterable<RowData> newParquetIterable(
      ContentScanTask<DataFile> task,
      Schema schema,
      Map<Integer, ?> idToConstant,
      Map<String, InputFile> inputFiles) {
    Parquet.ReadBuilder builder =
        Parquet.read(inputFiles.get(task.file().location()))
            .split(task.start(), task.length())
            .project(schema)
            .createReaderFunc(
                fileSchema -> FlinkParquetReaders.buildReader(schema, fileSchema, idToConstant))
            .filter(task.residual())
            .caseSensitive(caseSensitive)
            .reuseContainers();

    if (nameMapping != null) {
      builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
    }

    return builder.build();
  }

  private CloseableIterable<RowData> newOrcIterable(
      ContentScanTask<DataFile> task,
      Schema schema,
      Map<Integer, ?> idToConstant,
      Map<String, InputFile> inputFiles) {
    Schema readSchemaWithoutConstantAndMetadataFields =
        TypeUtil.selectNot(
            schema, Sets.union(idToConstant.keySet(), MetadataColumns.metadataFieldIds()));

    ORC.ReadBuilder builder =
        ORC.read(inputFiles.get(task.file().location()))
            .project(readSchemaWithoutConstantAndMetadataFields)
            .split(task.start(), task.length())
            .createReaderFunc(
                readOrcSchema -> new FlinkOrcReader(schema, readOrcSchema, idToConstant))
            .filter(task.residual())
            .caseSensitive(caseSensitive);

    if (nameMapping != null) {
      builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
    }

    return builder.build();
  }

  private static class ChangelogDeleteFilter extends DeleteFilter<RowData> {
    private final RowType requiredRowType;
    private final RowDataWrapper asStructLike;
    private final Map<String, InputFile> inputFiles;

    ChangelogDeleteFilter(
        ContentScanTask<DataFile> task,
        List<DeleteFile> deleteFiles,
        Schema tableSchema,
        Schema requestedSchema,
        Map<String, InputFile> inputFiles) {
      super(task.file().location(), deleteFiles, tableSchema, requestedSchema);
      this.requiredRowType = FlinkSchemaUtil.convert(requiredSchema());
      this.asStructLike = new RowDataWrapper(requiredRowType, requiredSchema().asStruct());
      this.inputFiles = inputFiles;
    }

    public RowType requiredRowType() {
      return requiredRowType;
    }

    @Override
    protected StructLike asStructLike(RowData row) {
      return asStructLike.wrap(row);
    }

    @Override
    protected InputFile getInputFile(String location) {
      return inputFiles.get(location);
    }
  }
}
