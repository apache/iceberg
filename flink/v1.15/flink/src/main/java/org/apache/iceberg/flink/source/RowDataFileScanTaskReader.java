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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
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
import org.apache.iceberg.flink.data.FlinkAvroReader;
import org.apache.iceberg.flink.data.FlinkOrcReader;
import org.apache.iceberg.flink.data.FlinkParquetReaders;
import org.apache.iceberg.flink.data.RowDataProjection;
import org.apache.iceberg.flink.data.RowDataUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PartitionUtil;

@Internal
public class RowDataFileScanTaskReader implements ScanTaskReader<RowData>, Serializable {

  private final Schema tableSchema;
  private final Schema projectedSchema;
  private final NameMapping nameMapping;
  private final boolean caseSensitive;
  private ScanTaskGroup<? extends ScanTask> taskGroup;
  private Map<String, InputFile> lazyInputFiles;
  private final FileIO io;
  private final EncryptionManager encryption;
  private final FlinkSourceFilter rowFilter;

  public RowDataFileScanTaskReader(
      Schema tableSchema,
      Schema projectedSchema,
      String nameMapping,
      boolean caseSensitive,
      ScanTaskGroup<? extends ScanTask> taskGroup,
      FileIO io,
      EncryptionManager encryption,
      List<Expression> filters) {
    this.tableSchema = tableSchema;
    this.projectedSchema = projectedSchema;
    this.nameMapping = nameMapping != null ? NameMappingParser.fromJson(nameMapping) : null;
    this.caseSensitive = caseSensitive;
    this.taskGroup = taskGroup;
    this.io = io;
    this.encryption = encryption;

    if (filters != null && !filters.isEmpty()) {
      Expression combinedExpression =
          filters.stream().reduce(Expressions.alwaysTrue(), Expressions::and);
      this.rowFilter = new FlinkSourceFilter(projectedSchema, combinedExpression, caseSensitive);
    } else {
      this.rowFilter = null;
    }
  }

  @Override
  public void taskGroup(ScanTaskGroup<? extends ScanTask> newTaskGroup) {
    this.taskGroup = newTaskGroup;
    this.lazyInputFiles = null;
  }

  @Override
  public ScanTaskGroup<? extends ScanTask> taskGroup() {
    return taskGroup;
  }

  @Override
  public CloseableIterator<RowData> open(ScanTask task) {
    return openFileScanTask((FileScanTask) task);
  }

  protected Schema projectedSchema() {
    return projectedSchema;
  }

  protected Schema tableSchema() {
    return tableSchema;
  }

  protected Stream<ContentFile<?>> referencedFiles(ScanTask task) {
    FileScanTask fileScanTask = (FileScanTask) task;
    return Stream.concat(Stream.of(fileScanTask.file()), fileScanTask.deletes().stream());
  }

  protected InputFile inputFile(String location) {
    return inputFiles().get(location);
  }

  private CloseableIterator<RowData> openFileScanTask(FileScanTask task) {
    FileScanTask scanTask = task;
    if (scanTask.isDataTask()) {
      throw new UnsupportedOperationException("Cannot read data task.");
    }

    Schema partitionSchema =
        TypeUtil.select(projectedSchema(), scanTask.spec().identitySourceIds());

    DataFile file = scanTask.file();
    String filePath = scanTask.file().path().toString();

    FileFormat format = file.format();
    long start = scanTask.start();
    long length = scanTask.length();
    Expression residual = scanTask.residual();
    InputFile inputFile = inputFile(filePath);

    Map<Integer, ?> idToConstant =
        partitionSchema.columns().isEmpty()
            ? ImmutableMap.of()
            : PartitionUtil.constantsMap(scanTask, RowDataUtil::convertConstant);

    FlinkDeleteFilter deletes =
        new FlinkDeleteFilter(filePath, scanTask.deletes(), tableSchema, projectedSchema());
    CloseableIterable<RowData> iterable =
        deletes.filter(
            newIterable(
                inputFile,
                format,
                start,
                length,
                residual,
                deletes.requiredSchema(),
                idToConstant));

    // Project the RowData to remove the extra meta columns.
    if (!projectedSchema().sameSchema(deletes.requiredSchema())) {
      RowDataProjection rowDataProjection =
          RowDataProjection.create(
              deletes.requiredRowType(),
              deletes.requiredSchema().asStruct(),
              projectedSchema().asStruct());
      iterable = CloseableIterable.transform(iterable, rowDataProjection::wrap);
    }

    return iterable.iterator();
  }

  private Map<String, InputFile> inputFiles() {
    if (lazyInputFiles == null) {
      Stream<EncryptedInputFile> encryptedFiles =
          taskGroup.tasks().stream().flatMap(this::referencedFiles).map(this::toEncryptedInputFile);

      // decrypt with the batch call to avoid multiple RPCs to a key server, if possible
      Iterable<InputFile> decryptedFiles = encryption.decrypt(encryptedFiles::iterator);

      Map<String, InputFile> files = Maps.newHashMap();
      decryptedFiles.forEach(decrypted -> files.putIfAbsent(decrypted.location(), decrypted));
      this.lazyInputFiles = ImmutableMap.copyOf(files);
    }

    return lazyInputFiles;
  }

  private EncryptedInputFile toEncryptedInputFile(ContentFile<?> file) {
    InputFile inputFile = io.newInputFile(file.path().toString());
    return EncryptedFiles.encryptedInput(inputFile, file.keyMetadata());
  }

  protected CloseableIterable<RowData> newIterable(
      InputFile file,
      FileFormat format,
      long start,
      long length,
      Expression residual,
      Schema schema,
      Map<Integer, ?> idToConstant) {

    CloseableIterable<RowData> iter;

    switch (format) {
      case PARQUET:
        iter = newParquetIterable(file, start, length, residual, schema, idToConstant);
        break;

      case AVRO:
        iter = newAvroIterable(file, start, length, schema, idToConstant);
        break;

      case ORC:
        iter = newOrcIterable(file, start, length, residual, schema, idToConstant);
        break;

      default:
        throw new UnsupportedOperationException("Cannot read unknown format: " + format);
    }

    if (rowFilter != null) {
      return CloseableIterable.filter(iter, rowFilter::filter);
    }
    return iter;
  }

  private CloseableIterable<RowData> newAvroIterable(
      InputFile file, long start, long length, Schema schema, Map<Integer, ?> idToConstant) {
    Avro.ReadBuilder builder =
        Avro.read(file)
            .reuseContainers()
            .project(schema)
            .split(start, length)
            .createReaderFunc(readSchema -> new FlinkAvroReader(schema, readSchema, idToConstant));

    if (nameMapping != null) {
      builder.withNameMapping(nameMapping);
    }

    return builder.build();
  }

  private CloseableIterable<RowData> newParquetIterable(
      InputFile file,
      long start,
      long length,
      Expression residual,
      Schema schema,
      Map<Integer, ?> idToConstant) {
    Parquet.ReadBuilder builder =
        Parquet.read(file)
            .split(start, length)
            .project(schema)
            .createReaderFunc(
                fileSchema -> FlinkParquetReaders.buildReader(schema, fileSchema, idToConstant))
            .filter(residual)
            .caseSensitive(caseSensitive)
            .reuseContainers();

    if (nameMapping != null) {
      builder.withNameMapping(nameMapping);
    }

    return builder.build();
  }

  private CloseableIterable<RowData> newOrcIterable(
      InputFile file,
      long start,
      long length,
      Expression residual,
      Schema schema,
      Map<Integer, ?> idToConstant) {
    Schema readSchemaWithoutConstantAndMetadataFields =
        TypeUtil.selectNot(
            schema, Sets.union(idToConstant.keySet(), MetadataColumns.metadataFieldIds()));

    ORC.ReadBuilder builder =
        ORC.read(file)
            .project(readSchemaWithoutConstantAndMetadataFields)
            .split(start, length)
            .createReaderFunc(
                readOrcSchema -> new FlinkOrcReader(schema, readOrcSchema, idToConstant))
            .filter(residual)
            .caseSensitive(caseSensitive);

    if (nameMapping != null) {
      builder.withNameMapping(nameMapping);
    }

    return builder.build();
  }

  protected class FlinkDeleteFilter extends DeleteFilter<RowData> {
    private final RowType requiredRowType;
    private final RowDataWrapper asStructLike;

    FlinkDeleteFilter(
        String filePath, List<DeleteFile> deletes, Schema tableSchema, Schema requestedSchema) {
      super(filePath, deletes, tableSchema, requestedSchema);
      this.requiredRowType = FlinkSchemaUtil.convert(requiredSchema());
      this.asStructLike = new RowDataWrapper(requiredRowType, requiredSchema().asStruct());
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
      return inputFile(location);
    }
  }
}
