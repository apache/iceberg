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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.*;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.encryption.InputFilesDecryptor;
import org.apache.iceberg.flink.data.FlinkAvroReader;
import org.apache.iceberg.flink.data.FlinkOrcReader;
import org.apache.iceberg.flink.data.FlinkParquetReaders;
import org.apache.iceberg.flink.data.RowDataProjection;
import org.apache.iceberg.flink.data.RowDataUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PartitionUtil;

@Internal
public class StreamingRowDataFileScanTaskReader implements FileScanTaskReader<RowData> {

    private final Schema tableSchema;
    private final Schema projectedSchema;
    private final String nameMapping;
    private final boolean caseSensitive;

    public StreamingRowDataFileScanTaskReader(
            Schema tableSchema, Schema projectedSchema,
            String nameMapping, boolean caseSensitive) {
        this.tableSchema = tableSchema;
        this.projectedSchema = projectedSchema;
        this.nameMapping = nameMapping;
        this.caseSensitive = caseSensitive;
    }

    @Override
    public CloseableIterator<RowData> open(FileScanTask task, InputFilesDecryptor inputFilesDecryptor) {
        Schema partitionSchema = TypeUtil.select(projectedSchema, task.spec().identitySourceIds());

        Map<Integer, ?> idToConstant = partitionSchema.columns().isEmpty() ? ImmutableMap.of() :
                PartitionUtil.constantsMap(task, RowDataUtil::convertConstant);

        RowDataFileScanTaskReader.FlinkDeleteFilter deletes =
                new RowDataFileScanTaskReader.FlinkDeleteFilter(task, tableSchema, projectedSchema, inputFilesDecryptor);
        CloseableIterable<RowData> iterable =
                deletes.applyPosDeletes(newIterable(task, deletes.requiredSchema(), idToConstant, inputFilesDecryptor));

        // Project the RowData to remove the extra meta columns.
        if (!projectedSchema.sameSchema(deletes.requiredSchema())) {
            RowDataProjection rowDataProjection = RowDataProjection.create(
                    deletes.requiredRowType(), deletes.requiredSchema().asStruct(), projectedSchema.asStruct());
            iterable = CloseableIterable.transform(iterable, rowDataProjection::wrap);
        }

        return iterable.iterator();
    }

    @Override
    public CloseableIterator<RowData> openDelete(FileScanTask task, InputFilesDecryptor inputFilesDecryptor) {
        Schema partitionSchema = TypeUtil.select(projectedSchema, task.spec().identitySourceIds());

        Map<Integer, ?> idToConstant = partitionSchema.columns().isEmpty() ? ImmutableMap.of() :
                PartitionUtil.constantsMap(task, RowDataUtil::convertConstant);

        RowDataFileScanTaskReader.FlinkDeleteFilter deletes =
                new RowDataFileScanTaskReader.FlinkDeleteFilter(task, tableSchema, projectedSchema, inputFilesDecryptor);
        CloseableIterable<RowData> iterable =
                newEqDeleteIterable(task, deletes.requiredSchema(), idToConstant, inputFilesDecryptor);

        // Project the RowData to remove the extra meta columns.
        if (!projectedSchema.sameSchema(deletes.requiredSchema())) {
            RowDataProjection rowDataProjection = RowDataProjection.create(
                    deletes.requiredRowType(), deletes.requiredSchema().asStruct(), projectedSchema.asStruct());
            iterable = CloseableIterable.transform(iterable, rowDataProjection::wrapDelete);
        }

        return iterable.iterator();
    }

    private CloseableIterable<RowData> newIterable(
            FileScanTask task, Schema schema, Map<Integer, ?> idToConstant, InputFilesDecryptor inputFilesDecryptor) {
        CloseableIterable<RowData> iter;
        if (task.isDataTask()) {
            throw new UnsupportedOperationException("Cannot read data task.");
        } else {
            switch (task.file().format()) {
                case PARQUET:
                    iter = newParquetIterable(task, schema, idToConstant, inputFilesDecryptor);
                    break;

                case AVRO:
                    iter = newAvroIterable(task, schema, idToConstant, inputFilesDecryptor);
                    break;

                case ORC:
                    iter = newOrcIterable(task, schema, idToConstant, inputFilesDecryptor);
                    break;

                default:
                    throw new UnsupportedOperationException(
                            "Cannot read unknown format: " + task.file().format());
            }
        }

        return iter;
    }

    private CloseableIterable<RowData> newEqDeleteIterable(
            FileScanTask task,
            Schema schema,
            Map<Integer, ?> idToConstant,
            InputFilesDecryptor inputFilesDecryptor) {
        CloseableIterable<RowData> iter;
        FileFormat fileFormat = task.file() != null ? task.file().format() : task.deletes().get(0).format();
        if (task.isDataTask()) {
            throw new UnsupportedOperationException("Cannot read data task.");
        } else {
            switch (fileFormat) {
                case PARQUET:
                    iter = newEqDeleteParquetIterable(task, schema, idToConstant, inputFilesDecryptor);
                    break;

                case AVRO:
                    iter = newEqDeleteAvroIterable(task, schema, idToConstant, inputFilesDecryptor);
                    break;

                case ORC:
                    iter = newEqDeleteOrcIterable(task, schema, idToConstant, inputFilesDecryptor);
                    break;

                default:
                    throw new UnsupportedOperationException(
                            "Cannot read unknown format: " + task.file().format());
            }
        }

        return iter;
    }

    private CloseableIterable<RowData> newAvroIterable(
            FileScanTask task, Schema schema, Map<Integer, ?> idToConstant, InputFilesDecryptor inputFilesDecryptor) {
        Avro.ReadBuilder builder = Avro.read(inputFilesDecryptor.getInputFile(task))
                .reuseContainers()
                .project(schema)
                .split(task.start(), task.length())
                .createReaderFunc(readSchema -> new FlinkAvroReader(schema, readSchema, idToConstant));

        if (nameMapping != null) {
            builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
        }

        return builder.build();
    }

    private CloseableIterable<RowData> newEqDeleteAvroIterable(
            FileScanTask task,
            Schema schema,
            Map<Integer, ?> idToConstant,
            InputFilesDecryptor inputFilesDecryptor) {
        if (task.deletes().stream().noneMatch(file -> file.content().equals(FileContent.EQUALITY_DELETES))) {
            return CloseableIterable.empty();
        }
        Iterable<RowData> closeableIterable = CloseableIterable.empty();
        List<InputFile> inputFiles  = inputFilesDecryptor.getEqDeleteInputFile(task);
        if (inputFiles !=null && inputFiles.size() > 0) {
            for (InputFile inputFile: inputFiles) {
                Avro.ReadBuilder builder = Avro.read(inputFile)
                        .reuseContainers()
                        .project(schema)
                        .split(task.start(), task.length())
                        .createReaderFunc(readSchema -> new FlinkAvroReader(schema, readSchema, idToConstant));

                if (nameMapping != null) {
                    builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
                }
                closeableIterable = Iterables.concat(closeableIterable, builder.build());
            }
        }
        return CloseableIterable.combine(closeableIterable, CloseableIterable.empty());
//    return CloseableIterable.concat(closeableIterable);
    }

    private CloseableIterable<RowData> newParquetIterable(
            FileScanTask task, Schema schema, Map<Integer, ?> idToConstant, InputFilesDecryptor inputFilesDecryptor) {
        Parquet.ReadBuilder builder = Parquet.read(inputFilesDecryptor.getInputFile(task))
                .reuseContainers()
                .split(task.start(), task.length())
                .project(schema)
                .createReaderFunc(fileSchema -> FlinkParquetReaders.buildReader(schema, fileSchema, idToConstant))
                .filter(task.residual())
                .caseSensitive(caseSensitive)
                .reuseContainers();

        if (nameMapping != null) {
            builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
        }

        return builder.build();
    }

    private CloseableIterable<RowData> newEqDeleteParquetIterable(
            FileScanTask task, Schema schema,
            Map<Integer, ?> idToConstant, InputFilesDecryptor inputFilesDecryptor) {
        if (task.deletes().stream().noneMatch(file -> file.content().equals(FileContent.EQUALITY_DELETES))) {
            return CloseableIterable.empty();
        }

        Iterable<RowData> closeableIterable = CloseableIterable.empty();
        List<InputFile> inputFiles  = inputFilesDecryptor.getEqDeleteInputFile(task);
        if (inputFiles !=null) {
            for (InputFile inputFile: inputFiles) {
                Parquet.ReadBuilder builder = Parquet.read(inputFile)
                        .reuseContainers()
                        .split(task.start(), task.length())
                        .project(schema)
                        .createReaderFunc(fileSchema -> FlinkParquetReaders.buildReader(schema, fileSchema, idToConstant))
                        .filter(task.residual())
                        .caseSensitive(caseSensitive)
                        .reuseContainers();

                if (nameMapping != null) {
                    builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
                }
                closeableIterable = Iterables.concat(closeableIterable, builder.build());
            }
        }
        return CloseableIterable.combine(closeableIterable, CloseableIterable.empty());
    }

    private CloseableIterable<RowData> newOrcIterable(
            FileScanTask task, Schema schema, Map<Integer, ?> idToConstant, InputFilesDecryptor inputFilesDecryptor) {
        Schema readSchemaWithoutConstantAndMetadataFields = TypeUtil.selectNot(
                schema,
                Sets.union(idToConstant.keySet(), MetadataColumns.metadataFieldIds()));

        ORC.ReadBuilder builder = ORC.read(inputFilesDecryptor.getInputFile(task))
                .project(readSchemaWithoutConstantAndMetadataFields)
                .split(task.start(), task.length())
                .createReaderFunc(readOrcSchema -> new FlinkOrcReader(schema, readOrcSchema, idToConstant))
                .filter(task.residual())
                .caseSensitive(caseSensitive);

        if (nameMapping != null) {
            builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
        }

        return builder.build();
    }

    private CloseableIterable<RowData> newEqDeleteOrcIterable(
            FileScanTask task,
            Schema schema,
            Map<Integer, ?> idToConstant,
            InputFilesDecryptor inputFilesDecryptor) {
        if (task.deletes().stream().noneMatch(file -> file.content().equals(FileContent.EQUALITY_DELETES))) {
            return CloseableIterable.empty();
        }

        Schema readSchemaWithoutConstantAndMetadataFields = TypeUtil.selectNot(
                schema,
                Sets.union(idToConstant.keySet(), MetadataColumns.metadataFieldIds()));

        Iterable<RowData> closeableIterable = CloseableIterable.empty();
        List<InputFile> inputFiles  = inputFilesDecryptor.getEqDeleteInputFile(task);
        if (inputFiles !=null) {
            for (InputFile inputFile: inputFiles) {
                ORC.ReadBuilder builder = ORC.read(inputFile)
                        .project(readSchemaWithoutConstantAndMetadataFields)
                        .split(task.start(), task.length())
                        .createReaderFunc(readOrcSchema -> new FlinkOrcReader(schema, readOrcSchema, idToConstant))
                        .filter(task.residual())
                        .caseSensitive(caseSensitive);

                if (nameMapping != null) {
                    builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
                }
                closeableIterable = Iterables.concat(closeableIterable, builder.build());
            }
        }
        return CloseableIterable.combine(closeableIterable, CloseableIterable.empty());
    }
}
