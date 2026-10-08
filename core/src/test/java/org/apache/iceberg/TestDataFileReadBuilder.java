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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.formats.DataFileReadBuilder;
import org.apache.iceberg.formats.FormatModel;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.formats.Stitcher;
import org.apache.iceberg.formats.StitcherBuilder;
import org.apache.iceberg.formats.StitcherRegistry;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TestDataFileReadBuilder {
  private static final String DATA_FILE_LOCATION = "s3://bucket/data/file.parquet";
  private static final String COLUMN_FILE_LOCATION = "s3://bucket/data/column-file.parquet";
  private static final Map<Integer, PartitionSpec> UNPARTITIONED =
      ImmutableMap.of(PartitionSpec.unpartitioned().specId(), PartitionSpec.unpartitioned());
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()),
          Types.NestedField.optional(3, "category", Types.StringType.get()));
  private static final ColumnFile COLUMN_FILE =
      ColumnFiles.builder()
          .withFormatVersion(4)
          .withFieldIds(List.of(2, 3))
          .withLocation(COLUMN_FILE_LOCATION)
          .withFileFormat(FileFormat.PARQUET)
          .withFileSizeInBytes(128L)
          .build();
  private static final int POSITION_ID = MetadataColumns.ROW_POSITION.fieldId();
  private static final Function<String, InputFile> MISSING_FILES = location -> null;
  private static final Function<String, InputFile> INPUT_FILES = location -> mock(InputFile.class);

  private static FormatModel<Row, Schema> model;

  private ReadBuilder<Row, Schema> dataFileReader;
  private ReadBuilder<Row, Schema> columnFileReader;

  @BeforeAll
  @SuppressWarnings("unchecked")
  static void registerModel() {
    model = mock(FormatModel.class);
    when(model.format()).thenReturn(FileFormat.PARQUET);
    doReturn(Row.class).when(model).type();
    FormatModelRegistry.register(model);

    StitcherBuilder<Row> stitcherBuilder = mock(StitcherBuilder.class);
    doReturn(Row.class).when(stitcherBuilder).type();
    when(stitcherBuilder.build(any(), any())).thenReturn(mock(Stitcher.class));
    StitcherRegistry.register(stitcherBuilder);

    FormatModel<Unstitchable, Schema> unstitchableModel = mock(FormatModel.class);
    when(unstitchableModel.format()).thenReturn(FileFormat.PARQUET);
    doReturn(Unstitchable.class).when(unstitchableModel).type();
    when(unstitchableModel.readBuilder(any())).thenReturn(mock(ReadBuilder.class));
    FormatModelRegistry.register(unstitchableModel);
  }

  @BeforeEach
  @SuppressWarnings("unchecked")
  void stubReaders() {
    this.dataFileReader = mock(ReadBuilder.class);
    this.columnFileReader = mock(ReadBuilder.class);
    when(dataFileReader.build()).thenReturn(CloseableIterable.empty());
    when(columnFileReader.build()).thenReturn(CloseableIterable.empty());
    when(model.readBuilder(any())).thenReturn(dataFileReader, columnFileReader);
  }

  @Test
  void rejectsColumnFilesWithoutStitcher() {
    DataFile file = dataFile(List.of(COLUMN_FILE));

    assertThatThrownBy(() -> DataFileReadBuilder.read(file, Unstitchable.class, INPUT_FILES))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot read a data file with column files: no stitcher is registered for type "
                + Unstitchable.class);
  }

  @Test
  void projectsFromEachFileTheFieldsItProvides() {
    DataFileReadBuilder.read(dataFile(List.of(COLUMN_FILE)), Row.class, INPUT_FILES)
        .project(SCHEMA)
        .build();

    assertThat(projectedIds(dataFileReader)).containsExactly(1);
    assertThat(projectedIds(columnFileReader)).containsExactly(2, 3);
  }

  @Test
  void readsOnlyTheDataFileWhenNoColumnFileIsProjected() {
    DataFileReadBuilder.read(dataFile(List.of(COLUMN_FILE)), Row.class, INPUT_FILES)
        .project(TypeUtil.select(SCHEMA, Set.of(1)))
        .build();

    verify(dataFileReader).build();
    verify(columnFileReader, never()).build();
  }

  @Test
  void readsPositionsFromTheDataFileWhenItProvidesNoProjectedField() {
    DataFileReadBuilder.read(dataFile(List.of(COLUMN_FILE)), Row.class, INPUT_FILES)
        .project(TypeUtil.select(SCHEMA, Set.of(2, 3)))
        .build();

    assertThat(projectedIds(dataFileReader)).containsExactly(POSITION_ID);
    assertThat(projectedIds(columnFileReader)).containsExactly(2, 3);
  }

  @Test
  void passesOptionsOfTheWholeRowToTheDataFileOnly() {
    DataFileReadBuilder.read(dataFile(List.of(COLUMN_FILE)), Row.class, INPUT_FILES)
        .split(0L, 128L)
        .idToConstant(ImmutableMap.of(1, 42L))
        .caseSensitive(false);

    verify(dataFileReader).split(0L, 128L);
    verify(dataFileReader).idToConstant(ImmutableMap.of(1, 42L));
    verify(columnFileReader, never()).split(0L, 128L);
    verify(columnFileReader, never()).idToConstant(any());
    verify(columnFileReader).caseSensitive(false);
  }

  @Test
  void pushesTheWholeFilterToTheDataFileWhenNoColumnFileIsRead() {
    Expression filter = Expressions.equal("id", 1L);

    DataFileReadBuilder.read(dataFile(null), Row.class, INPUT_FILES)
        .project(SCHEMA)
        .filter(filter)
        .build();

    verify(dataFileReader).filter(filter);
  }

  @Test
  void pushesToEachFileTheConjunctsItCanEvaluate() {
    Expression dataFileConjunct = Expressions.equal("id", 1L);
    Expression columnFileConjunct = Expressions.equal("data", "a");

    DataFileReadBuilder.read(dataFile(List.of(COLUMN_FILE)), Row.class, INPUT_FILES)
        .project(SCHEMA)
        .filter(Expressions.and(dataFileConjunct, columnFileConjunct))
        .build();

    verify(dataFileReader).filter(dataFileConjunct);
    verify(columnFileReader).filter(columnFileConjunct);
  }

  @Test
  void skipsConjunctsSpanningSeveralFiles() {
    Expression dataFileConjunct = Expressions.equal("id", 1L);

    DataFileReadBuilder.read(dataFile(List.of(COLUMN_FILE)), Row.class, INPUT_FILES)
        .project(SCHEMA)
        .filter(
            Expressions.and(
                dataFileConjunct,
                Expressions.or(Expressions.equal("id", 2L), Expressions.equal("data", "a"))))
        .build();

    verify(dataFileReader).filter(dataFileConjunct);
    verify(columnFileReader, never()).filter(any());
  }

  @Test
  void rejectsUnknownInputFileLocation() {
    DataFile file = dataFile(null);

    assertThatThrownBy(() -> DataFileReadBuilder.read(file, Row.class, MISSING_FILES))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find input file for location: " + DATA_FILE_LOCATION);
  }

  @Test
  void rejectsInvalidArguments() {
    assertThatThrownBy(() -> DataFileReadBuilder.read(null, Row.class, INPUT_FILES))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid data file: null");

    assertThatThrownBy(
            () ->
                DataFileReadBuilder.read(
                    dataFile(null), Row.class, (Function<String, InputFile>) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid input file provider: null");
  }

  private static List<Integer> projectedIds(ReadBuilder<Row, Schema> reader) {
    ArgumentCaptor<Schema> captor = ArgumentCaptor.forClass(Schema.class);
    verify(reader).project(captor.capture());
    return captor.getValue().columns().stream().map(Types.NestedField::fieldId).toList();
  }

  private static DataFile dataFile(List<ColumnFile> columnFiles) {
    TrackedFile file =
        new TrackedFileStruct(
            new TrackingStruct(EntryStatus.ADDED, 42L, 10L, 11L, null, 1000L, null, null, null),
            FileContent.DATA,
            4,
            DATA_FILE_LOCATION,
            FileFormat.PARQUET,
            100L,
            1024L,
            PartitionSpec.unpartitioned().specId(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            columnFiles);

    return TrackedFileAdapters.asDataFile(file, UNPARTITIONED);
  }

  /** Row type of the format model registered by this test. */
  private static class Row {}

  /** Row type that has no stitcher registered. */
  private static class Unstitchable {}
}
