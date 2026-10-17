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
package org.apache.iceberg.flink.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Writing a Flink {@code MULTISET<T>} column through the Iceberg data file writers.
 *
 * <p>{@code FlinkTypeToType#visit(MultisetType)} converts a {@code MULTISET<T>} to an Iceberg
 * {@code map<T, int>} of element to occurrence count, and {@code RowData} represents a map and a
 * multiset alike as {@code MapData}, so the writers handle a multiset as its equivalent map.
 *
 * <p>These tests pair the Iceberg map schema with a Flink {@link MultisetType}, which is the
 * pairing the write path sees for a multiset column. The other writer tests in this package cannot
 * produce it, because they derive the Flink type with {@code
 * FlinkSchemaUtil.convert(icebergSchema)}, which maps {@code map<string, int>} to a {@code
 * MapType}.
 */
class TestFlinkMultisetWrite {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(
              2,
              "tags",
              Types.MapType.ofRequired(3, 4, Types.StringType.get(), Types.IntegerType.get())));

  /** {@code ROW<id INT NOT NULL, tags MULTISET<STRING NOT NULL>>}. */
  private static final RowType MULTISET_OF_REQUIRED_STRING =
      rowTypeWithTags(new VarCharType(false, VarCharType.MAX_LENGTH));

  /** {@code ROW<id INT NOT NULL, tags MULTISET<STRING>>}, the nullable element case. */
  private static final RowType MULTISET_OF_NULLABLE_STRING =
      rowTypeWithTags(new VarCharType(true, VarCharType.MAX_LENGTH));

  private static final Map<String, Integer> TAGS = ImmutableMap.of("a", 2, "b", 1);

  @TempDir private Path temp;

  private static RowType rowTypeWithTags(LogicalType elementType) {
    return RowType.of(
        new LogicalType[] {new IntType(false), new MultisetType(true, elementType)},
        new String[] {"id", "tags"});
  }

  private static RowData rowWithTags(Map<String, Integer> tags) {
    if (tags == null) {
      return GenericRowData.of(1, null);
    }

    Map<Object, Object> counts = Maps.newHashMap();
    tags.forEach((tag, count) -> counts.put(StringData.fromString(tag), count));
    return GenericRowData.of(1, new GenericMapData(counts));
  }

  private static Map<String, Integer> tagsOf(RowData row) {
    if (row.isNullAt(1)) {
      return null;
    }

    MapData tags = row.getMap(1);
    ArrayData keys = tags.keyArray();
    ArrayData counts = tags.valueArray();
    Map<String, Integer> result = Maps.newHashMap();
    for (int i = 0; i < tags.size(); i++) {
      result.put(keys.getString(i).toString(), counts.getInt(i));
    }

    return result;
  }

  private static void assertRoundTrip(
      CloseableIterable<RowData> reader, Map<String, Integer> expectedTags) {
    Iterator<RowData> rows = reader.iterator();
    assertThat(rows).hasNext();
    RowData row = rows.next();
    assertThat(row.getInt(0)).isEqualTo(1);
    assertThat(tagsOf(row)).isEqualTo(expectedTags);
    assertThat(rows).isExhausted();
  }

  private void assertParquetRoundTrip(RowType flinkType, Map<String, Integer> tags)
      throws IOException {
    InMemoryOutputFile out = new InMemoryOutputFile();
    try (FileAppender<RowData> writer =
        Parquet.write(out)
            .schema(SCHEMA)
            .createWriterFunc(msgType -> FlinkParquetWriters.buildWriter(flinkType, msgType))
            .build()) {
      writer.add(rowWithTags(tags));
    }

    try (CloseableIterable<RowData> reader =
        Parquet.read(out.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(type -> FlinkParquetReaders.buildReader(SCHEMA, type))
            .build()) {
      assertRoundTrip(reader, tags);
    }
  }

  @Test
  void parquetRoundTripsMultiset() throws IOException {
    assertParquetRoundTrip(MULTISET_OF_REQUIRED_STRING, TAGS);
  }

  @Test
  void parquetRoundTripsMultisetOfNullableElement() throws IOException {
    assertParquetRoundTrip(MULTISET_OF_NULLABLE_STRING, TAGS);
  }

  @Test
  void parquetRoundTripsNullMultiset() throws IOException {
    assertParquetRoundTrip(MULTISET_OF_REQUIRED_STRING, null);
  }

  @Test
  void avroRoundTripsMultiset() throws IOException {
    InMemoryOutputFile out = new InMemoryOutputFile();
    try (FileAppender<RowData> writer =
        Avro.write(out)
            .schema(SCHEMA)
            .createWriterFunc(ignored -> new FlinkAvroWriter(MULTISET_OF_REQUIRED_STRING))
            .build()) {
      writer.add(rowWithTags(TAGS));
    }

    try (CloseableIterable<RowData> reader =
        Avro.read(out.toInputFile())
            .project(SCHEMA)
            .createResolvingReader(FlinkPlannedAvroReader::create)
            .build()) {
      assertRoundTrip(reader, TAGS);
    }
  }

  @Test
  void orcRoundTripsMultiset() throws IOException {
    File orcFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(orcFile.delete()).isTrue();

    OutputFile out = Files.localOutput(orcFile);
    try (FileAppender<RowData> writer =
        ORC.write(out)
            .schema(SCHEMA)
            .createWriterFunc(
                (iSchema, typDesc) ->
                    FlinkOrcWriter.buildWriter(MULTISET_OF_REQUIRED_STRING, iSchema))
            .build()) {
      writer.add(rowWithTags(TAGS));
    }

    try (CloseableIterable<RowData> reader =
        ORC.read(Files.localInput(orcFile))
            .project(SCHEMA)
            .createReaderFunc(type -> new FlinkOrcReader(SCHEMA, type))
            .build()) {
      assertRoundTrip(reader, TAGS);
    }
  }

  @Test
  void multisetConvertsToMapWithRequiredCount() {
    Types.MapType tags =
        FlinkSchemaUtil.convert(new MultisetType(true, new VarCharType(false, 42))).asMapType();

    assertThat(tags.keyType()).isEqualTo(Types.StringType.get());
    assertThat(tags.valueType()).isEqualTo(Types.IntegerType.get());
    assertThat(tags.isValueRequired())
        .as("the occurrence count a multiset converts to is required")
        .isTrue();
  }
}
