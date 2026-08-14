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
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.Map;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * A Flink {@code MULTISET<T>} is converted to an Iceberg {@code map<T, int>} of element to
 * occurrence count by {@code FlinkTypeToType#visit(MultisetType)}, so a table with a multiset
 * column can be created. The write path pairs the Iceberg map with the Flink {@link MultisetType},
 * which is not a {@code MapType}, so every data file writer used to reject it and the column could
 * never be written.
 *
 * <p>The existing writer tests cannot cover this: they derive the Flink type with {@code
 * FlinkSchemaUtil.convert(icebergSchema)}, which turns {@code map<string, int>} back into a {@code
 * MapType} and never produces a {@link MultisetType}.
 */
public class TestFlinkMultisetWrite {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(
              2,
              "tags",
              Types.MapType.ofRequired(3, 4, Types.StringType.get(), Types.IntegerType.get())));

  // ROW<id INT NOT NULL, tags MULTISET<STRING NOT NULL>> — what Flink hands the writers for a
  // multiset column, and what FlinkSchemaUtil.convert(SCHEMA) can never produce.
  private static final RowType FLINK_TYPE =
      RowType.of(
          new LogicalType[] {
            new IntType(false),
            new MultisetType(true, new VarCharType(false, VarCharType.MAX_LENGTH))
          },
          new String[] {"id", "tags"});

  private static RowData row() {
    Map<Object, Object> counts =
        ImmutableMap.of(StringData.fromString("a"), 2, StringData.fromString("b"), 1);
    return GenericRowData.of(1, new GenericMapData(counts));
  }

  @Test
  public void testParquetAcceptsMultiset() {
    assertThatCode(
            () -> {
              OutputFile out = new InMemoryOutputFile();
              try (FileAppender<RowData> writer =
                  Parquet.write(out)
                      .schema(SCHEMA)
                      .createWriterFunc(
                          msgType -> FlinkParquetWriters.buildWriter(FLINK_TYPE, msgType))
                      .build()) {
                writer.add(row());
              }
            })
        .doesNotThrowAnyException();
  }

  @Test
  public void testAvroAcceptsMultiset() {
    assertThatCode(
            () -> {
              OutputFile out = new InMemoryOutputFile();
              try (FileAppender<RowData> writer =
                  Avro.write(out)
                      .schema(SCHEMA)
                      .createWriterFunc(ignored -> new FlinkAvroWriter(FLINK_TYPE))
                      .build()) {
                writer.add(row());
              }
            })
        .doesNotThrowAnyException();
  }

  @Test
  public void testOrcAcceptsMultiset() {
    assertThatCode(
            () -> {
              java.io.File orcFile =
                  new java.io.File(
                      java.nio.file.Files.createTempDirectory("multiset").toFile(), "data.orc");
              OutputFile out = org.apache.iceberg.Files.localOutput(orcFile);
              try (FileAppender<RowData> writer =
                  ORC.write(out)
                      .schema(SCHEMA)
                      .createWriterFunc(
                          (iSchema, typDesc) -> FlinkOrcWriter.buildWriter(FLINK_TYPE, iSchema))
                      .build()) {
                writer.add(row());
              }
            })
        .doesNotThrowAnyException();
  }

  @Test
  public void testMultisetValueIsARequiredInt() {
    assertThat(SCHEMA.findField("tags").type().asMapType().isValueRequired())
        .as("occurrence count must stay required, matching FlinkTypeToType")
        .isTrue();
  }
}
