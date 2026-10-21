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
package org.apache.iceberg.parquet;

import static org.apache.iceberg.expressions.Expressions.equal;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.stream.Stream;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types.StringType;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TestParquetMetricsRowGroupFilter {
  @TempDir private File temp;

  private static Stream<LogicalTypeAnnotation> stringAnnotations() {
    return Stream.of(LogicalTypeAnnotation.enumType(), LogicalTypeAnnotation.jsonType());
  }

  @ParameterizedTest
  @MethodSource("stringAnnotations")
  void annotatedStringsWithoutFieldIds(LogicalTypeAnnotation annotation) throws IOException {
    MessageType writeSchema =
        new MessageType("test", Types.optional(PrimitiveTypeName.BINARY).as(annotation).named("s"));
    File file = new File(temp, "strings.parquet");
    String value = "\"café\"";
    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(ParquetIO.file(Files.localOutput(file)))
            .withType(writeSchema)
            .build()) {
      SimpleGroupFactory groups = new SimpleGroupFactory(writeSchema);
      writer.write(groups.newGroup().append("s", value));
    }

    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(Files.localInput(file)))) {
      MessageType fileSchema = reader.getFileMetaData().getSchema();
      assertThat(ParquetSchemaUtil.hasIds(fileSchema)).isFalse();
      Schema schema = ParquetSchemaUtil.convert(fileSchema);
      assertThat(schema.findType("s")).isEqualTo(StringType.get());
      assertThat(reader.getRowGroups()).hasSize(1);
      BlockMetaData rowGroup = reader.getRowGroups().get(0);
      assertThat(rowGroup.getColumns().get(0).getStatistics().hasNonNullValue()).isTrue();

      MessageType schemaWithIds = ParquetSchemaUtil.addFallbackIds(fileSchema);
      assertThat(
              new ParquetMetricsRowGroupFilter(schema, equal("s", value))
                  .shouldRead(schemaWithIds, rowGroup))
          .isTrue();
      assertThat(
              new ParquetMetricsRowGroupFilter(schema, equal("s", "\"aaa\""))
                  .shouldRead(schemaWithIds, rowGroup))
          .isFalse();
      assertThat(
              new ParquetMetricsRowGroupFilter(schema, equal("s", "\"zzz\""))
                  .shouldRead(schemaWithIds, rowGroup))
          .isFalse();
    }
  }
}
