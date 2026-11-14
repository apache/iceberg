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
package org.apache.iceberg.spark.data.vectorized.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.data.SparkParquetReaders;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkParquetReaders;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.variants.VariantTestUtil;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class TestParquetVectorizedVariantReads {
  @TempDir private File temp;

  @ParameterizedTest
  @CsvSource({"true,true", "true,false", "false,true", "false,false"})
  void missingPayloadIsVariantNull(boolean required, boolean dictionary) throws IOException {
    Schema schema = new Schema(Types.NestedField.of(1, !required, "var", Types.VariantType.get()));
    MessageType fileSchema =
        MessageTypeParser.parseMessageType(
            "message test { "
                + (required ? "required" : "optional")
                + " group var (VARIANT(1)) = 1 { required binary metadata; optional binary value; } }");
    byte[] metadata = ByteBuffers.toByteArray(VariantTestUtil.emptyMetadata());
    byte[] variantNull = {0};
    byte[][] expected =
        required
            ? new byte[][] {variantNull, variantNull}
            : new byte[][] {variantNull, variantNull, null};
    File file = new File(temp, "variants.parquet");
    SimpleGroupFactory groups = new SimpleGroupFactory(fileSchema);
    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(new LocalOutputFile(file.toPath()))
            .withType(fileSchema)
            .withDictionaryEncoding(dictionary)
            .build()) {
      Group missing = groups.newGroup();
      missing.addGroup("var").append("metadata", Binary.fromConstantByteArray(metadata));
      writer.write(missing);
      Group explicitNull = groups.newGroup();
      explicitNull
          .addGroup("var")
          .append("metadata", Binary.fromConstantByteArray(metadata))
          .append("value", Binary.fromConstantByteArray(variantNull));
      writer.write(explicitNull);
      if (!required) {
        writer.write(groups.newGroup());
      }
    }

    try (CloseableIterable<InternalRow> rows =
        Parquet.read(Files.localInput(file))
            .project(schema)
            .createReaderFunc(type -> SparkParquetReaders.buildReader(schema, type))
            .build()) {
      assertVariants(rows.iterator(), expected, metadata);
    }

    try (CloseableIterable<ColumnarBatch> batches =
        Parquet.read(Files.localInput(file))
            .project(schema)
            .recordsPerBatch(expected.length)
            .createBatchedReaderFunc(
                type -> VectorizedSparkParquetReaders.buildReader(schema, type, Map.of()))
            .build()) {
      Iterator<ColumnarBatch> iterator = batches.iterator();
      assertThat(iterator.hasNext()).isTrue();
      ColumnarBatch batch = iterator.next();
      assertVariants(batch.rowIterator(), expected, metadata);
      assertThat(batch.column(0).hasNull()).isEqualTo(!required);
      assertThat(batch.column(0).numNulls()).isEqualTo(required ? 0 : 1);
      assertThat(batch.column(0).getChild(0).getBinary(0)).isEqualTo(variantNull);
      assertThat(iterator.hasNext()).isFalse();
    }
  }

  private static void assertVariants(
      Iterator<InternalRow> rows, byte[][] expected, byte[] metadata) {
    for (byte[] value : expected) {
      assertThat(rows.hasNext()).isTrue();
      InternalRow row = rows.next();
      assertThat(row.isNullAt(0)).isEqualTo(value == null);
      if (value != null) {
        assertThat(row.getVariant(0).getValue()).isEqualTo(value);
        assertThat(row.getVariant(0).getMetadata()).isEqualTo(metadata);
      } else {
        assertThat(row.getVariant(0)).isNull();
      }
    }

    assertThat(rows.hasNext()).isFalse();
  }
}
