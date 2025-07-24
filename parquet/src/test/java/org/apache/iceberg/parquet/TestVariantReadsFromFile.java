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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hadoop.thirdparty.com.google.common.collect.Streams;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.InternalReader;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantTestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestVariantReadsFromFile {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "var", Types.VariantType.get()));

  private static final String CASE_LOCATION = null;
  private static final FileIO IO = new TestTables.LocalFileIO();

  private static Stream<JsonNode> cases() throws IOException {
    if (CASE_LOCATION == null) {
      return Stream.of(JsonUtil.mapper().readValue("{\"case_number\": -1}", JsonNode.class));
    }

    InputFile caseJsonInput = IO.newInputFile(CASE_LOCATION + "/cases.json");
    JsonNode cases = JsonUtil.mapper().readValue(caseJsonInput.newStream(), JsonNode.class);
    Preconditions.checkArgument(
        cases != null && cases.isArray(), "Invalid case JSON, not an array: %s", caseJsonInput);

    return Streams.stream(cases);
  }

  private static Stream<Arguments> errorCases() throws IOException {
    return cases()
        .filter(caseNode -> caseNode.has("error_message") || !caseNode.has("parquet_file"))
        .map(
            caseNode -> {
              int caseNumber = JsonUtil.getInt("case_number", caseNode);
              String testName = JsonUtil.getStringOrNull("test", caseNode);
              String parquetFile = JsonUtil.getStringOrNull("parquet_file", caseNode);
              String errorMessage = JsonUtil.getStringOrNull("error_message", caseNode);
              return Arguments.of(caseNumber, testName, parquetFile, errorMessage);
            });
  }

  private static Stream<Arguments> singleVariantCases() throws IOException {
    return cases()
        .filter(caseNode -> caseNode.has("variant_file") || !caseNode.has("parquet_file"))
        .map(
            caseNode -> {
              int caseNumber = JsonUtil.getInt("case_number", caseNode);
              String testName = JsonUtil.getStringOrNull("test", caseNode);
              String variant = JsonUtil.getStringOrNull("variant", caseNode);
              String parquetFile = JsonUtil.getStringOrNull("parquet_file", caseNode);
              String variantFile = JsonUtil.getStringOrNull("variant_file", caseNode);
              return Arguments.of(caseNumber, testName, variant, parquetFile, variantFile);
            });
  }

  private static Stream<Arguments> multiVariantCases() throws IOException {
    return cases()
        .filter(caseNode -> caseNode.has("variant_files") || !caseNode.has("parquet_file"))
        .map(
            caseNode -> {
              int caseNumber = JsonUtil.getInt("case_number", caseNode);
              String testName = JsonUtil.getStringOrNull("test", caseNode);
              String parquetFile = JsonUtil.getStringOrNull("parquet_file", caseNode);
              List<String> variantFiles =
                  caseNode.has("variant_files")
                      ? Lists.newArrayList(
                          Iterables.transform(
                              caseNode.get("variant_files"),
                              node -> node == null || node.isNull() ? null : node.asText()))
                      : null;
              String variants = JsonUtil.getStringOrNull("variants", caseNode);
              return Arguments.of(caseNumber, testName, variants, parquetFile, variantFiles);
            });
  }

  @ParameterizedTest
  @MethodSource("errorCases")
  public void testError(int caseNumber, String testName, String parquetFile, String errorMessage) {
    if (parquetFile == null) {
      return;
    }

    Assertions.assertThatThrownBy(() -> readParquet(parquetFile))
        .as("Test case %s: %s", caseNumber, testName)
        .hasMessageContaining(errorMessage);
  }

  @ParameterizedTest
  @MethodSource("singleVariantCases")
  public void testSingleVariant(
      int caseNumber, String testName, String variant, String parquetFile, String variantFile)
      throws IOException {
    if (parquetFile == null) {
      return;
    }

    Variant expected = readVariant(variantFile);

    Record record = readParquetRecord(parquetFile);
    Assertions.assertThat(record.getField("var")).isInstanceOf(Variant.class);
    Variant actual = (Variant) record.getField("var");
    VariantTestUtil.assertEqual(expected.metadata(), actual.metadata());
    VariantTestUtil.assertEqual(expected.value(), actual.value());
  }

  @ParameterizedTest
  @MethodSource("multiVariantCases")
  public void testMultiVariant(
      int caseNumber,
      String testName,
      String variants,
      String parquetFile,
      List<String> variantFiles)
      throws IOException {
    if (parquetFile == null) {
      return;
    }

    List<Record> records = readParquet(parquetFile);

    for (int i = 0; i < records.size(); i += 1) {
      Record record = records.get(i);
      String variantFile = variantFiles.get(i);

      if (variantFile != null) {
        Variant expected = readVariant(variantFile);
        Assertions.assertThat(record.getField("var")).isInstanceOf(Variant.class);
        Variant actual = (Variant) record.getField("var");
        VariantTestUtil.assertEqual(expected.metadata(), actual.metadata());
        VariantTestUtil.assertEqual(expected.value(), actual.value());
      } else {
        Assertions.assertThat(record.getField("var")).isNull();
      }
    }
  }

  private Variant readVariant(String variantFile) throws IOException {
    try (InputStream in = IO.newInputFile(CASE_LOCATION + "/" + variantFile).newStream()) {
      byte[] variantBytes = in.readAllBytes();
      return Variant.from(ByteBuffer.wrap(variantBytes).order(ByteOrder.LITTLE_ENDIAN));
    }
  }

  private Record readParquetRecord(String parquetFile) throws IOException {
    return Iterables.getOnlyElement(readParquet(parquetFile));
  }

  private List<Record> readParquet(String parquetFile) throws IOException {
    try (CloseableIterable<Record> reader =
        Parquet.read(IO.newInputFile(CASE_LOCATION + "/" + parquetFile))
            .project(SCHEMA)
            .createReaderFunc(fileSchema -> InternalReader.create(SCHEMA, fileSchema))
            .build()) {
      return Lists.newArrayList(reader);
    }
  }
}
