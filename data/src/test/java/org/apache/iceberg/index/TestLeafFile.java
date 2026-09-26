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
package org.apache.iceberg.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.Files;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestLeafFile {

  @TempDir private Path tempDir;

  private static final Types.NestedField STRING_KEY_FIELD =
      Types.NestedField.required(3, "order_id", Types.StringType.get());
  private static final Types.NestedField LONG_KEY_FIELD =
      Types.NestedField.required(7, "created_at", Types.LongType.get());

  private File newFile(String name) {
    return new File(tempDir.toFile(), name);
  }

  @Test
  void writeAndReadAllRoundTrip() {
    File file = newFile("leaf-roundtrip.parquet");

    try (LeafFileWriter writer = new LeafFileWriter(Files.localOutput(file), STRING_KEY_FIELD)) {
      writer.add(
          LeafFileEntry.builder()
              .keyValue("abc-123")
              .transformValue(42L)
              .filePath("s3://warehouse/orders/data-0.parquet")
              .position(17L)
              .build());
      writer.add(
          LeafFileEntry.builder()
              .keyValue("def-456")
              .transformValue(42L)
              .filePath("s3://warehouse/orders/data-0.parquet")
              .position(203L)
              .build());
    }

    List<LeafFileEntry> entries =
        LeafFileReader.readAll(Files.localInput(file), STRING_KEY_FIELD);

    assertThat(entries).hasSize(2);
    assertThat(entries.get(0).keyValue()).isEqualTo("abc-123");
    assertThat(entries.get(0).transformValue()).isEqualTo(42L);
    assertThat(entries.get(0).filePath()).isEqualTo("s3://warehouse/orders/data-0.parquet");
    assertThat(entries.get(0).position()).isEqualTo(17L);
    assertThat(entries.get(1).keyValue()).isEqualTo("def-456");
    assertThat(entries.get(1).position()).isEqualTo(203L);
  }

  @Test
  void readMatchingFindsExactEqualityMatch() {
    File file = newFile("leaf-equality.parquet");

    try (LeafFileWriter writer = new LeafFileWriter(Files.localOutput(file), STRING_KEY_FIELD)) {
      writer.addAll(
          List.of(
              LeafFileEntry.builder()
                  .keyValue("aaa")
                  .transformValue(1L)
                  .filePath("f1.parquet")
                  .position(0L)
                  .build(),
              LeafFileEntry.builder()
                  .keyValue("bbb")
                  .transformValue(1L)
                  .filePath("f1.parquet")
                  .position(1L)
                  .build(),
              LeafFileEntry.builder()
                  .keyValue("ccc")
                  .transformValue(1L)
                  .filePath("f1.parquet")
                  .position(2L)
                  .build()));
    }

    List<LeafFileEntry> matches =
        LeafFileReader.readMatching(
            Files.localInput(file),
            STRING_KEY_FIELD,
            Expressions.equal(STRING_KEY_FIELD.name(), "bbb"));

    assertThat(matches).hasSize(1);
    assertThat(matches.get(0).keyValue()).isEqualTo("bbb");
    assertThat(matches.get(0).position()).isEqualTo(1L);
  }

  @Test
  void readMatchingFindsNoMatchForAbsentKey() {
    File file = newFile("leaf-nomatch.parquet");

    try (LeafFileWriter writer = new LeafFileWriter(Files.localOutput(file), STRING_KEY_FIELD)) {
      writer.add(
          LeafFileEntry.builder()
              .keyValue("aaa")
              .transformValue(1L)
              .filePath("f1.parquet")
              .position(0L)
              .build());
    }

    List<LeafFileEntry> matches =
        LeafFileReader.readMatching(
            Files.localInput(file),
            STRING_KEY_FIELD,
            Expressions.equal(STRING_KEY_FIELD.name(), "does-not-exist"));

    assertThat(matches).isEmpty();
  }

  @Test
  void readMatchingRangeFindsRangeMatchesForLongKey() {
    File file = newFile("leaf-range.parquet");

    try (LeafFileWriter writer = new LeafFileWriter(Files.localOutput(file), LONG_KEY_FIELD)) {
      writer.addAll(
          List.of(
              LeafFileEntry.builder()
                  .keyValue(100L)
                  .transformValue(100L)
                  .filePath("f1.parquet")
                  .position(0L)
                  .build(),
              LeafFileEntry.builder()
                  .keyValue(200L)
                  .transformValue(200L)
                  .filePath("f1.parquet")
                  .position(1L)
                  .build(),
              LeafFileEntry.builder()
                  .keyValue(300L)
                  .transformValue(300L)
                  .filePath("f1.parquet")
                  .position(2L)
                  .build(),
              LeafFileEntry.builder()
                  .keyValue(400L)
                  .transformValue(400L)
                  .filePath("f1.parquet")
                  .position(3L)
                  .build()));
    }

    List<LeafFileEntry> matches =
        LeafFileReader.readMatching(
            Files.localInput(file),
            LONG_KEY_FIELD,
            Expressions.and(
                Expressions.greaterThanOrEqual(LONG_KEY_FIELD.name(), 150L),
                Expressions.lessThanOrEqual(LONG_KEY_FIELD.name(), 350L)));

    assertThat(matches).hasSize(2);
    assertThat(matches.get(0).keyValue()).isEqualTo(200L);
    assertThat(matches.get(1).keyValue()).isEqualTo(300L);
  }

  @Test
  void leafFileEntryBuilderRejectsMissingFields() {
    assertThatThrownBy(() -> LeafFileEntry.builder().transformValue(1L).position(0L).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("keyValue is required");

    assertThatThrownBy(() -> LeafFileEntry.builder().keyValue("x").position(-1L).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("filePath is required");

    assertThatThrownBy(
            () ->
                LeafFileEntry.builder().keyValue("x").filePath("f.parquet").position(-1L).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("position must be >= 0");
  }

  @Test
  void writerRejectsOutOfOrderTransformValue() {
    File file = newFile("leaf-unsorted-transform-value.parquet");

    try (LeafFileWriter writer = new LeafFileWriter(Files.localOutput(file), STRING_KEY_FIELD)) {
      writer.add(
          LeafFileEntry.builder()
              .keyValue("aaa")
              .transformValue(5L)
              .filePath("f1.parquet")
              .position(0L)
              .build());

      assertThatThrownBy(
              () ->
                  writer.add(
                      LeafFileEntry.builder()
                          .keyValue("bbb")
                          .transformValue(4L)
                          .filePath("f1.parquet")
                          .position(1L)
                          .build()))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("must be added in non-decreasing")
          .hasMessageContaining("(4, bbb)")
          .hasMessageContaining("(5, aaa)");
    }
  }

  @Test
  void writerRejectsOutOfOrderKeyValueWithinSameTransformValue() {
    File file = newFile("leaf-unsorted-key.parquet");

    try (LeafFileWriter writer = new LeafFileWriter(Files.localOutput(file), STRING_KEY_FIELD)) {
      writer.add(
          LeafFileEntry.builder()
              .keyValue("bbb")
              .transformValue(1L)
              .filePath("f1.parquet")
              .position(0L)
              .build());

      assertThatThrownBy(
              () ->
                  writer.add(
                      LeafFileEntry.builder()
                          .keyValue("aaa")
                          .transformValue(1L)
                          .filePath("f1.parquet")
                          .position(1L)
                          .build()))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("must be added in non-decreasing");
    }
  }

  @Test
  void writerAllowsEqualConsecutiveKeys() {
    File file = newFile("leaf-duplicate-keys.parquet");

    try (LeafFileWriter writer = new LeafFileWriter(Files.localOutput(file), STRING_KEY_FIELD)) {
      writer.add(
          LeafFileEntry.builder()
              .keyValue("aaa")
              .transformValue(1L)
              .filePath("f1.parquet")
              .position(0L)
              .build());
      // Same (transform_value, key_value) as the previous entry -- must not throw, since a key
      // value is not required to be unique across rows.
      writer.add(
          LeafFileEntry.builder()
              .keyValue("aaa")
              .transformValue(1L)
              .filePath("f1.parquet")
              .position(1L)
              .build());
    }

    List<LeafFileEntry> entries =
        LeafFileReader.readAll(Files.localInput(file), STRING_KEY_FIELD);
    assertThat(entries).hasSize(2);
  }
}
