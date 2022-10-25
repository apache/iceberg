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
package org.apache.iceberg.puffin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.UncheckedIOException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Test;

public class TestFileMetadataParser {
  @Test
  public void testInvalidJson() {
    assertThatThrownBy(() -> FileMetadataParser.fromJson((String) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("argument \"content\" is null");

    assertThatThrownBy(() -> FileMetadataParser.fromJson(""))
        .isInstanceOf(UncheckedIOException.class)
        .hasMessageContaining("No content to map due to end-of-input");

    assertThatThrownBy(() -> FileMetadataParser.fromJson("{"))
        .isInstanceOf(UncheckedIOException.class)
        .hasMessageContaining("Unexpected end-of-input: expected close marker for Object");

    assertThatThrownBy(() -> FileMetadataParser.fromJson("{\"blobs\": []"))
        .isInstanceOf(UncheckedIOException.class)
        .hasMessageContaining("Unexpected end-of-input: expected close marker for Object");
  }

  @Test
  public void testMinimalFileMetadata() {
    testJsonSerialization(
        new FileMetadata(ImmutableList.of(), ImmutableMap.of()),
        "{\n" + "  \"blobs\" : [ ]\n" + "}");
  }

  @Test
  public void testFileProperties() {
    testJsonSerialization(
        new FileMetadata(ImmutableList.of(), ImmutableMap.of("a property", "a property value")),
        "{\n"
            + "  \"blobs\" : [ ],\n"
            + "  \"properties\" : {\n"
            + "    \"a property\" : \"a property value\"\n"
            + "  }\n"
            + "}");

    testJsonSerialization(
        new FileMetadata(
            ImmutableList.of(),
            ImmutableMap.of("a property", "a property value", "another one", "also with value")),
        "{\n"
            + "  \"blobs\" : [ ],\n"
            + "  \"properties\" : {\n"
            + "    \"a property\" : \"a property value\",\n"
            + "    \"another one\" : \"also with value\"\n"
            + "  }\n"
            + "}");
  }

  @Test
  public void testMissingBlobs() {
    assertThatThrownBy(() -> FileMetadataParser.fromJson("{\"properties\": {}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: blobs");
  }

  @Test
  public void testBadBlobs() {
    assertThatThrownBy(() -> FileMetadataParser.fromJson("{\"blobs\": {}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse blobs from non-array: {}");
  }

  @Test
  public void testBlobMetadata() {
    testJsonSerialization(
        new FileMetadata(
            ImmutableList.of(
                new BlobMetadata(
                    "type-a", ImmutableList.of(1), 14, 3, 4, 16, null, ImmutableMap.of()),
                new BlobMetadata(
                    "type-bbb",
                    ImmutableList.of(2, 3, 4),
                    77,
                    4,
                    Integer.MAX_VALUE * 10000L,
                    79834,
                    null,
                    ImmutableMap.of())),
            ImmutableMap.of()),
        "{\n"
            + "  \"blobs\" : [ {\n"
            + "    \"type\" : \"type-a\",\n"
            + "    \"fields\" : [ 1 ],\n"
            + "    \"snapshot-id\" : 14,\n"
            + "    \"sequence-number\" : 3,\n"
            + "    \"offset\" : 4,\n"
            + "    \"length\" : 16\n"
            + "  }, {\n"
            + "    \"type\" : \"type-bbb\",\n"
            + "    \"fields\" : [ 2, 3, 4 ],\n"
            + "    \"snapshot-id\" : 77,\n"
            + "    \"sequence-number\" : 4,\n"
            + "    \"offset\" : 21474836470000,\n"
            + "    \"length\" : 79834\n"
            + "  } ]\n"
            + "}");
  }

  @Test
  public void testBlobProperties() {
    testJsonSerialization(
        new FileMetadata(
            ImmutableList.of(
                new BlobMetadata(
                    "type-a",
                    ImmutableList.of(1),
                    14,
                    3,
                    4,
                    16,
                    null,
                    ImmutableMap.of("some key", "some value"))),
            ImmutableMap.of()),
        "{\n"
            + "  \"blobs\" : [ {\n"
            + "    \"type\" : \"type-a\",\n"
            + "    \"fields\" : [ 1 ],\n"
            + "    \"snapshot-id\" : 14,\n"
            + "    \"sequence-number\" : 3,\n"
            + "    \"offset\" : 4,\n"
            + "    \"length\" : 16,\n"
            + "    \"properties\" : {\n"
            + "      \"some key\" : \"some value\"\n"
            + "    }\n"
            + "  } ]\n"
            + "}");
  }

  @Test
  public void testFieldNumberOutOfRange() {
    assertThatThrownBy(
            () ->
                FileMetadataParser.fromJson(
                    "{\n"
                        + "  \"blobs\" : [ {\n"
                        + "    \"type\" : \"type-a\",\n"
                        + "    \"fields\" : [ "
                        + (Integer.MAX_VALUE + 1L)
                        + " ],\n"
                        + "    \"offset\" : 4,\n"
                        + "    \"length\" : 16\n"
                        + "  } ]\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse integer from non-int value in fields: 2147483648");
  }

  private void testJsonSerialization(FileMetadata fileMetadata, String json) {
    assertThat(FileMetadataParser.toJson(fileMetadata, true)).isEqualTo(json);

    // Test round-trip. Note that FileMetadata doesn't implement equals()
    FileMetadata parsed = FileMetadataParser.fromJson(json);
    assertThat(FileMetadataParser.toJson(parsed, true)).isEqualTo(json);
  }
}
