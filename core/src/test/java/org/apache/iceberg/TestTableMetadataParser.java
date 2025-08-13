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

import static org.apache.iceberg.PartitionSpec.unpartitioned;
import static org.apache.iceberg.TableMetadata.newTableMetadata;
import static org.apache.iceberg.TableMetadataParser.getFileExtension;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;
import org.apache.iceberg.TableMetadataParser.Codec;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.ContentCache;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types.BooleanType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestTableMetadataParser {

  private static final Schema SCHEMA = new Schema(optional(1, "b", BooleanType.get()));

  @Parameters(name = "codecName = {0}")
  private static List<Object> parameters() {
    return Arrays.asList("none", "gzip");
  }

  @Parameter private String codecName;

  private FileIO io;
  private final List<String> fileNames = new ArrayList<>();

  @TestTemplate
  public void testGzipCompressionProperty() throws IOException {
    io = new HadoopFileIO();
    String fileName = getFileName("v3");
    TableMetadata metadata = createTableMetadata();
    writeTableMetadata(fileName, metadata);
    readAndTestTableMetadata(fileName, metadata);

    ContentCache cache = TableMetadataParser.contentCache(io);
    assertThat(cache.stats().loadCount()).isEqualTo(0);
    assertThat(cache.stats().hitCount()).isEqualTo(0);
    assertThat(cache.stats().missCount()).isEqualTo(0);
  }

  @TestTemplate
  public void testGzipCompressionPropertyWithCache() throws IOException {
    io = new HadoopFileIO();
    io.initialize(ImmutableMap.of(CatalogProperties.IO_TABLE_METADATA_CACHE_ENABLED, "true"));
    String fileName = getFileName("v3");
    TableMetadata metadata = createTableMetadata();
    writeTableMetadata(fileName, metadata);
    readAndTestTableMetadata(fileName, metadata);

    ContentCache cache = TableMetadataParser.contentCache(io);
    assertThat(cache.stats().loadCount()).isEqualTo(1);
    assertThat(cache.stats().hitCount()).isEqualTo(0);
    assertThat(cache.stats().missCount()).isEqualTo(1);

    readAndTestTableMetadata(fileName, metadata);
    assertThat(cache.stats().loadCount()).isEqualTo(1);
    assertThat(cache.stats().hitCount()).isEqualTo(1);
    assertThat(cache.stats().missCount()).isEqualTo(1);

    String anotherFileName = getFileName("another-v3");
    writeTableMetadata(anotherFileName, metadata);
    readAndTestTableMetadata(anotherFileName, metadata);
    assertThat(cache.stats().loadCount()).isEqualTo(2);
    assertThat(cache.stats().hitCount()).isEqualTo(1);
    assertThat(cache.stats().missCount()).isEqualTo(2);
  }

  @TestTemplate
  public void testGzipCompressionPropertyWithSmallCache() throws IOException {
    io = new HadoopFileIO();
    io.initialize(ImmutableMap.of(
        CatalogProperties.IO_TABLE_METADATA_CACHE_ENABLED, "true",
        CatalogProperties.IO_TABLE_METADATA_CACHE_MAX_TOTAL_BYTES, "1",
        CatalogProperties.IO_TABLE_METADATA_CACHE_MAX_CONTENT_LENGTH, "1"
    ));
    String fileName = getFileName("v3");
    TableMetadata metadata = createTableMetadata();
    writeTableMetadata(fileName, metadata);
    readAndTestTableMetadata(fileName, metadata);

    ContentCache cache = TableMetadataParser.contentCache(io);
    assertThat(cache.stats().loadCount()).isEqualTo(0);
    assertThat(cache.stats().hitCount()).isEqualTo(0);
    assertThat(cache.stats().missCount()).isEqualTo(0);
  }

  @BeforeEach
  public void setup() {
    fileNames.clear();
  }

  @AfterEach
  public void cleanup() throws IOException {
    for (String fileName : fileNames) {
      java.nio.file.Files.deleteIfExists(Paths.get(fileName));
      java.nio.file.Files.deleteIfExists(Paths.get("." + fileName + ".crc"));
    }
    io.close();
  }

  private String getFileName(String prefix) {
    Codec codec = Codec.fromName(codecName);
    String fileExtension = getFileExtension(codec);
    String fileName = prefix + fileExtension;
    fileNames.add(fileName);
    return fileName;
  }

  private TableMetadata createTableMetadata() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.METADATA_COMPRESSION, codecName);
    String location = "file://tmp/db/table";
    return newTableMetadata(SCHEMA, unpartitioned(), location, properties);
  }

  private void writeTableMetadata(String fileName, TableMetadata metadata) {
    OutputFile outputFile = io.newOutputFile(fileName);
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.METADATA_COMPRESSION, codecName);
    TableMetadataParser.write(metadata, outputFile);
  }

  private void readAndTestTableMetadata(String fileName, TableMetadata metadata) throws IOException {
    assertThat(isCompressed(fileName)).isEqualTo(Codec.fromName(codecName) == Codec.GZIP);
    TableMetadata actualMetadata =
            TableMetadataParser.read(io, io.newInputFile(fileName));
    verifyMetadata(metadata, actualMetadata);
  }

  private void verifyMetadata(TableMetadata expected, TableMetadata actual) {
    assertThat(actual.schema().asStruct()).isEqualTo(expected.schema().asStruct());
    assertThat(actual.location()).isEqualTo(expected.location());
    assertThat(actual.lastColumnId()).isEqualTo(expected.lastColumnId());
    assertThat(actual.properties()).isEqualTo(expected.properties());
  }

  private boolean isCompressed(String path) throws IOException {
    try (InputStream ignored = new GZIPInputStream(new FileInputStream(path))) {
      return true;
    } catch (ZipException e) {
      if (e.getMessage().equals("Not in GZIP format")) {
        return false;
      } else {
        throw e;
      }
    }
  }
}
