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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;
import org.apache.iceberg.TableMetadataParser.Codec;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types.BooleanType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TableMetadataParserTest {

  private static final Schema SCHEMA = new Schema(optional(1, "b", BooleanType.get()));

  @Parameters(name = "codecName = {0}")
  private static List<Object> parameters() {
    return Arrays.asList("none", "gzip");
  }

  @Parameter private String codecName;

  @TestTemplate
  public void testGzipCompressionProperty() throws IOException {
    Codec codec = Codec.fromName(codecName);
    String fileExtension = getFileExtension(codec);
    String fileName = "v3" + fileExtension;
    OutputFile outputFile = Files.localOutput(fileName);
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.METADATA_COMPRESSION, codecName);
    String location = "file://tmp/db/table";
    TableMetadata metadata = newTableMetadata(SCHEMA, unpartitioned(), location, properties);
    TableMetadataParser.write(metadata, outputFile);
    assertThat(isCompressed(fileName)).isEqualTo(codec == Codec.GZIP);
    TableMetadata actualMetadata =
        TableMetadataParser.read(null, Files.localInput(new File(fileName)));
    verifyMetadata(metadata, actualMetadata);
  }

  @AfterEach
  public void cleanup() throws IOException {
    Codec codec = Codec.fromName(codecName);
    Path metadataFilePath = Paths.get("v3" + getFileExtension(codec));
    java.nio.file.Files.deleteIfExists(metadataFilePath);
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
