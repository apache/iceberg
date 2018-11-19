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
package com.netflix.iceberg;

import com.netflix.iceberg.io.OutputFile;
import com.netflix.iceberg.types.Types.BooleanType;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;

import static avro.shaded.com.google.common.collect.Lists.newArrayList;
import static com.netflix.iceberg.ConfigProperties.COMPRESS_METADATA;
import static com.netflix.iceberg.PartitionSpec.unpartitioned;
import static com.netflix.iceberg.TableMetadata.newTableMetadata;
import static com.netflix.iceberg.TableMetadataParser.getFileExtension;
import static com.netflix.iceberg.types.Types.NestedField.optional;

public class TableMetadataParserTest {

  private final Schema SCHEMA = new Schema(newArrayList(optional(1, "b", BooleanType.get())));
  private final TableMetadata EXPECTED = newTableMetadata(null, SCHEMA, unpartitioned(), "file://tmp/db/table");

  @Test
  public void testCompressionProperty() throws IOException {
    final boolean[] props = {true, false};
    final Configuration configuration = new Configuration();
    for (boolean prop : props) {
      configuration.setBoolean(COMPRESS_METADATA, prop);
      final OutputFile outputFile = Files.localOutput(getFileExtension(configuration));
      TableMetadataParser.write(EXPECTED, outputFile);
      Assert.assertEquals(prop, isCompressed(getFileExtension(configuration)));
      final TableMetadata read = TableMetadataParser.read(null, Files.localInput(new File(getFileExtension(configuration))));
      verifyMetadata(read);
    }
  }

  @After
  public void cleanup() throws IOException {
    final boolean[] props = {true, false};
    Configuration configuration = new Configuration();
    for (boolean prop : props) {
      configuration.setBoolean(COMPRESS_METADATA, prop);
      java.nio.file.Files.deleteIfExists(Paths.get(getFileExtension(configuration)));
    }
  }

  private void verifyMetadata(TableMetadata read) {
    Assert.assertEquals(EXPECTED.schema().asStruct(), read.schema().asStruct());
    Assert.assertEquals(EXPECTED.location(), read.location());
    Assert.assertEquals(EXPECTED.lastColumnId(), read.lastColumnId());
    Assert.assertEquals(EXPECTED.properties(), read.properties());
  }

  private boolean isCompressed(String path) throws IOException {
    try (InputStream ignored = new GzipCompressorInputStream(new FileInputStream(new File(path)))) {
      return true;
    } catch (IOException e) {
      if (e.getMessage().equals("Input is not in the .gz format"))
        return false;
      else
        throw e;
    }
  }
}
