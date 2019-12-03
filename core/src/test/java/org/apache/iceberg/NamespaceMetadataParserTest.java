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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.io.OutputFile;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


@RunWith(Parameterized.class)
public class NamespaceMetadataParserTest {
  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { "none" },
        new Object[] { "gzip" }
    };
  }

  private final String codecName;

  public NamespaceMetadataParserTest(String codecName) {
    this.codecName = codecName;
  }

  @Test
  public void testCompressionProperty() throws IOException {
    NamespaceMetadataParser.Codec codec = NamespaceMetadataParser.Codec.fromName(codecName);
    String fileExtension = NamespaceMetadataParser.getFileExtension(codec);
    String fileName = "v3" + fileExtension;
    OutputFile outputFile = Files.localOutput(fileName);

    String location = "file://tmp/db/";
    Namespace namespace = Namespace.of("n1", "n2", "n3");
    namespace.setProperties(TableProperties.METADATA_COMPRESSION, codecName);
    namespace.setProperties("owner", "apache");
    namespace.addTables("table1");
    namespace.addTables("table2");
    NamespaceMetadata metadata = NamespaceMetadata.newNamespaceMetadata(location, namespace);
    NamespaceMetadataParser.write(metadata, outputFile);
    Assert.assertEquals(codec == NamespaceMetadataParser.Codec.GZIP, isCompressed(fileName));
    NamespaceMetadata actualMetadata = NamespaceMetadataParser.read(null, Files.localInput(new File(fileName)));
    verifyMetadata(metadata, actualMetadata);
  }

  @After
  public void cleanup() throws IOException {
    NamespaceMetadataParser.Codec codec = NamespaceMetadataParser.Codec.fromName(codecName);
    Path metadataFilePath = Paths.get("v3" + NamespaceMetadataParser.getFileExtension(codec));
    java.nio.file.Files.deleteIfExists(metadataFilePath);
  }

  private void verifyMetadata(NamespaceMetadata expected, NamespaceMetadata actual) {
    Assert.assertEquals(expected.location(), actual.location());
    Assert.assertEquals(expected.uuid(), actual.uuid());
  }

  private boolean isCompressed(String path) throws IOException {
    try (InputStream ignored = new GZIPInputStream(new FileInputStream(new File(path)))) {
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
