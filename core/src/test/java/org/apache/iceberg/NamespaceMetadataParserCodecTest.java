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

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class NamespaceMetadataParserCodecTest {
  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testCompressionCodec() {
    Assert.assertEquals(NamespaceMetadataParser.Codec.GZIP,
        NamespaceMetadataParser.Codec.fromName("gzip"));
    Assert.assertEquals(NamespaceMetadataParser.Codec.GZIP,
        NamespaceMetadataParser.Codec.fromName("gZiP"));
    Assert.assertEquals(NamespaceMetadataParser.Codec.GZIP,
        NamespaceMetadataParser.Codec.fromFileName("v3.gz.namespace_metadata.json"));
    Assert.assertEquals(NamespaceMetadataParser.Codec.GZIP,
        NamespaceMetadataParser.Codec.fromFileName("v3-f326-4b66-a541-7b1c.gz.namespace_metadata.json"));
    Assert.assertEquals(NamespaceMetadataParser.Codec.GZIP,
        NamespaceMetadataParser.Codec.fromFileName("v3-f326-4b66-a541-7b1c.namespace_metadata.json.gz"));
    Assert.assertEquals(NamespaceMetadataParser.Codec.NONE,
        NamespaceMetadataParser.Codec.fromName("none"));
    Assert.assertEquals(NamespaceMetadataParser.Codec.NONE,
        NamespaceMetadataParser.Codec.fromName("nOnE"));
    Assert.assertEquals(NamespaceMetadataParser.Codec.NONE,
        NamespaceMetadataParser.Codec.fromFileName("v3.namespace_metadata.json"));
    Assert.assertEquals(NamespaceMetadataParser.Codec.NONE,
        NamespaceMetadataParser.Codec.fromFileName("v3-f326-4b66-a541-7b1c.namespace_metadata.json"));
  }

  @Test
  public void testInvalidCodecName() {
    exceptionRule.expect(IllegalArgumentException.class);
    exceptionRule.expectMessage("No enum constant");
    NamespaceMetadataParser.Codec.fromName("invalid");
  }

  @Test
  public void testInvalidFileName() {
    exceptionRule.expect(IllegalArgumentException.class);
    exceptionRule.expectMessage("path/to/file.parquet is not a valid metadata file");
    NamespaceMetadataParser.Codec.fromFileName("path/to/file.parquet");
  }
}
