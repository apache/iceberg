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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.TableMetadataParser.Codec;
import org.junit.jupiter.api.Test;

public class TableMetadataParserCodecTest {

  @Test
  public void testCompressionCodec() {
    assertThat(Codec.fromName("gzip")).isEqualTo(Codec.GZIP);
    assertThat(Codec.fromName("gZiP")).isEqualTo(Codec.GZIP);
    assertThat(Codec.fromFileName("v3.gz.metadata.json")).isEqualTo(Codec.GZIP);
    assertThat(Codec.fromFileName("v3-f326-4b66-a541-7b1c.gz.metadata.json")).isEqualTo(Codec.GZIP);
    assertThat(Codec.fromFileName("v3-f326-4b66-a541-7b1c.metadata.json.gz")).isEqualTo(Codec.GZIP);
    assertThat(Codec.fromName("none")).isEqualTo(Codec.NONE);
    assertThat(Codec.fromName("nOnE")).isEqualTo(Codec.NONE);
    assertThat(Codec.fromFileName("v3.metadata.json")).isEqualTo(Codec.NONE);
    assertThat(Codec.fromFileName("v3-f326-4b66-a541-7b1c.metadata.json")).isEqualTo(Codec.NONE);
  }

  @Test
  public void testInvalidCodecName() {
    assertThatThrownBy(() -> Codec.fromName("invalid"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid codec name: invalid");
  }

  @Test
  public void testInvalidFileName() {
    assertThatThrownBy(() -> Codec.fromFileName("path/to/file.parquet"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("path/to/file.parquet is not a valid metadata file");
  }
}
