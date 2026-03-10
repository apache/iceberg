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
package org.apache.iceberg.lance;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.FileFormat;
import org.junit.jupiter.api.Test;

/** Unit tests for the LANCE entry in {@link FileFormat}. */
public class TestFileFormatLance {

  @Test
  public void testLanceEnumExists() {
    FileFormat lance = FileFormat.LANCE;
    assertThat(lance).isNotNull();
    assertThat(lance.name()).isEqualTo("LANCE");
  }

  @Test
  public void testLanceIsSplittable() {
    assertThat(FileFormat.LANCE.isSplittable()).isTrue();
  }

  @Test
  public void testLanceAddExtension() {
    String result = FileFormat.LANCE.addExtension("data");
    assertThat(result).isEqualTo("data.lance");
  }

  @Test
  public void testLanceAddExtensionIdempotent() {
    String result = FileFormat.LANCE.addExtension("data.lance");
    assertThat(result).isEqualTo("data.lance");
  }

  @Test
  public void testLanceFromFileName() {
    FileFormat format = FileFormat.fromFileName("test_data.lance");
    assertThat(format).isEqualTo(FileFormat.LANCE);
  }

  @Test
  public void testLanceFromString() {
    assertThat(FileFormat.fromString("lance")).isEqualTo(FileFormat.LANCE);
    assertThat(FileFormat.fromString("LANCE")).isEqualTo(FileFormat.LANCE);
    assertThat(FileFormat.fromString("Lance")).isEqualTo(FileFormat.LANCE);
  }

  @Test
  public void testOtherFormatsUnchanged() {
    assertThat(FileFormat.PARQUET.isSplittable()).isTrue();
    assertThat(FileFormat.ORC.isSplittable()).isTrue();
    assertThat(FileFormat.AVRO.isSplittable()).isTrue();
    assertThat(FileFormat.PUFFIN.isSplittable()).isFalse();
    assertThat(FileFormat.METADATA.isSplittable()).isFalse();
  }
}
