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
package org.apache.iceberg.encryption;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

public class TestStandardKeyMetadataParser {

  @Test
  public void testParser() {
    ByteBuffer encryptionKey = ByteBuffer.wrap("0123456789012345".getBytes(StandardCharsets.UTF_8));
    ByteBuffer aadPrefix = ByteBuffer.wrap("1234567890123456".getBytes(StandardCharsets.UTF_8));
    StandardKeyMetadata metadata =
        new StandardKeyMetadata(encryptionKey.array(), aadPrefix.array());
    ByteBuffer serialized = metadata.buffer();

    StandardKeyMetadata parsedMetadata = StandardKeyMetadata.parse(serialized);
    assertThat(encryptionKey).isEqualTo(parsedMetadata.encryptionKey());
    assertThat(aadPrefix).isEqualTo(parsedMetadata.aadPrefix());
  }

  @Test
  public void testUnsupportedVersion() {
    ByteBuffer badBuffer = ByteBuffer.wrap(new byte[] {0x02});
    assertThatThrownBy(() -> StandardKeyMetadata.parse(badBuffer))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot resolve schema for version: 2");
  }
}
