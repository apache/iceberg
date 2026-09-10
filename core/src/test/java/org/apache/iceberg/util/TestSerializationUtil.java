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
package org.apache.iceberg.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TestSerializationUtil {
  @Test
  void bytesRoundTripPreservesValue() {
    String original = "s3://bucket/table/metadata/v1.metadata.json";
    byte[] bytes = SerializationUtil.serializeToBytes(original);
    String roundTripped = SerializationUtil.deserializeFromBytes(bytes);
    assertThat(roundTripped).isEqualTo(original);
  }

  @Test
  void bytesRoundTripPreservesMapContents() {
    Map<String, Integer> original = new HashMap<>();
    original.put("added-records", 42);
    original.put("total-files", 7);

    byte[] bytes = SerializationUtil.serializeToBytes(original);
    Map<String, Integer> roundTripped = SerializationUtil.deserializeFromBytes(bytes);
    assertThat(roundTripped).isEqualTo(original);
  }

  @Test
  void deserializeFromBytesReturnsNullForNullInput() {
    assertThat((Object) SerializationUtil.deserializeFromBytes(null)).isNull();
  }

  @Test
  void base64RoundTripPreservesValue() {
    String original = "s3://bucket/table/metadata/v1.metadata.json";
    String encoded = SerializationUtil.serializeToBase64(original);
    String roundTripped = SerializationUtil.deserializeFromBase64(encoded);
    assertThat(roundTripped).isEqualTo(original);
  }

  @Test
  void deserializeFromBase64ReturnsNullForNullInput() {
    assertThat((Object) SerializationUtil.deserializeFromBase64(null)).isNull();
  }

  @Test
  void base64RoundTripHandlesMimeLineWrapping() {
    // A payload whose base64 exceeds 76 characters forces the MIME encoder to insert line breaks;
    // the round trip verifies the MIME decoder tolerates that wrapping.
    String original = "a".repeat(1000);
    String encoded = SerializationUtil.serializeToBase64(original);
    assertThat(encoded).contains("\n");

    String roundTripped = SerializationUtil.deserializeFromBase64(encoded);
    assertThat(roundTripped).isEqualTo(original);
  }
}
