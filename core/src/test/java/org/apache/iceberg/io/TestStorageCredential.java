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
package org.apache.iceberg.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestStorageCredential {
  @Test
  public void invalidPrefix() {
    assertThatThrownBy(() -> StorageCredential.create("", Map.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid prefix: must be non-empty");
  }

  @Test
  public void invalidConfig() {
    assertThatThrownBy(() -> StorageCredential.create("prefix", Map.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid config: must be non-empty");
  }

  @Test
  public void kryoSerDe() throws IOException {
    StorageCredential credential =
        StorageCredential.create("randomPrefix", Map.of("token", "storageToken"));
    assertThat(TestHelpers.KryoHelpers.roundTripSerialize(credential)).isEqualTo(credential);
  }

  @Test
  public void javaSerDe() throws IOException, ClassNotFoundException {
    StorageCredential credential =
        StorageCredential.create("randomPrefix", Map.of("token", "storageToken"));
    assertThat(TestHelpers.roundTripSerialize(credential)).isEqualTo(credential);
  }

  @Test
  public void roundTripFromToMap() {
    assertThat(StorageCredential.fromMap(ImmutableMap.of())).isEmpty();

    List<StorageCredential> credentials =
        ImmutableList.of(
            StorageCredential.create(
                "s3://bucket1",
                Map.of("accessKey", "key1", "secretKey", "secretKey1", "otherKey", "otherVal")),
            StorageCredential.create(
                "s3://bucket2",
                Map.of("accessKey", "key2", "secretKey", "secretKey2", "otherKey", "otherVal")),
            StorageCredential.create(
                "s3",
                Map.of(
                    "accessKey",
                    "genericKey",
                    "secretKey",
                    "genericSecretKey",
                    "key1",
                    "val1",
                    "key2",
                    "val2")));

    assertThat(StorageCredential.fromMap(StorageCredential.toMap(credentials)))
        .hasSameElementsAs(credentials);
  }

  @Test
  public void invalidToMap() {
    assertThatThrownBy(() -> StorageCredential.toMap(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid storage credentials: null");
  }

  @Test
  public void invalidFromMap() {
    assertThatThrownBy(() -> StorageCredential.fromMap(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid storage credentials config: null");

    assertThatThrownBy(
            () ->
                StorageCredential.fromMap(
                    ImmutableMap.of("storage-credential.s3://bucket", "s3://bucket")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid storage credential config with prefix s3://bucket: null or empty");
  }
}
