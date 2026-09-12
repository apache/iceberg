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
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestPreSignedUrlReader {

  private static final String KEY = "data/part-0.parquet";

  private final Random random = new Random(1);
  @TempDir private Path temp;
  private PreSignedUrlTestServer server;
  private PreSignedUrlReader reader;

  @BeforeEach
  public void before() throws Exception {
    this.server = new PreSignedUrlTestServer(temp);
    this.reader = new PreSignedUrlReader(ImmutableMap.of());
  }

  @AfterEach
  public void after() throws Exception {
    reader.close();
    server.close();
  }

  @Test
  public void testRead() throws IOException {
    byte[] content = new byte[1024 * 1024];
    random.nextBytes(content);
    String url = put(content);

    InputFile file = reader.newInputFile(url, content.length);
    assertThat(file.location()).isEqualTo(url);
    assertThat(readAll(file)).isEqualTo(content);
    assertThat(server.ranges().get(0)).isEqualTo("bytes=0-");
  }

  @Test
  public void testLengthFromCaller() {
    byte[] content = new byte[10];
    random.nextBytes(content);
    InputFile file = reader.newInputFile(put(content), 12345);
    assertThat(file.getLength()).isEqualTo(12345);
    assertThat(server.ranges()).isEmpty();
  }

  @Test
  public void testLengthProbed() {
    byte[] content = new byte[10];
    random.nextBytes(content);
    InputFile file = reader.newInputFile(put(content), 0);

    assertThat(file.getLength()).isEqualTo(content.length);
    assertThat(file.getLength()).isEqualTo(content.length);
    assertThat(server.ranges()).hasSize(1);
    assertThat(server.ranges().get(0)).isEqualTo("bytes=0-0");
  }

  @Test
  public void testMissingObject() {
    InputFile file = reader.newInputFile(server.url("data/missing.parquet"), 10);
    assertThat(file.exists()).isFalse();
    assertThatThrownBy(() -> file.newStream().read()).isInstanceOf(NotFoundException.class);
  }

  @Test
  public void testSeek() throws IOException {
    byte[] content = new byte[3 * 1024 * 1024];
    random.nextBytes(content);
    InputFile file = reader.newInputFile(put(content), content.length);

    try (SeekableInputStream stream = file.newStream()) {
      assertThat(stream.read()).isEqualTo(content[0] & 0xff);

      // short forward seek: skip in the open stream
      stream.seek(100);
      assertThat(stream.read()).isEqualTo(content[100] & 0xff);
      assertThat(server.ranges()).hasSize(1);

      // long forward seek: reopen
      long far = 2L * 1024 * 1024 + 7;
      stream.seek(far);
      assertThat(stream.read()).isEqualTo(content[(int) far] & 0xff);
      assertThat(server.ranges()).hasSize(2);
      assertThat(server.ranges().get(1)).isEqualTo("bytes=" + far + "-");

      // backward seek: reopen
      stream.seek(5);
      byte[] chunk = new byte[10];
      ByteStreams.readFully(stream, chunk);
      assertThat(chunk).isEqualTo(Arrays.copyOfRange(content, 5, 15));
      assertThat(server.ranges()).hasSize(3);

      // at the end
      stream.seek(content.length);
      assertThat(stream.read()).isEqualTo(-1);
    }
  }

  @Test
  public void testServerIgnoringRange() {
    byte[] content = new byte[1024 * 1024];
    random.nextBytes(content);
    InputFile file = reader.newInputFile(put(content), content.length);
    server.ignoreRange = true;

    assertThatThrownBy(
            () -> {
              try (SeekableInputStream stream = file.newStream()) {
                stream.seek(512);
                stream.read();
              }
            })
        .isInstanceOf(IOException.class)
        .hasMessageContaining("ignored Range");
  }

  private String put(byte[] content) {
    server.put(KEY, content);
    return server.url(KEY);
  }

  private static byte[] readAll(InputFile file) throws IOException {
    try (SeekableInputStream stream = file.newStream()) {
      return ByteStreams.toByteArray(stream);
    }
  }
}
