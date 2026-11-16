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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestStreamingInitialOffsetStore {

  @TempDir private Path checkpointDir;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void persistsOffsetAndCreatesParentDirectories(boolean scanAllFiles) {
    Path checkpoint = checkpointDir.resolve("nested/checkpoint");
    StreamingOffset expected = new StreamingOffset(34L, 7L, scanAllFiles);
    StreamingInitialOffsetStore store =
        new StreamingInitialOffsetStore(checkpoint.toString(), new Configuration(), () -> expected);

    assertThat(store.initialOffset()).isEqualTo(expected);
    assertThat(checkpoint.resolve("offsets/0"))
        .isRegularFile()
        .hasContent(
            "{\"version\":1,\"snapshot_id\":34,\"position\":7,\"scan_all_files\":%s}"
                .formatted(scanAllFiles));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void restoresStoredOffsetWithoutInitializing(boolean scanAllFiles) throws IOException {
    Path offsetFile = checkpointDir.resolve("offsets/0");
    Files.createDirectories(offsetFile.getParent());
    Files.writeString(
        offsetFile,
        "{\"version\":1,\"snapshot_id\":34,\"position\":7,\"scan_all_files\":%s}"
            .formatted(scanAllFiles));
    StreamingInitialOffsetStore store =
        new StreamingInitialOffsetStore(
            checkpointDir.toString(),
            new Configuration(),
            () -> {
              throw new AssertionError("Must restore the persisted offset without initializing");
            });

    assertThat(store.initialOffset()).isEqualTo(new StreamingOffset(34L, 7L, scanAllFiles));
  }

  @ParameterizedTest
  @MethodSource("invalidOffsets")
  void rejectsInvalidCheckpointWithoutReinitializing(
      String json, Class<? extends Throwable> exceptionClass, String message) throws IOException {
    Path offsetFile = checkpointDir.resolve("offsets/0");
    Files.createDirectories(offsetFile.getParent());
    Files.writeString(offsetFile, json);
    StreamingInitialOffsetStore store =
        new StreamingInitialOffsetStore(
            checkpointDir.toString(),
            new Configuration(),
            () -> {
              throw new AssertionError("Must not replace an invalid checkpoint with a new offset");
            });

    assertThatThrownBy(store::initialOffset)
        .isInstanceOf(exceptionClass)
        .hasMessageContaining(message);
    assertThat(offsetFile).hasContent(json);
  }

  private static Stream<Arguments> invalidOffsets() {
    return Stream.of(
        Arguments.of(
            "{\"version\":2,\"snapshot_id\":34,\"position\":7,\"scan_all_files\":false}",
            IllegalArgumentException.class,
            "Version 2 is not supported"),
        Arguments.of(
            "{\"version\":\"foo\",\"snapshot_id\":34,\"position\":7,\"scan_all_files\":false}",
            IllegalArgumentException.class,
            "Cannot parse to an integer value: version"),
        Arguments.of(
            "{\"snapshot_id\":34,\"position\":7,\"scan_all_files\":false}",
            IllegalArgumentException.class,
            "Cannot parse missing int: version"),
        Arguments.of(
            "{\"version\":1",
            UncheckedIOException.class,
            "Failed to read StreamingOffset from json"));
  }

  @Test
  void restoresStartOffsetWithoutReinitializing() {
    AtomicInteger initializations = new AtomicInteger();
    StreamingInitialOffsetStore firstStore =
        new StreamingInitialOffsetStore(
            checkpointDir.toString(),
            new Configuration(),
            () -> {
              initializations.incrementAndGet();
              return StreamingOffset.START_OFFSET;
            });

    assertThat(firstStore.initialOffset()).isEqualTo(StreamingOffset.START_OFFSET);

    StreamingInitialOffsetStore restoredStore =
        new StreamingInitialOffsetStore(
            checkpointDir.toString(),
            new Configuration(),
            () -> {
              initializations.incrementAndGet();
              return new StreamingOffset(34L, 0L, false);
            });

    assertThat(restoredStore.initialOffset()).isEqualTo(StreamingOffset.START_OFFSET);
    assertThat(initializations).hasValue(1);
  }
}
