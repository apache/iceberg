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
package org.apache.iceberg.flink;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.Vector;
import java.util.stream.Collectors;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class FlinkFileIOTest {
  private final Random random = new Random(1);

  private FileSystem fs;
  private FlinkFileIO flinkFileIO;

  @TempDir private File tempDir;

  @BeforeEach
  public void before() throws Exception {
    fs = FileSystem.getLocalFileSystem();

    flinkFileIO = new FlinkFileIO();
  }

  @Test
  public void testListPrefix() {
    Path parent = new Path(tempDir.toURI());

    List<Integer> scaleSizes = Lists.newArrayList(1, 1000, 2500);

    scaleSizes
        .parallelStream()
        .forEach(
            scale -> {
              Path scalePath = new Path(parent, Integer.toString(scale));

              createRandomFiles(scalePath, scale);
              Assertions.assertThat(
                      Streams.stream(flinkFileIO.listPrefix(scalePath.toUri().toString())).count())
                  .isEqualTo((long) scale);
            });

    long totalFiles = scaleSizes.stream().mapToLong(Integer::longValue).sum();
    Assertions.assertThat(Streams.stream(flinkFileIO.listPrefix(parent.toUri().toString())).count())
        .isEqualTo(totalFiles);
  }

  @Test
  public void testFileExists() throws IOException {
    Path parent = new Path(tempDir.toURI());
    Path randomFilePath = new Path(parent, "random-file-" + UUID.randomUUID());
    fs.create(randomFilePath, FileSystem.WriteMode.OVERWRITE);

    // check existence of the created file
    Assertions.assertThat(flinkFileIO.newInputFile(randomFilePath.toUri().toString()).exists())
        .isTrue();
    fs.delete(randomFilePath, false);
    Assertions.assertThat(flinkFileIO.newInputFile(randomFilePath.toUri().toString()).exists())
        .isFalse();
  }

  @Test
  public void testDeletePrefix() {
    Path parent = new Path(tempDir.toURI());

    List<Integer> scaleSizes = Lists.newArrayList(1, 1000, 2500);

    scaleSizes
        .parallelStream()
        .forEach(
            scale -> {
              Path scalePath = new Path(parent, Integer.toString(scale));

              createRandomFiles(scalePath, scale);
              flinkFileIO.deletePrefix(scalePath.toUri().toString());

              // Hadoop filesystem will throw if the path does not exist
              Assertions.assertThatThrownBy(
                      () -> flinkFileIO.listPrefix(scalePath.toUri().toString()).iterator())
                  .isInstanceOf(UncheckedIOException.class)
                  .hasMessageContaining("java.io.FileNotFoundException");
            });

    flinkFileIO.deletePrefix(parent.toUri().toString());
    // Hadoop filesystem will throw if the path does not exist
    Assertions.assertThatThrownBy(
            () -> flinkFileIO.listPrefix(parent.toUri().toString()).iterator())
        .isInstanceOf(UncheckedIOException.class)
        .hasMessageContaining("java.io.FileNotFoundException");
  }

  @Test
  public void testDeleteFiles() {
    Path parent = new Path(tempDir.toURI());
    List<Path> filesCreated = createRandomFiles(parent, 10);
    flinkFileIO.deleteFiles(filesCreated.stream().map(Path::toString).collect(Collectors.toList()));
    filesCreated.forEach(
        file ->
            Assertions.assertThat(flinkFileIO.newInputFile(file.toString()).exists()).isFalse());
  }

  @Test
  public void testDeleteFilesErrorHandling() {
    List<String> filesCreated =
        random.ints(2).mapToObj(x -> "fakefsnotreal://file-" + x).collect(Collectors.toList());
    Assertions.assertThatThrownBy(() -> flinkFileIO.deleteFiles(filesCreated))
        .isInstanceOf(BulkDeletionFailureException.class)
        .hasMessage("Failed to delete 2 files");
  }

  @Test
  public void testFlinkFileIOReadWrite() throws IOException {
    FileIO testFlinkFileIO = new FlinkFileIO();

    Path parent = new Path(tempDir.toURI());
    Path randomFilePath = new Path(parent, "random-file-" + UUID.randomUUID());
    byte[] expected = "DUMMY".getBytes(StandardCharsets.UTF_8);

    // Write
    OutputFile outputFile = testFlinkFileIO.newOutputFile(randomFilePath.getPath());
    try (PositionOutputStream outputStream = outputFile.create()) {
      outputStream.write(expected);
    }

    // Read
    InputFile inputFile = testFlinkFileIO.newInputFile(randomFilePath.getPath());
    try (SeekableInputStream inputStream = inputFile.newStream()) {
      byte[] actual = new byte[(int) inputFile.getLength()];
      inputStream.read(actual);
      Assertions.assertThat(actual).isEqualTo(expected);
    }
  }

  @Test
  public void testFlinkFileIOKryoSerialization() throws IOException {
    FileIO testFlinkFileIO = new FlinkFileIO();

    // Flink fileIO should be serializable when properties are passed as immutable map
    testFlinkFileIO.initialize(ImmutableMap.of("k1", "v1"));
    FileIO roundTripSerializedFileIO = TestHelpers.KryoHelpers.roundTripSerialize(testFlinkFileIO);

    Assertions.assertThat(roundTripSerializedFileIO.properties())
        .isEqualTo(testFlinkFileIO.properties());
  }

  @Test
  public void testFlinkFileIOJavaSerialization() throws IOException, ClassNotFoundException {
    FileIO testFlinkFileIO = new FlinkFileIO();

    // Flink fileIO should be serializable when properties are passed as immutable map
    testFlinkFileIO.initialize(ImmutableMap.of("k1", "v1"));
    FileIO roundTripSerializedFileIO = TestHelpers.roundTripSerialize(testFlinkFileIO);

    Assertions.assertThat(roundTripSerializedFileIO.properties())
        .isEqualTo(testFlinkFileIO.properties());
  }

  private List<Path> createRandomFiles(Path parent, int count) {
    Vector<Path> paths = new Vector<>();
    random
        .ints(count)
        .parallel()
        .forEach(
            i -> {
              try {
                Path path = new Path(parent, "file-" + i);
                paths.add(path);
                fs.create(path, FileSystem.WriteMode.OVERWRITE);
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            });
    return paths;
  }
}
