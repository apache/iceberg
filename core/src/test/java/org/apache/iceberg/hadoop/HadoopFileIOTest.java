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
package org.apache.iceberg.hadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class HadoopFileIOTest {
  private final Random random = new Random(1);

  private FileSystem fs;
  private HadoopFileIO hadoopFileIO;

  @TempDir static File tempDir;

  @BeforeEach
  public void before() throws Exception {
    Configuration conf = new Configuration();
    fs = FileSystem.getLocal(conf);

    hadoopFileIO = new HadoopFileIO(conf);
  }

  @Test
  public void testListPrefix() {
    Path parent = new Path(tempDir.toURI());

    List<Integer> scaleSizes = Lists.newArrayList(1, 5, 500);

    scaleSizes
        .parallelStream()
        .forEach(
            scale -> {
              Path scalePath = new Path(parent, Integer.toString(scale));

              createRandomFiles(scalePath, scale);
              assertEquals(
                  (long) scale,
                  Streams.stream(hadoopFileIO.listPrefix(scalePath.toUri().toString())).count());
            });

    long totalFiles = scaleSizes.stream().mapToLong(Integer::longValue).sum();
    assertEquals(
        totalFiles, Streams.stream(hadoopFileIO.listPrefix(parent.toUri().toString())).count());
  }

  @Test
  public void testFileExists() throws IOException {
    Path parent = new Path(tempDir.toURI());
    Path randomFilePath = new Path(parent, "random-file-" + UUID.randomUUID().toString());
    fs.createNewFile(randomFilePath);

    // check existence of the created file
    Assert.assertTrue(hadoopFileIO.newInputFile(randomFilePath.toUri().toString()).exists());

    fs.delete(randomFilePath, false);
    Assert.assertFalse(hadoopFileIO.newInputFile(randomFilePath.toUri().toString()).exists());
  }

  @Test
  public void testDeletePrefix() {
    Path parent = new Path(tempDir.toURI());

    List<Integer> scaleSizes = Lists.newArrayList(1, 5, 500);

    scaleSizes
        .parallelStream()
        .forEach(
            scale -> {
              Path scalePath = new Path(parent, Integer.toString(scale));

              createRandomFiles(scalePath, scale);
              hadoopFileIO.deletePrefix(scalePath.toUri().toString());

              // Hadoop filesystem will throw if the path does not exist
              assertThrows(
                  UncheckedIOException.class,
                  () -> hadoopFileIO.listPrefix(scalePath.toUri().toString()).iterator());
            });

    hadoopFileIO.deletePrefix(parent.toUri().toString());
    // Hadoop filesystem will throw if the path does not exist
    assertThrows(
        UncheckedIOException.class,
        () -> hadoopFileIO.listPrefix(parent.toUri().toString()).iterator());
  }

  @Test
  public void testDeleteFiles() {
    Path parent = new Path(tempDir.toURI());
    List<Path> filesCreated = createRandomFiles(parent, 10);
    hadoopFileIO.deleteFiles(
        filesCreated.stream().map(Path::toString).collect(Collectors.toList()));
    filesCreated.forEach(
        file -> Assert.assertFalse(hadoopFileIO.newInputFile(file.toString()).exists()));
  }

  @Test
  public void testDeleteFilesErrorHandling() {
    List<String> filesCreated =
        random.ints(2).mapToObj(x -> "fakefsnotreal://file-" + x).collect(Collectors.toList());
    Assert.assertThrows(
        "Should throw a BulkDeletionFailure Exceptions when files can't be deleted",
        BulkDeletionFailureException.class,
        () -> hadoopFileIO.deleteFiles(filesCreated));
  }

  @Test
  public void testHadoopFileIOKryoSerialization() throws IOException {
    FileIO testHadoopFileIO = new HadoopFileIO();

    // hadoop fileIO should be serializable when properties are passed as immutable map
    testHadoopFileIO.initialize(ImmutableMap.of("k1", "v1"));
    FileIO roundTripSerializedFileIO = TestHelpers.KryoHelpers.roundTripSerialize(testHadoopFileIO);

    Assert.assertEquals(testHadoopFileIO.properties(), roundTripSerializedFileIO.properties());
  }

  @Test
  public void testHadoopFileIOJavaSerialization() throws IOException, ClassNotFoundException {
    FileIO testHadoopFileIO = new HadoopFileIO();

    // hadoop fileIO should be serializable when properties are passed as immutable map
    testHadoopFileIO.initialize(ImmutableMap.of("k1", "v1"));
    FileIO roundTripSerializedFileIO = TestHelpers.roundTripSerialize(testHadoopFileIO);

    Assert.assertEquals(testHadoopFileIO.properties(), roundTripSerializedFileIO.properties());
  }

  private List<Path> createRandomFiles(Path parent, int count) {
    List<Path> paths = Lists.newArrayList();
    random
        .ints(count)
        .parallel()
        .forEach(
            i -> {
              try {
                Path path = new Path(parent, "file-" + i);
                paths.add(path);
                fs.createNewFile(path);
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            });
    return paths;
  }
}
