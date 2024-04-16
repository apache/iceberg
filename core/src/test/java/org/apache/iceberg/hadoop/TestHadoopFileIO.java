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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.Vector;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileIOParser;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestHadoopFileIO {
  private final Random random = new Random(1);

  private FileSystem fs;
  private HadoopFileIO hadoopFileIO;

  @TempDir private File tempDir;

  @BeforeEach
  public void before() throws Exception {
    Configuration conf = new Configuration();
    fs = FileSystem.getLocal(conf);

    hadoopFileIO = new HadoopFileIO(conf);
    HadoopFileIO.HADOOP_BULK_DELETE.set(true);
  }

  @Test
  public void testListPrefix() {
    Path parent = new Path(tempDir.toURI());

    List<Integer> scaleSizes = Lists.newArrayList(1, 1000, 2500);

    scaleSizes.parallelStream()
        .forEach(
            scale -> {
              Path scalePath = new Path(parent, Integer.toString(scale));

              createRandomFiles(scalePath, scale);
              assertThat(
                      Streams.stream(hadoopFileIO.listPrefix(scalePath.toUri().toString())).count())
                  .isEqualTo((long) scale);
            });

    long totalFiles = scaleSizes.stream().mapToLong(Integer::longValue).sum();
    final String parentString = parent.toUri().toString();
    final List<FileInfo> files =
        Streams.stream(hadoopFileIO.listPrefix(parentString)).collect(Collectors.toList());
    assertThat(files.size())
        .describedAs("Files found under %s", parentString)
        .isEqualTo(totalFiles);

    // having gone to the effort of creating a few thousand files, delete them
    // this stresses the bulk delete memory/execution path and if profiled,
    // can highlight performance mismatches across the implementations.
    final Iterator<String> locations = files.stream().map(FileInfo::location).iterator();
    hadoopFileIO.deleteFiles(() -> locations);

    // now there are no files, but the parent directory still exists as it was not
    // deleted.
    assertThat(Streams.stream(hadoopFileIO.listPrefix(parentString)).count())
        .describedAs("Files found under %s after bulk delete", parentString)
        .isEqualTo(0);
  }

  @Test
  public void testFileExists() throws IOException {
    Path parent = new Path(tempDir.toURI());
    Path randomFilePath = file(new Path(parent, "random-file-" + UUID.randomUUID()));

    // check existence of the created file
    assertPathExists(randomFilePath);
    fs.delete(randomFilePath, false);
    assertPathDoesNotExist(randomFilePath);
  }

  @Test
  public void testDeletePrefix() {
    Path parent = new Path(tempDir.toURI());

    List<Integer> scaleSizes = Lists.newArrayList(1, 1000, 2500);

    scaleSizes.parallelStream()
        .forEach(
            scale -> {
              Path scalePath = new Path(parent, Integer.toString(scale));

              createRandomFiles(scalePath, scale);
              hadoopFileIO.deletePrefix(scalePath.toUri().toString());

              // Hadoop filesystem will throw if the path does not exist
              assertThatThrownBy(
                      () -> hadoopFileIO.listPrefix(scalePath.toUri().toString()).iterator())
                  .isInstanceOf(UncheckedIOException.class)
                  .hasMessageContaining("java.io.FileNotFoundException");
            });

    hadoopFileIO.deletePrefix(parent.toUri().toString());
    // Hadoop filesystem will throw if the path does not exist
    assertThatThrownBy(() -> hadoopFileIO.listPrefix(parent.toUri().toString()).iterator())
        .isInstanceOf(UncheckedIOException.class)
        .hasMessageContaining("java.io.FileNotFoundException");
  }

  @Test
  public void testDeleteFiles() {
    Path parent = new Path(tempDir.toURI());
    List<Path> filesCreated = createRandomFiles(parent, 10);
    hadoopFileIO.deleteFiles(
        filesCreated.stream().map(Path::toString).collect(Collectors.toList()));
    filesCreated.forEach(this::assertPathDoesNotExist);
  }

  @Test
  public void testDeleteFilesErrorHandling() {
    Path parent = new Path(tempDir.toURI());
    // two files whose schema doesn't resolve
    List<String> filesCreated =
        random.ints(2).mapToObj(x -> "fakefsnotreal://file-" + x).collect(Collectors.toList());
    // one file in the local FS which doesn't actually exist but whose schema is valid
    // this MUST NOT be recorded as a failure
    filesCreated.add(new Path(parent, "file-not-exist").toUri().toString());
    // and a valid file which must be deleted
    Path path = file(new Path(parent, "random-file-" + UUID.randomUUID()));
    filesCreated.add(path.toUri().toString());

    // delete all of them and expect a failure
    assertThatThrownBy(() -> hadoopFileIO.deleteFiles(filesCreated))
        .describedAs("Exception raised by deleteFiles()")
        .isInstanceOf(BulkDeletionFailureException.class)
        .hasMessage("Failed to delete 2 files")
        .matches(
            (e) -> ((BulkDeletionFailureException) e).numberFailedObjects() == 2,
            "Wrong number of failures");

    // the file actually created is gone
    assertPathDoesNotExist(path);
  }

  @Test
  public void testBulkDeleteEmptyList() {
    // Deleting an empty list is harmless
    hadoopFileIO.deleteFiles(new ArrayList<>());

    // disable bulk delete and repeat
    HadoopFileIO.HADOOP_BULK_DELETE.set(false);
    hadoopFileIO.deleteFiles(new ArrayList<>());
  }

  @Test
  public void testBulkDeleteEmptyDirectory() throws Throwable {
    verifyBulkDeleteEmptyDirectorySucceeds();
  }

  @Test
  public void testBulkDeleteEmptyDirectoryClassic() throws Throwable {
    // disable bulk delete and repeat
    HadoopFileIO.HADOOP_BULK_DELETE.set(false);
    verifyBulkDeleteEmptyDirectorySucceeds();
  }

  private void verifyBulkDeleteEmptyDirectorySucceeds() throws IOException {
    // Empty directories are deleted.
    Path parent = new Path(tempDir.toURI());
    final Path dir = new Path(parent, "dir-" + UUID.randomUUID());
    fs.mkdirs(dir);
    final Path file = file(new Path(parent, "file-" + UUID.randomUUID()));
    List<String> paths = Lists.newArrayList(dir.toUri().toString(), file.toUri().toString());
    hadoopFileIO.deleteFiles(paths);
    assertPathDoesNotExist(file);
    assertPathDoesNotExist(dir);
  }

  @Test
  public void testBulkDeleteNonEmptyDirectory() throws Throwable {
    verifyBulkDeleteNonEmptyDirectoryFails();
  }

  @Test
  public void testBulkDeleteNonEmptyDirectoryClassic() throws Throwable {
    HadoopFileIO.HADOOP_BULK_DELETE.set(false);
    verifyBulkDeleteNonEmptyDirectoryFails();
  }

  private void verifyBulkDeleteNonEmptyDirectoryFails() throws Throwable {
    // non-empty directories cannot be deleted; other paths in the bulk request
    // are still deleted
    Path parent = new Path(tempDir.toURI());
    final Path dir = new Path(parent, "dir-" + UUID.randomUUID());
    final Path file = file(new Path(parent, "file-" + UUID.randomUUID()));
    final Path child = file(new Path(dir, "file-" + UUID.randomUUID()));
    List<String> paths = Lists.newArrayList(dir.toUri().toString(), file.toUri().toString());
    assertThatThrownBy(() -> hadoopFileIO.deleteFiles(paths))
        .describedAs("Exception raised by deleteFiles()")
        .isInstanceOf(BulkDeletionFailureException.class)
        .hasMessageContaining("Failed")
        .matches(
            e -> ((BulkDeletionFailureException) e).numberFailedObjects() == 1,
            "Wrong number of failures");
    assertPathDoesNotExist(file);
    assertPathExists(child);
  }

  @Test
  public void testDeleteEmptyDirectory() throws Throwable {
    // Empty directories are deleted.
    Path parent = new Path(tempDir.toURI());
    final Path dir = new Path(parent, "dir-" + UUID.randomUUID());
    fs.mkdirs(dir);
    hadoopFileIO.deleteFile(dir.toUri().toString());
    assertPathDoesNotExist(dir);
  }

  @Test
  public void testDeleteNonEmptyDirectory() throws Throwable {
    // directories with children must not be deleted.
    Path parent = new Path(tempDir.toURI());
    final Path dir = new Path(parent, "dir-" + UUID.randomUUID());
    final Path file = file(new Path(dir, "file"));
    assertThatThrownBy(() -> hadoopFileIO.deleteFile(dir.toUri().toString()))
        .describedAs("Exception raised by deleteFile(%s)", dir)
        .isInstanceOf(RuntimeIOException.class)
        .hasMessageContaining("Failed to delete file:");
    assertPathExists(file);
  }

  @ParameterizedTest
  @MethodSource("org.apache.iceberg.TestHelpers#serializers")
  public void testHadoopFileIOSerialization(
      TestHelpers.RoundTripSerializer<FileIO> roundTripSerializer)
      throws IOException, ClassNotFoundException {

    FileIO testHadoopFileIO = new HadoopFileIO();

    // hadoop fileIO should be serializable when properties are passed as immutable map
    testHadoopFileIO.initialize(ImmutableMap.of("k1", "v1", "k2", "v2"));
    FileIO roundTripSerializedFileIO = roundTripSerializer.apply(testHadoopFileIO);

    assertThat(roundTripSerializedFileIO.properties()).isEqualTo(testHadoopFileIO.properties());
  }

  @Test
  public void testResolvingFileIOLoad() {
    ResolvingFileIO resolvingFileIO = new ResolvingFileIO();
    resolvingFileIO.setConf(new Configuration());
    resolvingFileIO.initialize(ImmutableMap.of());
    FileIO result =
        DynMethods.builder("io")
            .hiddenImpl(ResolvingFileIO.class, String.class)
            .build(resolvingFileIO)
            .invoke("hdfs://foo/bar");
    assertThat(result).isInstanceOf(HadoopFileIO.class);
  }

  @Test
  public void testJsonParserWithoutHadoopConf() throws Exception {
    this.hadoopFileIO = new HadoopFileIO();

    hadoopFileIO.initialize(ImmutableMap.of("properties-bar", "2"));
    assertThat(hadoopFileIO.properties().get("properties-bar")).isEqualTo("2");

    testJsonParser(hadoopFileIO, tempDir);
  }

  @Test
  public void testJsonParserWithHadoopConf() throws Exception {
    this.hadoopFileIO = new HadoopFileIO();

    Configuration hadoopConf = new Configuration();
    hadoopConf.setInt("hadoop-conf-foo", 1);
    hadoopFileIO.setConf(hadoopConf);
    assertThat(hadoopFileIO.conf().get("hadoop-conf-foo")).isNotNull();

    hadoopFileIO.initialize(ImmutableMap.of("properties-bar", "2"));
    assertThat(hadoopFileIO.properties().get("properties-bar")).isEqualTo("2");

    testJsonParser(hadoopFileIO, tempDir);
  }

  private static void testJsonParser(HadoopFileIO hadoopFileIO, File tempDir) throws Exception {
    String json = FileIOParser.toJson(hadoopFileIO);
    try (FileIO deserialized = FileIOParser.fromJson(json)) {
      assertThat(deserialized).isInstanceOf(HadoopFileIO.class);
      HadoopFileIO deserializedHadoopFileIO = (HadoopFileIO) deserialized;

      // properties are carried over during serialization and deserialization
      assertThat(deserializedHadoopFileIO.properties()).isEqualTo(hadoopFileIO.properties());

      // FileIOParser doesn't serialize and deserialize Hadoop configuration
      // so config "foo" is not restored in deserialized object.
      assertThat(deserializedHadoopFileIO.conf().get("hadoop-conf-foo")).isNull();

      // make sure deserialized io can create input file
      Files.createTempDirectory(tempDir.toPath(), "junit");
      deserializedHadoopFileIO.newInputFile(
          File.createTempFile("test", "parquet", tempDir).toString());
    }
  }

  /**
   * Create a zero byte file at a path, overwriting any file which is there.
   *
   * @param path path
   * @return the path of the file
   */
  Path file(Path path) {
    try {
      hadoopFileIO.newOutputFile(path.toString()).createOrOverwrite().close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return path;
  }

  private List<Path> createRandomFiles(Path parent, int count) {
    Vector<Path> paths = new Vector<>();
    random.ints(count).parallel().forEach(i -> paths.add(file(new Path(parent, "file-" + i))));
    return paths;
  }

  private void assertPathDoesNotExist(Path path) {
    assertThat(hadoopFileIO.newInputFile(path.toUri().toString()).exists())
        .describedAs("File %s must not exist", path)
        .isFalse();
  }

  private void assertPathExists(Path path) {
    assertThat(hadoopFileIO.newInputFile(path.toUri().toString()).exists())
        .describedAs("File %s must exist", path)
        .isTrue();
  }
}
