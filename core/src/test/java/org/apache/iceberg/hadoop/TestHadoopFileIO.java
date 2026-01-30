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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.iceberg.hadoop.HadoopFileIO.DELETE_TRASH_SCHEMAS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.TrashPolicy;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileIOParser;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class TestHadoopFileIO {
  private final Random random = new Random(1);

  private FileSystem fs;
  private HadoopFileIO hadoopFileIO;
  private boolean trashEnabled;

  @TempDir private File tempDir;

  @BeforeEach
  public void before() throws Exception {
    resetBinding(false);
  }

  /**
   * Purge trash as a cleanup operation if the test case created a FS with trash enabled; avoids
   * accrual of many empty files in this path. Note: this affects the entire user account.
   */
  @AfterEach
  public void purgeTrash() throws IOException {
    if (trashEnabled) {
      fs.delete(fs.getTrashRoot(new Path(tempDir.toURI())), true);
    }
  }

  /**
   * Resets fs and hadoopFileIO fields to a configuration built from the supplied settings.
   *
   * @param useTrash enable trash settings
   * @throws UncheckedIOException on failures to create a new FS.
   */
  private void resetBinding(boolean useTrash) {
    Configuration conf = new Configuration();
    trashEnabled = useTrash;
    if (useTrash) {
      conf.set(FS_TRASH_INTERVAL_KEY, "60");
      conf.set(DELETE_TRASH_SCHEMAS, " file , hdfs, viewfs");
    }
    try {
      FileSystem.closeAllForUGI(UserGroupInformation.getCurrentUser());
      fs = FileSystem.getLocal(conf);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    hadoopFileIO = new HadoopFileIO(fs.getConf());
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
    assertThat(Streams.stream(hadoopFileIO.listPrefix(parent.toUri().toString())).count())
        .isEqualTo(totalFiles);
  }

  @Test
  public void testFileExists() throws IOException {
    Path parent = new Path(tempDir.toURI());
    Path randomFilePath = new Path(parent, "random-file-" + UUID.randomUUID());
    fs.createNewFile(randomFilePath);

    // check existence of the created file
    assertThat(hadoopFileIO.newInputFile(randomFilePath.toUri().toString()).exists()).isTrue();
    fs.delete(randomFilePath, false);
    assertThat(hadoopFileIO.newInputFile(randomFilePath.toUri().toString()).exists()).isFalse();
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
  public void testDeletePrefixWithTrashEnabled() {
    resetBinding(true);
    Path parent = new Path(tempDir.toURI());

    List<Integer> scaleSizes = Lists.newArrayList(1, 1000, 2500);

    scaleSizes.parallelStream()
        .forEach(
            scale -> {
              Path scalePath = new Path(parent, Integer.toString(scale));

              List<Path> filesCreated = createRandomFiles(scalePath, scale);
              hadoopFileIO.deletePrefix(scalePath.toUri().toString());

              // Hadoop filesystem will throw if the path does not exist
              assertThatThrownBy(
                      () -> hadoopFileIO.listPrefix(scalePath.toUri().toString()).iterator())
                  .isInstanceOf(UncheckedIOException.class)
                  .hasMessageContaining("java.io.FileNotFoundException")
                  .cause()
                  .isInstanceOf(FileNotFoundException.class);
              filesCreated.forEach(
                  file -> {
                    String fileSuffix = Path.getPathWithoutSchemeAndAuthority(file).toString();
                    String trashPath =
                        fs.getTrashRoot(scalePath).toString() + "/Current" + fileSuffix;
                    assertPathExists(trashPath);
                    // delete that path. As it is in trash, it gets deleted.
                    hadoopFileIO.deleteFile(trashPath);
                  });
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
    filesCreated.forEach(file -> assertPathDoesNotExist(file.toString()));
  }

  @Test
  public void testDefaultTrashSchemas() {
    // hdfs is a default trash schema; viewfs is, file isn't
    assertThat(hadoopFileIO.isTrashSchema(new Path("hdfs:///")))
        .describedAs("hdfs schema")
        .isTrue();
    assertThat(hadoopFileIO.isTrashSchema(new Path("viewfs:///")))
        .describedAs("viewfs schema")
        .isTrue();
    assertThat(hadoopFileIO.isTrashSchema(new Path("file:///")))
        .describedAs("file schema")
        .isFalse();
  }

  @Test
  public void testRemoveTrashSchemas() {
    // set the schema list to "" and verify that the default values are gone
    final Configuration conf = new Configuration(false);
    conf.set(DELETE_TRASH_SCHEMAS, "");
    hadoopFileIO = new HadoopFileIO(conf);
    assertThat(hadoopFileIO.isTrashSchema(new Path("hdfs:///"))).isFalse();
  }

  @Test
  public void testDeleteFilesWithTrashEnabled() {
    resetBinding(true);
    hadoopFileIO = new HadoopFileIO(fs.getConf());
    assertThat(hadoopFileIO.isTrashSchema(new Path("file:///"))).isTrue();

    Path parent = new Path(tempDir.toURI());
    List<Path> filesCreated = createRandomFiles(parent, 10);
    hadoopFileIO.deleteFiles(
        filesCreated.stream().map(Path::toString).collect(Collectors.toList()));
    filesCreated.forEach(
        file -> assertThat(hadoopFileIO.newInputFile(file.toString()).exists()).isFalse());
    filesCreated.forEach(
        file -> {
          String fileSuffix = Path.getPathWithoutSchemeAndAuthority(file).toString();
          String trashPath = fs.getTrashRoot(parent).toString() + "/Current" + fileSuffix;
          assertPathExists(trashPath);
          // delete that path. As it is in trash, it gets deleted.
          hadoopFileIO.deleteFile(trashPath);
        });
  }

  /**
   * Use a trash policy which raises an exception when moving a file to trash; verify that
   * deleteFile() falls back to delete. Various counters are checked simply to verify that the
   * failing trash policy was invoked.
   */
  @Test
  public void testTrashFailureFallBack() throws Exception {
    resetBinding(true);
    // the filesystem config needs to be modified to use the test trash policy.
    final Configuration conf = fs.getConf();
    conf.set("fs.trash.classname", TestTrashPolicy.class.getName());
    // check loading works.
    final long instances = TestTrashPolicy.INSTANCES.get();
    final long exceptions = TestTrashPolicy.EXCEPTIONS.get();
    Trash trash = new Trash(conf);
    assertThat(trash.isEnabled()).isTrue();
    assertThat(TestTrashPolicy.INSTANCES.get()).isEqualTo(instances + 1);

    // now create the file IO with the same conf.
    hadoopFileIO = new HadoopFileIO(conf);
    assertThat(hadoopFileIO.isTrashSchema(new Path("file:///"))).isTrue();

    Path parent = new Path(tempDir.toURI());
    Path path = new Path(parent, "child");
    fs.createNewFile(path);
    final String p = path.toUri().toString();
    // this will delete the file, even with the simulated IOE on moveToTrash
    hadoopFileIO.deleteFile(p);
    assertPathDoesNotExist(p);
    assertThat(TestTrashPolicy.INSTANCES.get())
        .describedAs("TestTrashPolicy instantiations")
        .isEqualTo(instances + 2);
    assertThat(TestTrashPolicy.EXCEPTIONS.get())
        .describedAs("TestTrashPolicy exceptions")
        .isEqualTo(exceptions + 1);
  }

  /**
   * Verify semantics of a missing file delete are the same with and without trash: no reported
   * error.
   *
   * @param useTrash use trash in the FS.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testDeleteMissingFileToTrash(boolean useTrash) {
    resetBinding(useTrash);
    Path path = new Path(new Path(tempDir.toURI()), "missing");
    final String missing = path.toUri().toString();
    hadoopFileIO.deleteFile(missing);
    assertPathDoesNotExist(missing);
  }

  @Test
  public void testDeleteFilesErrorHandling() {
    List<String> filesCreated =
        random.ints(2).mapToObj(x -> "fakefsnotreal://file-" + x).collect(Collectors.toList());
    assertThatThrownBy(() -> hadoopFileIO.deleteFiles(filesCreated))
        .isInstanceOf(BulkDeletionFailureException.class)
        .hasMessage("Failed to delete 2 files");
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
    try (ResolvingFileIO resolvingFileIO = new ResolvingFileIO()) {
      resolvingFileIO.setConf(new Configuration());
      resolvingFileIO.initialize(ImmutableMap.of());
      FileIO result =
          DynMethods.builder("io")
              .hiddenImpl(ResolvingFileIO.class, String.class)
              .build(resolvingFileIO)
              .invoke("hdfs://foo/bar");
      assertThat(result).isInstanceOf(HadoopFileIO.class);
    }
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
          new File(tempDir, "test" + System.nanoTime() + ".parquet").toString());
    }
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
                fs.createNewFile(path);
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            });
    return paths;
  }

  /**
   * Assert a path exists.
   *
   * @param path URI to file/dir.
   */
  private void assertPathExists(String path) {
    assertThat(hadoopFileIO.newInputFile(path).exists())
        .describedAs("File %s must exist", path)
        .isTrue();
  }

  /**
   * Assert a path does not exist.
   *
   * @param path URI to file/dir.
   */
  private void assertPathDoesNotExist(String path) {
    assertThat(hadoopFileIO.newInputFile(path).exists())
        .describedAs("File %s must exist", path)
        .isFalse();
  }

  /**
   * Test TrashPolicy. Increments the counter {@link #INSTANCES} on every instantiation. On a call
   * to {@link #moveToTrash(Path)} it increments the counter {@link #EXCEPTIONS} and throws an
   * exception.
   */
  private static final class TestTrashPolicy extends TrashPolicy {
    private static final AtomicLong INSTANCES = new AtomicLong();
    private static final AtomicLong EXCEPTIONS = new AtomicLong();

    private TestTrashPolicy() {
      INSTANCES.incrementAndGet();
    }

    @Override
    public void initialize(Configuration conf, FileSystem filesystem, Path home) {}

    @Override
    public void initialize(Configuration conf, FileSystem filesystem) {}

    @Override
    public boolean isEnabled() {
      return true;
    }

    @Override
    public boolean moveToTrash(Path path) throws IOException {
      EXCEPTIONS.incrementAndGet();
      throw new IOException("Simulated failure");
    }

    @Override
    public void createCheckpoint() {}

    @Override
    public void deleteCheckpoint() {}

    @Override
    public void deleteCheckpointsImmediately() {}

    @Override
    public Path getCurrentTrashDir() {
      return null;
    }

    @Override
    public Path getCurrentTrashDir(Path path) {
      return null;
    }

    @Override
    public Runnable getEmptier() {
      return null;
    }
  }
}
