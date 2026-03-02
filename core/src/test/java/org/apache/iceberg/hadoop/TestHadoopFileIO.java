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

import static org.apache.iceberg.hadoop.BulkDeleter.BULK_DELETE_CLASS;
import static org.apache.iceberg.hadoop.HadoopFileIO.BULK_DELETE_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BulkDelete;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileIOParser;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
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

  @TempDir private File tempDir;

  @BeforeEach
  public void before() throws Exception {
    resetFileIOBinding(false);
  }

  /**
   * Resets fs and hadoopFileIO fields to a configuration built from the supplied settings. The two
   * settings are not orthogonal; if bulk delete is enabled then deleteFiles() hands off to the FS
   * and its bulk delete operation, while single file delete may still go through trash.
   *
   * @param bulkDelete use bulk delete
   * @throws UncheckedIOException on failures to create a new FS.
   */
  private void resetFileIOBinding(boolean bulkDelete) {
    Configuration conf = new Configuration();
    conf.setBoolean(BULK_DELETE_ENABLED, bulkDelete);
    try {
      FileSystem.closeAllForUGI(UserGroupInformation.getCurrentUser());
      fs = FileSystem.getLocal(conf);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    hadoopFileIO = new HadoopFileIO(fs.getConf());
  }

  @Test
  public void testListPrefixAndDeleteFiles() {
    resetFileIOBinding(true);
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
    final List<FileInfo> files = Streams.stream(hadoopFileIO.listPrefix(parentString)).toList();
    assertThat(files.size())
        .describedAs("Files found under %s", parentString)
        .isEqualTo(totalFiles);

    // Delete the files.
    final Iterator<String> locations = files.stream().map(FileInfo::location).iterator();
    hadoopFileIO.deleteFiles(() -> locations);
  }

  @Test
  public void testFileExists() throws IOException {
    Path parent = new Path(tempDir.toURI());
    Path randomFilePath = new Path(parent, "random-file-" + UUID.randomUUID());
    fs.createNewFile(randomFilePath);

    // check existence of the created file
    final String path = randomFilePath.toUri().toString();
    assertThat(hadoopFileIO.newInputFile(path).exists()).isTrue();
    fs.delete(randomFilePath, false);
    assertThat(hadoopFileIO.newInputFile(path).exists()).isFalse();
    assertThatThrownBy(() -> hadoopFileIO.newInputFile(path).getLength())
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining(path);
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

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testDeleteFiles(boolean bulkDelete) {
    resetFileIOBinding(bulkDelete);
    Path parent = new Path(tempDir.toURI());
    List<Path> filesCreated = createRandomFiles(parent, 10);
    hadoopFileIO.deleteFiles(filesCreated.stream().map(Path::toString).toList());
    filesCreated.forEach(
        file -> assertThat(hadoopFileIO.newInputFile(file.toString()).exists()).isFalse());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testDeleteFilesErrorHandling(boolean bulkDelete) {
    resetFileIOBinding(bulkDelete);
    hadoopFileIO = new HadoopFileIO(fs.getConf());
    assertThat(hadoopFileIO.useBulkDeleteApi())
        .describedAs("Bulk Delete API use")
        .isEqualTo(bulkDelete);
    Path parent = new Path(tempDir.toURI());

    List<String> filesToDelete =
        random.ints(2).mapToObj(x -> "fakefsnotreal://file-" + x).collect(Collectors.toList());
    // one file in the local FS which doesn't actually exist but whose scheme is valid
    // this MUST NOT be recorded as a failure
    final String localButMissing = new Path(parent, "file-not-exist").toUri().toString();
    filesToDelete.add(localButMissing);
    final String exists = touch(new Path(parent, "exists")).toString();
    filesToDelete.add(exists);
    assertThatThrownBy(() -> hadoopFileIO.deleteFiles(filesToDelete))
        .describedAs("Exception raised by deleteFiles()")
        .isInstanceOf(BulkDeletionFailureException.class)
        .hasMessage("Failed to delete 2 files")
        .matches(
            (e) -> ((BulkDeletionFailureException) e).numberFailedObjects() == 2,
            "Wrong number of failures");
    assertPathDoesNotExist(localButMissing);
    assertPathDoesNotExist(exists);
  }

  /**
   * Force the API resource to probe for to a missing class, then expect a delete with bulk delete
   * enabled to fail.
   */
  @Test
  public void testDeleteFailureuWhenClassNotFound() {
    try {
      resetFileIOBinding(true);
      BulkDeleter.setApiResource("no/such/class");
      assertThatThrownBy(() -> hadoopFileIO.deleteFiles(List.of(tempDir.toURI().toString())))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining(BULK_DELETE_ENABLED);
    } finally {
      // change the resource to probe for to be that of the BulkDelete class.
      BulkDeleter.setApiResource(BULK_DELETE_CLASS);
    }
  }

  /**
   * Local FS will let you delete an empty dir on a non-recursive delete, but not one with a child.
   * Create a dir with a child, expect a failure, but also expect the file path provided to have
   * been deleted.
   */
  @Test
  public void testBulkDeleteNonEmptyDirectory() {
    resetFileIOBinding(true);
    Path parent = new Path(tempDir.toURI());
    final String file = touch(new Path(parent, "file")).toString();
    final File directory = new File(tempDir, "dir");
    directory.mkdirs();
    touch(new Path(parent, "dir/child"));

    final String dirString = directory.toURI().toString();
    assertPathExists(dirString);
    // directory delete resulted in the whole operation reported as a failure.
    assertThatThrownBy(() -> hadoopFileIO.deleteFiles(List.of(dirString, file)))
        .isInstanceOf(BulkDeletionFailureException.class)
        .hasMessageContaining("Failed to delete 1 files");

    // directory is still there
    assertPathExists(dirString);
    // the file has been deleted
    assertPathDoesNotExist(file);
  }

  /**
   * With bulk delete disabled, deleteFiles() works. This shows the classic invocation path is used.
   */
  @Test
  public void testDeleteFilesWithBulkDeleteNotFound() {
    try {
      Path parent = new Path(tempDir.toURI());
      BulkDeleter.setApiResource("no/such/class");
      assertThat(BulkDeleter.apiAvailable()).isFalse();

      List<Path> filesCreated = createRandomFiles(parent, 5);
      hadoopFileIO.deleteFiles(filesCreated.stream().map(Path::toString).toList());
      filesCreated.forEach(
          file -> assertThat(hadoopFileIO.newInputFile(file.toString()).exists()).isFalse());
    } finally {
      // change the resource to probe for to be that of the BulkDelete class.
      BulkDeleter.setApiResource(BULK_DELETE_CLASS);
    }
  }

  /**
   * A stub BulkDelete implementation allows for a page size greater than 1 to be tested, through
   * the DeleteContext class along with reporting of partial failures.
   */
  @Test
  public void testDeleteContext() {

    // stub delete with page size of two, path #3 will be rejected on a delete
    final StubBulkDelete stubBulkDelete = new StubBulkDelete(2, 3);
    final BulkDeleter.DeleteContext context = new BulkDeleter.DeleteContext(stubBulkDelete);
    assertThat(context.pageSize()).isEqualTo(2);
    assertThat(context.size()).isEqualTo(0);
    assertThat(context.pageIsComplete()).isFalse();
    // add one path, assert state changes
    final Path p1 = new Path("/p1");
    context.add(p1);
    assertThat(context.size()).isEqualTo(1);
    assertThat(context.pageIsComplete()).isFalse();
    // add a second path, the page is now complete
    final Path p2 = new Path("/p2");
    context.add(p2);
    assertThat(context.pageIsComplete()).isTrue();
    // take a snapshot, it has the expected size and the context is reset
    final Set<Path> snapshot = context.snapshotDeletedFiles();
    assertThat(snapshot).hasSize(2);
    assertThat(context.size()).isEqualTo(0);
    assertThat(context.pageIsComplete()).isFalse();
    // perform the delete
    assertThat(context.deleteBatch(snapshot)).isEmpty();
    final Path p3 = new Path("/p3");
    context.add(p3);
    final Path p4 = new Path("/p4");
    context.add(p4);
    assertThat(context.pageIsComplete()).isTrue();
    // delete all without a snapshot (as is done at the end of deleteFiles());
    // this is going report a failure.
    assertThat(context.deleteBatch(context.deletedFiles()))
        .hasSize(1)
        .allSatisfy(
            r -> {
              assertThat(r.getKey()).isEqualTo(p3);
              assertThat(r.getValue()).isEqualTo("simulated failure");
            });
    // still deleted everything
    assertThat(stubBulkDelete.deleteCount).isEqualTo(4);
    assertThat(stubBulkDelete.deletedFiles).containsExactlyInAnyOrder(p1, p2, p4);
    context.close();
  }

  /** Stub implementation of BulkDelete to verify invocation and failure handling. */
  private static final class StubBulkDelete implements BulkDelete {
    private final int pageSize;
    private final int failOnDeleteCount;
    private final Set<Path> deletedFiles = Sets.newHashSet();
    private int deleteCount;

    StubBulkDelete(int pageSize, int failOnDeleteCount) {
      this.pageSize = pageSize;
      this.failOnDeleteCount = failOnDeleteCount;
    }

    @Override
    public int pageSize() {
      return pageSize;
    }

    @Override
    public Path basePath() {
      return new Path("/");
    }

    @Override
    public List<Map.Entry<Path, String>> bulkDelete(Collection<Path> paths)
        throws IOException, IllegalArgumentException {
      List<Map.Entry<Path, String>> entries = Lists.newArrayList();
      for (Path path : paths) {
        deleteCount++;
        if (deleteCount == failOnDeleteCount) {
          entries.add(new AbstractMap.SimpleEntry<>(path, "simulated failure"));
        } else {
          deletedFiles.add(path);
        }
      }
      return entries;
    }

    @Override
    public void close() throws IOException {}
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
          new File(tempDir, "test" + System.nanoTime() + ".parquet").toString());
    }
  }

  private List<Path> createRandomFiles(Path parent, int count) {
    Vector<Path> paths = new Vector<>();
    random.ints(count).parallel().forEach(i -> paths.add(touch(new Path(parent, "file-" + i))));
    return paths;
  }

  /**
   * Create a file at a path, overwriting any existing file.
   *
   * @param path path of file.
   */
  private Path touch(Path path) {
    try {
      fs.create(path, true).close();
      return path;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
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
}
