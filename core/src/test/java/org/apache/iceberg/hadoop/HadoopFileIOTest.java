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
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.Vector;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.hadoop.wrappedio.DynamicWrappedIO;
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
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test IO through {@link HadoopFileIO}.
 *
 * <p>Tests which call {@link HadoopFileIO#deleteFiles(Iterable)} are parameterized on the hadoop
 * bulk delete API option;
 */
@ExtendWith(ParameterizedTestExtension.class)
public class HadoopFileIOTest {

  private final Random random = new Random(1);

  /** Should the test try to use the bulk IO API. */
  @Parameter private boolean useBulkIOApi;

  @Parameters(name = "useBulkIOApi = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(false, true);
  }

  /** Is the bulk delete API available through reflection. */
  private boolean bulkDeleteAvailable;

  private FileSystem fs;
  private HadoopFileIO hadoopFileIO;

  @TempDir private File tempDir;

  @BeforeEach
  public void before() throws Exception {

    // try load the bulk delete API.
    // if it is not available and this test run is parameterized to use it
    // the tests will be skipped.
    DynamicWrappedIO wrappedIO = new DynamicWrappedIO(getClass().getClassLoader());
    bulkDeleteAvailable = wrappedIO.bulkDeleteAvailable();
    if (useBulkIOApi) {
      // skip this parameterization if the bulk delete API is not available.
      assumeHadoopBulkDeleteAvailable();
    }
    // create a local FS configured to use bulk delete based on the
    // test run parameterization.
    Configuration conf = new Configuration();
    conf.setBoolean(HadoopFileIO.BULK_DELETE_ENABLED, useBulkIOApi);

    fs = FileSystem.getLocal(conf);

    hadoopFileIO = new HadoopFileIO(conf);
  }

  /**
   * Assume that the Hadoop bulk delete API is available. This is done by trying to dyamically load
   * the {@code WrappedIO} class.
   */
  private void assumeHadoopBulkDeleteAvailable() {
    assumeThat(bulkDeleteAvailable)
        .describedAs("Bulk Delete methods available")
        .isTrue();
  }

  /**
   * Create many files and verify that listing all files under the parent path returns the expected
   * count.
   *
   * <p>After creation, bulk deletion is invoked.
   *
   * <p>This can be used to compare the performance of single delete versus bulk delete API use. For
   * the local filesystem with a page size of 1, the times should be similar.
   */
  @TestTemplate
  public void testListPrefixAndDeleteFiles() throws IOException {
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
    Path randomFilePath = new Path(parent, "random-file-" + UUID.randomUUID());
    fs.create(randomFilePath, true).close();

    // check existence of the created file
    assertThat(hadoopFileIO.newInputFile(randomFilePath.toUri().toString()).exists()).isTrue();
    fs.delete(randomFilePath, false);
    assertThat(hadoopFileIO.newInputFile(randomFilePath.toUri().toString()).exists()).isFalse();
    assertThatThrownBy(
            () -> hadoopFileIO.newInputFile(randomFilePath.toUri().toString()).getLength())
        .isInstanceOf(NotFoundException.class);
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

              // Hadoop filesystem will throw a wrapped FileNotFoundException if the
              // path does not exist
              assertThatThrownBy(
                      () -> hadoopFileIO.listPrefix(scalePath.toUri().toString()).iterator())
                  .isInstanceOf(UncheckedIOException.class)
                  .hasCauseInstanceOf(FileNotFoundException.class);
            });

    hadoopFileIO.deletePrefix(parent.toUri().toString());
    // Hadoop filesystem will throw if the path does not exist
    assertThatThrownBy(() -> hadoopFileIO.listPrefix(parent.toUri().toString()).iterator())
        .isInstanceOf(UncheckedIOException.class)
        .hasCauseInstanceOf(FileNotFoundException.class);
  }

  @TestTemplate
  public void testDeleteFiles() {
    Path parent = new Path(tempDir.toURI());
    List<Path> filesCreated = createRandomFiles(parent, 10);
    hadoopFileIO.deleteFiles(
        filesCreated.stream().map(Path::toString).collect(Collectors.toList()));
    filesCreated.forEach(
        file ->
            assertThatThrownBy(() -> hadoopFileIO.newInputFile(file.toString()).getLength())
                .isInstanceOf(NotFoundException.class));
  }

  @TestTemplate
  public void testDeleteFilesErrorHandling() {
    Path parent = new Path(tempDir.toURI());
    // two files whose schema doesn't resolve
    List<String> filesCreated =
        random.ints(2).mapToObj(x -> "fakefsnotreal://file-" + x).collect(Collectors.toList());
    // one file in the local FS which doesn't actually exist but whose schema is valid
    // this MUST NOT be recorded as a failure
    filesCreated.add(new Path(parent, "file-not-exist").toUri().toString());
    assertThatThrownBy(() -> hadoopFileIO.deleteFiles(filesCreated))
        .describedAs("Exception raised by deleteFiles()")
        .isInstanceOf(BulkDeletionFailureException.class)
        .hasMessage("Failed to delete 2 files")
        .matches(
            (e) -> ((BulkDeletionFailureException) e).numberFailedObjects() == 2,
            "Wrong number of failures");
  }

  @Test
  public void testHadoopFileIOKryoSerialization() throws IOException {
    FileIO testHadoopFileIO = new HadoopFileIO();

    // hadoop fileIO should be serializable when properties are passed as immutable map
    testHadoopFileIO.initialize(ImmutableMap.of("k1", "v1"));
    FileIO roundTripSerializedFileIO = TestHelpers.KryoHelpers.roundTripSerialize(testHadoopFileIO);

    assertThat(roundTripSerializedFileIO.properties()).isEqualTo(testHadoopFileIO.properties());
  }

  @Test
  public void testHadoopFileIOJavaSerialization() throws IOException, ClassNotFoundException {
    FileIO testHadoopFileIO = new HadoopFileIO();

    // hadoop fileIO should be serializable when properties are passed as immutable map
    testHadoopFileIO.initialize(ImmutableMap.of("k1", "v1"));
    FileIO roundTripSerializedFileIO = TestHelpers.roundTripSerialize(testHadoopFileIO);

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

  /**
   * Verify that when bulk delete is available and enabled in the configuration, it will be used.
   */
  @TestTemplate
  public void testBulkDeleteAPIAvailablility() {
    assertThat(hadoopFileIO.isBulkDeleteApiUsed())
        .describedAs(
            "Bulk Delete API use where useBulkIOApi=%s, bulkDeleteAvailable=%s",
            useBulkIOApi, bulkDeleteAvailable)
        .isEqualTo(useBulkIOApi && bulkDeleteAvailable);
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
      String inputFilePath =
          Files.createTempDirectory(tempDir.toPath(), "junit").toFile().getAbsolutePath()
              + "/test.parquet";
      deserializedHadoopFileIO.newInputFile(
          File.createTempFile("test", "parquet", tempDir).toString());
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
                fs.create(path, true).close();
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            });
    return paths;
  }
}
