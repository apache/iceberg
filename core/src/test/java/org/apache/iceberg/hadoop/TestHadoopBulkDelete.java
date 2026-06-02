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

import static org.apache.iceberg.hadoop.HadoopFileIO.BULK_DELETE_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BulkDelete;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests of {@link HadoopBulkDelete}; based off {@link TestHadoopFileIO} with some helper methods
 * copied and tests duplicated then parameterized for bulk delete on.
 */
public class TestHadoopBulkDelete {
  private final Random random = new Random(1);

  private FileSystem fs;
  private HadoopFileIO hadoopFileIO;

  @TempDir private File tempDir;

  @BeforeEach
  public void before() throws Exception {
    resetFileIOBinding(true);
    HadoopBulkDelete.markApiUnavailable(false);
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
  public void testDeleteFiles() {
    Path parent = new Path(tempDir.toURI());
    List<Path> filesCreated = createRandomFiles(parent, 10);
    hadoopFileIO.deleteFiles(filesCreated.stream().map(Path::toString).toList());
    assertPathsDoNotExist(filesCreated);
  }

  /** Test both delete paths through CatalogUtil. */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testBulkDeleteThroughCatalogUtil(boolean bulkDelete) {
    resetFileIOBinding(bulkDelete);
    List<Path> filesCreated = createRandomFiles(new Path(tempDir.toURI()), 10);
    CatalogUtil.deleteFiles(
        hadoopFileIO, filesCreated.stream().map(Path::toString).toList(), "uncommitted");
    assertPathsDoNotExist(filesCreated);
  }

  /**
   * Mock the bulk delete API as unavailable, verify that CatalogUtil.deleteFiles() doesn't raise an
   * exception -simply fails to delete the files. A warning is logged; this isn't picked up in the
   * test.
   */
  @Test
  public void testCatalogUtilWhenBulkDeleteUnavailable() {
    markBulkDeleteApiAsUnavailable();
    List<Path> filesCreated = createRandomFiles(new Path(tempDir.toURI()), 10);
    CatalogUtil.deleteFiles(
        hadoopFileIO, filesCreated.stream().map(Path::toString).toList(), "uncommitted");
    assertPathsExist(filesCreated);
  }

  /**
   * Extension of {@link TestHadoopFileIO} version, which verifies that even in the presence of
   * failures, files which can be deleted are.
   *
   * @param bulkDelete parameterization flag.
   */
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
    assertFileDoesNotExist(localButMissing);
    assertFileDoesNotExist(exists);
  }

  /**
   * Mock the bulk delete API as unavailable, then expect a delete with bulk delete enabled to fail.
   */
  @Test
  public void testDeleteFailureWhenBulkDeleteUnavailable() {
    resetFileIOBinding(true);
    markBulkDeleteApiAsUnavailable();
    assertThatThrownBy(() -> hadoopFileIO.deleteFiles(List.of(tempDir.toURI().toString())))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(BULK_DELETE_ENABLED);
  }

  /**
   * Create a dir with a child, expect a failure trying to delete the child, but also expect other
   * paths supplied to have been deleted. This is a way of generating the partial-bulk-delete
   * failure state.
   */
  @Test
  public void testBulkDeleteNonEmptyDirectory() {
    Path parent = new Path(tempDir.toURI());
    final String file = touch(new Path(parent, "file")).toString();
    final File directory = new File(tempDir, "dir");
    directory.mkdirs();
    touch(new Path(parent, "dir/child"));

    final String dirString = directory.toURI().toString();
    assertFileExists(dirString);
    // directory delete resulted in the whole operation reported as a failure.
    assertThatThrownBy(() -> hadoopFileIO.deleteFiles(List.of(dirString, file)))
        .isInstanceOf(BulkDeletionFailureException.class)
        .hasMessageContaining("Failed to delete 1 files");

    // directory is still there
    assertFileExists(dirString);
    // the file has been deleted
    assertFileDoesNotExist(file);
  }

  /**
   * With bulk delete disabled, deleteFiles() still works when bulk delete is not available This
   * tests the classic invocation path of Spark 3.x. This test correctly triggers an exception on
   */
  @Test
  public void testDeleteFilesWithHadoopBulkDeleteNotAvailable() {
    resetFileIOBinding(false);
    markBulkDeleteApiAsUnavailable();
    Path parent = new Path(tempDir.toURI());
    List<Path> filesCreated = createRandomFiles(parent, 5);
    hadoopFileIO.deleteFiles(filesCreated.stream().map(Path::toString).toList());
    assertPathsDoNotExist(filesCreated);
  }

  private static void markBulkDeleteApiAsUnavailable() {
    HadoopBulkDelete.markApiUnavailable(true);
    assertThat(HadoopBulkDelete.apiAvailable())
        .describedAs("Mocking must make HadoopBulkDelete.apiAvailable() return false")
        .isFalse();
  }

  /**
   * A stub HadoopBulkDelete implementation allows for a page size greater than 1 to be tested,
   * through the DeleteContext class along with reporting of partial failures.
   */
  @Test
  public void testBulkDeleteLargerContext() {

    final Path root = new Path("/");

    // stub delete with page size of two, path #3 will be rejected on a deleteBatch() call
    final StubBulkDelete stubBulkDelete = new StubBulkDelete(2, 3);
    final HadoopBulkDelete.DeleteContext context =
        new HadoopBulkDelete.DeleteContext(root, stubBulkDelete);
    assertThat(context.pageSize()).isEqualTo(2);
    assertThat(context.size()).isEqualTo(0);
    assertThat(context.pageSizeReached()).isFalse();
    // add one path, assert state changes
    final Path p1 = new Path(root, "p1");
    context.add(p1);
    assertThat(context.size()).isEqualTo(1);
    assertThat(context.pageSizeReached()).isFalse();
    // add a second path, the page is now complete
    final Path p2 = new Path(root, "p2");
    context.add(p2);
    assertThat(context.pageSizeReached()).isTrue();
    // take a snapshot, it has the expected size and the context is reset
    final Set<Path> snapshot = context.snapshotDeletedFiles();
    assertThat(snapshot).hasSize(2);
    assertThat(context.size()).isEqualTo(0);
    assertThat(context.pageSizeReached()).isFalse();
    // perform the delete
    assertThat(context.deleteBatch(snapshot).failures()).isEmpty();
    final Path p3 = new Path(root, "p3");
    context.add(p3);
    final Path p4 = new Path(root, "p4");
    context.add(p4);
    assertThat(context.pageSizeReached()).isTrue();
    // delete all without a snapshot (as is done at the end of deleteFiles());
    // this is going report a failure.
    assertThat(context.deleteBatch(context.deletedFiles()).failures())
        .hasSize(1)
        .allSatisfy(
            r -> {
              assertThat(r.path()).isEqualTo(p3);
              assertThat(r.errorText()).isEqualTo("simulated failure");
            });
    // still deleted everything
    assertThat(stubBulkDelete.deleteCount).isEqualTo(4);
    assertThat(stubBulkDelete.deletedFiles).containsExactlyInAnyOrder(p1, p2, p4);
    context.close();
  }

  /**
   * Stub implementation of Hadoop's BulkDelete API which can be used behind HadoopBulkDelete to
   * verify invocation and failure handling.
   */
  private static final class StubBulkDelete implements BulkDelete {
    /** Page Size. */
    private final int pageSize;

    /** Failure count; used for fault injection. */
    private final int failOnDeleteCount;

    /** Count of successfully files. */
    private int deleteCount;

    /** Set of deleted files. */
    private final Set<Path> deletedFiles = Sets.newHashSet();

    /**
     * Create a stub.
     *
     * @param pageSize page size of deletions.
     * @param failOnDeleteCount when should a delete fail; set to 0 for no failures.
     */
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
        throws IllegalArgumentException {
      List<Map.Entry<Path, String>> failures = Lists.newArrayList();
      for (Path path : paths) {
        deleteCount++;
        if (deleteCount == failOnDeleteCount) {
          failures.add(new AbstractMap.SimpleEntry<>(path, "simulated failure"));
        } else {
          deletedFiles.add(path);
        }
      }
      return failures;
    }

    @Override
    public void close() {}
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
   * @return the path.
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
   * Assert a file exists.
   *
   * @param file URI to file/dir.
   */
  private void assertFileExists(String file) {
    assertThat(hadoopFileIO.newInputFile(file).exists())
        .describedAs("File %s must exist", file)
        .isTrue();
  }

  /**
   * Assert a file does not exist.
   *
   * @param file URI to file/dir.
   */
  private void assertFileDoesNotExist(String file) {
    assertThat(hadoopFileIO.newInputFile(file).exists())
        .describedAs("File %s must exist", file)
        .isFalse();
  }

  /**
   * Given a list of paths, assert none of them exist.
   *
   * @param paths list of paths
   */
  private void assertPathsDoNotExist(List<Path> paths) {
    paths.forEach(path -> assertFileDoesNotExist(path.toString()));
  }

  /**
   * Given a list of paths, assert all of them exist.
   *
   * @param paths list of paths
   */
  private void assertPathsExist(List<Path> paths) {
    paths.forEach(path -> assertFileExists(path.toString()));
  }
}
