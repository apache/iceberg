/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.actions.rewrite;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MockFileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestBinPackStrategy extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 2 }; // We don't actually use the format version since everything is mock
  }

  private long MB = 1024 * 1024;

  public TestBinPackStrategy(int formatVersion) {
    super(formatVersion);
  }

  class TestBinPackStrategyImpl extends BinPackStrategy {

    @Override
    public Table table() {
      return table;
    }

    @Override
    public List<DataFile> rewriteFiles(List<FileScanTask> filesToRewrite) {
      throw new UnsupportedOperationException();
    }
  }

  private List<FileScanTask> filesOfSize(long... sizes) {
    return Arrays.stream(sizes).mapToObj(size -> new MockFileScanTask(size * MB)).collect(Collectors.toList());
  }

  private RewriteStrategy defaultBinPack() {
    return new TestBinPackStrategyImpl().options(Collections.emptyMap());
  }

  @Test
  public void testFilteringAllValid() {
    RewriteStrategy strategy = defaultBinPack();
    Iterable<FileScanTask> testFiles = filesOfSize( 100, 100, 100, 100, 1000);

    Iterable<FileScanTask> filtered = strategy.selectFilesToRewrite(testFiles);

    Assert.assertEquals("No files should be removed from the set", testFiles, filtered);
  }

  @Test
  public void testFilteringRemoveInvalid() {
    RewriteStrategy strategy = defaultBinPack();
    Iterable<FileScanTask> testFiles = filesOfSize( 500, 500, 500, 600, 600);

    Iterable<FileScanTask> filtered = strategy.selectFilesToRewrite(testFiles);

    Assert.assertEquals("All files should be removed from the set", Collections.emptyList(), filtered);
  }

  @Test
  public void testFilteringCustomMinMaxFileSize() {
    RewriteStrategy strategy = defaultBinPack().options(ImmutableMap.of(
        BinPackStrategy.MAX_FILE_SIZE_BYTES, Long.toString(550 * MB),
        BinPackStrategy.MIN_FILE_SIZE_BYTES, Long.toString(490 * MB)
    ));

    Iterable<FileScanTask> testFiles = filesOfSize( 500, 500, 480, 480, 560, 520);
    Iterable<FileScanTask> expectedFiles = filesOfSize(480, 480, 560);

    Iterable<FileScanTask> filtered = strategy.selectFilesToRewrite(testFiles);

    Assert.assertEquals("Should remove files that exceed or are smaller than new bounds", expectedFiles, filtered);
  }

  @Test
  public void testGroupingMinInputFilesInvalid() {
    RewriteStrategy strategy = defaultBinPack().options(ImmutableMap.of(
        BinPackStrategy.MIN_NUM_INPUT_FILES, Integer.toString(5)
    ));

    Iterable<FileScanTask> testFiles = filesOfSize( 1, 1, 1, 1);

    Iterable<List<FileScanTask>> grouped = strategy.planFileGroups(testFiles);

    Assert.assertEquals("Should plan 0 groups, not enough input files",
        0, Iterables.size(grouped));
  }

  @Test
  public void testGroupingMinInputFilesValid() {
    RewriteStrategy strategy = defaultBinPack().options(ImmutableMap.of(
        BinPackStrategy.MIN_NUM_INPUT_FILES, Integer.toString(5)
    ));

    Iterable<FileScanTask> testFiles = filesOfSize( 1, 1, 1, 1, 1);

    Iterable<List<FileScanTask>> grouped = strategy.planFileGroups(testFiles);

    Assert.assertEquals("Should plan 1 groups since there are enough input files",
        ImmutableList.of(testFiles), grouped);
  }

  @Test
  public void testGroupingMinOutputFilesInvalid() {
    RewriteStrategy strategy = defaultBinPack().options(ImmutableMap.of(
        BinPackStrategy.MIN_NUM_OUTPUT_FILES, Integer.toString(3)
    ));


    Iterable<FileScanTask> testFiles = filesOfSize( 200, 200, 200, 200, 200);

    Iterable<List<FileScanTask>> grouped = strategy.planFileGroups(testFiles);

    Assert.assertEquals("Should plan 1 groups since there would be 2 output files",
        Collections.emptyList(), grouped);
  }

  @Test
  public void testGroupingMinOutputFilesValid() {
    RewriteStrategy strategy = defaultBinPack().options(ImmutableMap.of(
        BinPackStrategy.MIN_NUM_OUTPUT_FILES, Integer.toString(2)
    ));

    Iterable<FileScanTask> testFiles = filesOfSize( 200, 200, 200, 200, 200);

    Iterable<List<FileScanTask>> grouped = strategy.planFileGroups(testFiles);

    Assert.assertEquals("Should plan 1 groups since there would be 2 output files",
        ImmutableList.of(testFiles), grouped);
  }

  @Test
  public void testMaxGroupSize() {
    RewriteStrategy strategy = defaultBinPack().options(ImmutableMap.of(
        RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Long.toString(1000 * MB)
    ));

    Iterable<FileScanTask> testFiles = filesOfSize( 300, 300, 300, 300, 300, 300);

    Iterable<List<FileScanTask>> grouped = strategy.planFileGroups(testFiles);

    Assert.assertEquals("Should plan 2 groups since there is enough data for two groups",
        2, Iterables.size(grouped));
  }
}
