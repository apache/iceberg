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

package org.apache.iceberg.actions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MockFileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableTestBase;
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
    return new Object[] {2}; // We don't actually use the format version since everything is mock
  }

  private static final long MB = 1024 * 1024;

  public TestBinPackStrategy(int formatVersion) {
    super(formatVersion);
  }

  class TestBinPackStrategyImpl extends BinPackStrategy {

    @Override
    public Table table() {
      return table;
    }

    @Override
    public Set<DataFile> rewriteFiles(List<FileScanTask> filesToRewrite) {
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

    Iterable<FileScanTask> testFiles = filesOfSize(100, 100, 100, 100, 1000);
    Iterable<FileScanTask> filtered = ImmutableList.copyOf(strategy.selectFilesToRewrite(testFiles));

    Assert.assertEquals("No files should be removed from the set", testFiles, filtered);
  }

  @Test
  public void testFilteringRemoveInvalid() {
    RewriteStrategy strategy = defaultBinPack();

    Iterable<FileScanTask> testFiles = filesOfSize(500, 500, 500, 600, 600);
    Iterable<FileScanTask> filtered = ImmutableList.copyOf(strategy.selectFilesToRewrite(testFiles));

    Assert.assertEquals("All files should be removed from the set", Collections.emptyList(), filtered);
  }

  @Test
  public void testFilteringCustomMinMaxFileSize() {
    RewriteStrategy strategy = defaultBinPack().options(ImmutableMap.of(
        BinPackStrategy.MAX_FILE_SIZE_BYTES, Long.toString(550 * MB),
        BinPackStrategy.MIN_FILE_SIZE_BYTES, Long.toString(490 * MB)
    ));

    Iterable<FileScanTask> testFiles = filesOfSize(500, 500, 480, 480, 560, 520);
    Iterable<FileScanTask> expectedFiles = filesOfSize(480, 480, 560);
    Iterable<FileScanTask> filtered = ImmutableList.copyOf(strategy.selectFilesToRewrite(testFiles));

    Assert.assertEquals("Should remove files that exceed or are smaller than new bounds", expectedFiles, filtered);
  }

  @Test
  public void testGroupingMinInputFilesInvalid() {
    RewriteStrategy strategy = defaultBinPack().options(ImmutableMap.of(
        BinPackStrategy.MIN_INPUT_FILES, Integer.toString(5)
    ));

    Iterable<FileScanTask> testFiles = filesOfSize(1, 1, 1, 1);

    Iterable<List<FileScanTask>> grouped = strategy.planFileGroups(testFiles);

    Assert.assertEquals("Should plan 0 groups, not enough input files",
        0, Iterables.size(grouped));
  }

  @Test
  public void testGroupWithLargeFileMinInputFiles() {
    RewriteStrategy strategy = defaultBinPack().options(ImmutableMap.of(
        BinPackStrategy.MIN_INPUT_FILES, Integer.toString(5)
    ));

    Iterable<FileScanTask> testFiles = filesOfSize(2000);

    Iterable<List<FileScanTask>> grouped = strategy.planFileGroups(testFiles);

    Assert.assertEquals("Should plan 1 groups, not enough input files but the input file exceeds our max" +
            "and can be written into at least one new target-file-size files",
        ImmutableList.of(testFiles), grouped);
  }

  @Test
  public void testGroupingMinInputFilesValid() {
    RewriteStrategy strategy = defaultBinPack().options(ImmutableMap.of(
        BinPackStrategy.MIN_INPUT_FILES, Integer.toString(5)
    ));

    Iterable<FileScanTask> testFiles = filesOfSize(1, 1, 1, 1, 1);

    Iterable<List<FileScanTask>> grouped = strategy.planFileGroups(testFiles);

    Assert.assertEquals("Should plan 1 groups since there are enough input files",
        ImmutableList.of(testFiles), grouped);
  }

  @Test
  public void testMaxGroupSize() {
    RewriteStrategy strategy = defaultBinPack().options(ImmutableMap.of(
        RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Long.toString(1000 * MB)
    ));

    Iterable<FileScanTask> testFiles = filesOfSize(300, 300, 300, 300, 300, 300);

    Iterable<List<FileScanTask>> grouped = strategy.planFileGroups(testFiles);

    Assert.assertEquals("Should plan 2 groups since there is enough data for two groups",
        2, Iterables.size(grouped));
  }

  @Test
  public void testNumOuputFiles() {
    BinPackStrategy strategy = (BinPackStrategy) defaultBinPack();
    long targetFileSize = strategy.targetFileSize();
    Assert.assertEquals("Should keep remainder if the remainder is a valid size",
        2, strategy.numOutputFiles(targetFileSize + 450 * MB));
    Assert.assertEquals("Should discard remainder file if the remainder is very small",
        1, strategy.numOutputFiles(targetFileSize + 40 * MB));
    Assert.assertEquals("Should keep remainder file if it would change average file size greatly",
        2, strategy.numOutputFiles((long) (targetFileSize + 0.40 * targetFileSize)));
    Assert.assertEquals("Should discard remainder if file is small and wouldn't change average that much",
        200, strategy.numOutputFiles(200 * targetFileSize + 13 * MB));
    Assert.assertEquals("Should keep remainder if it's a valid size",
        201, strategy.numOutputFiles(200 * targetFileSize + 499 * MB));
    Assert.assertEquals("Should not return 0 even for very small files",
        1, strategy.numOutputFiles(1));
  }

  @Test
  public void testInvalidOptions() {
    AssertHelpers.assertThrows("Should not allow max size smaller than target",
        IllegalArgumentException.class, () -> {
            defaultBinPack().options(ImmutableMap.of(
                BinPackStrategy.MAX_FILE_SIZE_BYTES, Long.toString(1 * MB)));
        });

    AssertHelpers.assertThrows("Should not allow min size larger than target",
        IllegalArgumentException.class, () -> {
          defaultBinPack().options(ImmutableMap.of(
              BinPackStrategy.MIN_FILE_SIZE_BYTES, Long.toString(1000 * MB)));
        });

    AssertHelpers.assertThrows("Should not allow min input size smaller tha 1",
        IllegalArgumentException.class, () -> {
          defaultBinPack().options(ImmutableMap.of(
              BinPackStrategy.MIN_INPUT_FILES, Long.toString(-5)));
        });

    AssertHelpers.assertThrows("Should not allow negative target size",
        IllegalArgumentException.class, () -> {
          defaultBinPack().options(ImmutableMap.of(
              RewriteDataFiles.TARGET_FILE_SIZE_BYTES, Long.toString(-5)));
        });
  }
}
