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
package org.apache.iceberg;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.actions.BaseRewriteDataFilesAction;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestRewriteLimitSize extends TableTestBase {

  private final long totalSize;
  private final Set<String> expectedDeletedFiles;

  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
      {20L, Sets.newHashSet("/path/to/data-a.parquet", "/path/to/data-a-2.parquet")},
      {
        40L,
        Sets.newHashSet(
            "/path/to/data-a.parquet",
            "/path/to/data-a-2.parquet",
            "/path/to/data-b.parquet",
            "/path/to/data-b-2.parquet")
      }
    };
  }

  public TestRewriteLimitSize(long totalSize, Set<String> expectedDeletedFiles) {
    super(2); // We don't actually use the format version since everything is mock
    this.totalSize = totalSize;
    this.expectedDeletedFiles = expectedDeletedFiles;
  }

  @Before
  public void before() throws Exception {
    table
        .newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_A2)
        .appendFile(FILE_B)
        .appendFile(FILE_B2)
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .commit();
  }

  @Test
  public void limitRewriteSizeTest() {
    MockAction action = new MockAction(table);
    RewriteDataFilesActionResult result = action.totalSize(totalSize).execute();
    Set<CharSequence> deletedFilesNames =
        result.deletedDataFiles().stream().map(ContentFile::path).collect(Collectors.toSet());

    Assert.assertEquals(expectedDeletedFiles, deletedFilesNames);
  }

  private static class MockAction extends BaseRewriteDataFilesAction<MockAction> {

    protected MockAction(Table table) {
      super(table);
    }

    @Override
    protected FileIO fileIO() {
      return table().io();
    }

    @Override
    protected List<DataFile> rewriteDataForTasks(List<CombinedScanTask> combinedScanTask) {
      return Collections.emptyList();
    }

    @Override
    protected MockAction self() {
      return this;
    }
  }
}
