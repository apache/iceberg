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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class TestRewriteTablePathUtil {

  @Test
  public void testStagingPathPreservesDirectoryStructure() {
    String sourcePrefix = "/source/table";
    String stagingDir = "/staging/";

    // Two files with same name but different paths
    String file1 = "/source/table/hash1/delete_0_0_0.parquet";
    String file2 = "/source/table/hash2/delete_0_0_0.parquet";

    String stagingPath1 = RewriteTablePathUtil.stagingPath(file1, sourcePrefix, stagingDir);
    String stagingPath2 = RewriteTablePathUtil.stagingPath(file2, sourcePrefix, stagingDir);

    // Should preserve directory structure to avoid conflicts
    assertThat(stagingPath1)
        .startsWith(stagingDir)
        .isEqualTo("/staging/hash1/delete_0_0_0.parquet")
        .isNotEqualTo(stagingPath2);
    assertThat(stagingPath2)
        .startsWith(stagingDir)
        .isEqualTo("/staging/hash2/delete_0_0_0.parquet");
  }

  @Test
  public void testStagingPathBackwardCompatibility() {
    // Test that the deprecated method still works
    String originalPath = "/some/path/file.parquet";
    String stagingDir = "/staging/";

    String result = RewriteTablePathUtil.stagingPath(originalPath, stagingDir);

    assertThat(result).isEqualTo("/staging/file.parquet");
  }

  @Test
  public void testStagingPathWithComplexPaths() {
    String sourcePrefix = "/warehouse/db/table";
    String stagingDir = "/tmp/staging/";

    String filePath = "/warehouse/db/table/data/year=2023/month=01/part-00001.parquet";
    String result = RewriteTablePathUtil.stagingPath(filePath, sourcePrefix, stagingDir);

    assertThat(result).isEqualTo("/tmp/staging/data/year=2023/month=01/part-00001.parquet");
  }

  @Test
  public void testStagingPathWithNoMiddlePart() {
    // Test case where file is directly under source prefix (no middle directory structure)
    String sourcePrefix = "/source/table";
    String stagingDir = "/staging/";
    String fileDirectlyUnderPrefix = "/source/table/file.parquet";

    // Test new method
    String newMethodResult =
        RewriteTablePathUtil.stagingPath(fileDirectlyUnderPrefix, sourcePrefix, stagingDir);

    // Test old deprecated method
    String oldMethodResult = RewriteTablePathUtil.stagingPath(fileDirectlyUnderPrefix, stagingDir);

    // Both methods should behave the same when there's no middle part
    assertThat(newMethodResult).isEqualTo("/staging/file.parquet");
    assertThat(oldMethodResult).isEqualTo("/staging/file.parquet");
    assertThat(newMethodResult).isEqualTo(oldMethodResult);
  }
}
