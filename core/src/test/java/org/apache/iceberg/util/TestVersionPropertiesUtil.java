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

package org.apache.iceberg.util;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class TestVersionPropertiesUtil {

  @BeforeAll
  public static void loadVersionPropertiesFromFile() {
    VersionPropertiesUtil.loadBuildInfo();
  }

  @Test
  public void testGitHash() {
    String gitHash = VersionPropertiesUtil.gitHash();
    Assert.assertNotNull("Git hash should not be null", gitHash);
    Assert.assertTrue("Git hash should have 10 characters", gitHash.length() == 10);
  }

  @Test
  public void testGitHashFull() {
    String gitHashFull = VersionPropertiesUtil.gitHashFull();
    Assertions.assertThat(gitHashFull)
        .isNotNull()
        .startsWith(VersionPropertiesUtil.gitHash())
        .hasSize(40);
  }

  @Test
  public void testProjectName() {
    String projectName = VersionPropertiesUtil.projectName();
    Assert.assertEquals("Project version should match the project name where VersionPropertiesUtil is found",
        "iceberg-core", projectName);
  }

  @Test
  @Disabled("In CI, projectVersion is the same as gitHash though the runtime jar has the expected value")
  public void testProjectVersion() {
    // projectVersion potentially ends with -SNAPSHOT and has the form XX.YY.ZZ, e.g. 0.14.0-SNAPSHOT
    String projectVersion = VersionPropertiesUtil.projectVersion();
    String projectVersionWithoutSnapshot = projectVersion
        .replaceAll("-SNAPSHOT", "")
        .replaceAll("\\.", "");
    Assertions.assertThat(projectVersionWithoutSnapshot).isNotNull()
        .containsOnlyDigits();
  }
}
