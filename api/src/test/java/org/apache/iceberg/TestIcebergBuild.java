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

import java.util.Locale;
import java.util.regex.Pattern;
import org.junit.Assert;
import org.junit.Test;

public class TestIcebergBuild {
  @Test
  public void testFullVersion() {
    Assert.assertEquals(
        "Should build full version from version and commit ID",
        "Apache Iceberg " + IcebergBuild.version() + " (commit " + IcebergBuild.gitCommitId() + ")",
        IcebergBuild.fullVersion());
  }

  @Test
  public void testVersion() {
    Assert.assertNotEquals("Should not use unknown version", "unknown", IcebergBuild.version());
  }

  @Test
  public void testGitCommitId() {
    Assert.assertNotEquals(
        "Should not use unknown commit ID", "unknown", IcebergBuild.gitCommitId());
    Assert.assertTrue(
        "Should be a hexadecimal string of 20 bytes",
        Pattern.compile("[0-9a-f]{40}")
            .matcher(IcebergBuild.gitCommitId().toLowerCase(Locale.ROOT))
            .matches());
  }
}
