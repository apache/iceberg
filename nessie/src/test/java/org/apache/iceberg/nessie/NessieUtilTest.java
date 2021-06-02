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

package org.apache.iceberg.nessie;

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
import org.projectnessie.model.CommitMeta;

import static org.apache.iceberg.nessie.NessieUtil.SPARK_APP_ID;
import static org.apache.iceberg.nessie.NessieUtil.SPARK_USER;

public class NessieUtilTest {

  @Test(expected = IllegalArgumentException.class)
  public void testBuildingCommitMetadataWithNullCatalogOptions() {
    NessieUtil.buildCommitMetadata("msg", null);
  }

  @Test
  public void testSparkAppIdAndUserIsSetOnCommitMetadata() {
    String commitMsg = "commit msg";
    String appId = "SPARK_ID_123";
    String user = "sparkUser";
    CommitMeta commitMeta = NessieUtil.buildCommitMetadata(
        commitMsg,
        ImmutableMap.of(SPARK_APP_ID, appId, SPARK_USER, user));
    Assert.assertEquals(commitMsg, commitMeta.getMessage());
    Assert.assertEquals(user, commitMeta.getAuthor());
    Assert.assertEquals(2, commitMeta.getProperties().size());
    Assert.assertEquals("iceberg", commitMeta.getProperties().get("application.type"));
    Assert.assertEquals(appId, commitMeta.getProperties().get(SPARK_APP_ID));
  }

  @Test
  public void testAuthorIsSetOnCommitMetadata() {
    String commitMsg = "commit msg";
    CommitMeta commitMeta = NessieUtil.buildCommitMetadata(commitMsg, ImmutableMap.of());
    Assert.assertEquals(commitMsg, commitMeta.getMessage());
    Assert.assertEquals(System.getProperty("user.name"), commitMeta.getAuthor());
    Assert.assertEquals(1, commitMeta.getProperties().size());
    Assert.assertEquals("iceberg", commitMeta.getProperties().get("application.type"));
  }
}
