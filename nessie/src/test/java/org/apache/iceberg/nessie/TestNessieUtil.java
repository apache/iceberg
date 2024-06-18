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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.CommitMeta;

public class TestNessieUtil {

  @Test
  public void testBuildingCommitMetadataWithNullCatalogOptions() {
    assertThatThrownBy(() -> NessieUtil.buildCommitMetadata("msg", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("catalogOptions must not be null");
  }

  @Test
  public void testSparkAppIdAndUserIsSetOnCommitMetadata() {
    String commitMsg = "commit msg";
    String appId = "SPARK_ID_123";
    String user = "sparkUser";
    CommitMeta commitMeta =
        NessieUtil.buildCommitMetadata(
            commitMsg,
            ImmutableMap.of(CatalogProperties.APP_ID, appId, CatalogProperties.USER, user));
    assertThat(commitMeta.getMessage()).isEqualTo(commitMsg);
    assertThat(commitMeta.getAuthor()).isEqualTo(user);
    assertThat(commitMeta.getProperties()).hasSize(2);
    assertThat(commitMeta.getProperties().get(NessieUtil.APPLICATION_TYPE)).isEqualTo("iceberg");
    assertThat(commitMeta.getProperties().get(CatalogProperties.APP_ID)).isEqualTo(appId);
  }

  @Test
  public void testAuthorIsSetOnCommitMetadata() {
    String commitMsg = "commit msg";
    CommitMeta commitMeta = NessieUtil.buildCommitMetadata(commitMsg, ImmutableMap.of());
    assertThat(commitMeta.getMessage()).isEqualTo(commitMsg);
    assertThat(commitMeta.getAuthor()).isEqualTo(System.getProperty("user.name"));
    assertThat(commitMeta.getProperties()).hasSize(1);
    assertThat(commitMeta.getProperties().get(NessieUtil.APPLICATION_TYPE)).isEqualTo("iceberg");
  }

  @Test
  public void testAuthorIsNullWithoutJvmUser() {
    String jvmUserName = System.getProperty("user.name");
    try {
      System.clearProperty("user.name");
      CommitMeta commitMeta = NessieUtil.buildCommitMetadata("commit msg", ImmutableMap.of());
      assertThat(commitMeta.getAuthor()).isNull();
    } finally {
      System.setProperty("user.name", jvmUserName);
    }
  }
}
