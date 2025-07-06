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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

public class TestFileURI {

  @Test
  public void testSchemeMatchWithSameScheme() {
    FileURI uri1 =
        new FileURI(
            new Path("hdfs://namenode/path").toUri(),
            Collections.emptyMap(),
            Collections.emptyMap());
    FileURI uri2 =
        new FileURI(
            new Path("hdfs://namenode/path").toUri(),
            Collections.emptyMap(),
            Collections.emptyMap());

    assertThat(uri1.schemeMatch(uri2)).isTrue();
  }

  @Test
  public void testSchemeMatchWithDifferentScheme() {
    FileURI uri1 =
        new FileURI(
            new Path("hdfs://namenode/path").toUri(),
            Collections.emptyMap(),
            Collections.emptyMap());
    FileURI uri2 =
        new FileURI(
            new Path("file:///path").toUri(), Collections.emptyMap(), Collections.emptyMap());

    assertThat(uri1.schemeMatch(uri2)).isFalse();
  }

  @Test
  public void testSchemeMatchWithEmptyScheme() {
    FileURI uri1 =
        new FileURI(
            new Path("hdfs://namenode/path").toUri(),
            Collections.emptyMap(),
            Collections.emptyMap());
    FileURI uri2 =
        new FileURI(new Path("/path").toUri(), Collections.emptyMap(), Collections.emptyMap());

    assertThat(uri1.schemeMatch(uri2)).isFalse();
  }

  @Test
  public void testSchemeMatchWithNullScheme() {
    FileURI uri1 = new FileURI();
    uri1.setScheme(null);
    FileURI uri2 = new FileURI();
    uri2.setScheme("hdfs");

    assertThat(uri1.schemeMatch(uri2)).isTrue();
  }

  @Test
  public void testAuthorityMatchWithSameAuthority() {
    FileURI uri1 =
        new FileURI(
            new Path("hdfs://namenode/path").toUri(),
            Collections.emptyMap(),
            Collections.emptyMap());
    FileURI uri2 =
        new FileURI(
            new Path("hdfs://namenode/path").toUri(),
            Collections.emptyMap(),
            Collections.emptyMap());

    assertThat(uri1.authorityMatch(uri2)).isTrue();
  }

  @Test
  public void testAuthorityMatchWithDifferentAuthority() {
    FileURI uri1 =
        new FileURI(
            new Path("hdfs://namenode1/path").toUri(),
            Collections.emptyMap(),
            Collections.emptyMap());
    FileURI uri2 =
        new FileURI(
            new Path("hdfs://namenode2/path").toUri(),
            Collections.emptyMap(),
            Collections.emptyMap());

    assertThat(uri1.authorityMatch(uri2)).isFalse();
  }

  @Test
  public void testAuthorityMatchWithEmptyAuthority() {
    FileURI uri1 =
        new FileURI(
            new Path("hdfs://namenode/path").toUri(),
            Collections.emptyMap(),
            Collections.emptyMap());
    FileURI uri2 =
        new FileURI(
            new Path("file:///path").toUri(), Collections.emptyMap(), Collections.emptyMap());

    assertThat(uri1.authorityMatch(uri2)).isFalse();
  }

  @Test
  public void testAuthorityMatchWithNullAuthority() {
    FileURI uri1 = new FileURI();
    uri1.setAuthority(null);
    FileURI uri2 = new FileURI();
    uri2.setAuthority("namenode");

    assertThat(uri1.authorityMatch(uri2)).isTrue();
  }

  @Test
  public void testSchemeMapping() {
    Map<String, String> schemeMap = Maps.newHashMap();
    schemeMap.put("HDFS", "hdfs");

    FileURI uri1 =
        new FileURI(new Path("HDFS://namenode/path").toUri(), schemeMap, Collections.emptyMap());
    FileURI uri2 =
        new FileURI(new Path("hdfs://namenode/path").toUri(), schemeMap, Collections.emptyMap());

    assertThat(uri1.schemeMatch(uri2)).isTrue();
  }

  @Test
  public void testAuthorityMapping() {
    Map<String, String> authorityMap = Maps.newHashMap();
    authorityMap.put("OLD-NODE", "new-node");

    FileURI uri1 =
        new FileURI(new Path("hdfs://OLD-NODE/path").toUri(), Collections.emptyMap(), authorityMap);
    FileURI uri2 =
        new FileURI(new Path("hdfs://new-node/path").toUri(), Collections.emptyMap(), authorityMap);

    assertThat(uri1.authorityMatch(uri2)).isTrue();
  }
}
