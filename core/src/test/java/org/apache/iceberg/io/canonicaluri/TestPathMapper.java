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
package org.apache.iceberg.io.canonicaluri;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestPathMapper {
  @Test
  void mapsCanonicalRootAndPreservesPathData() {
    PathMapper mapper = mapper(rule("root", "s3://warehouse", "ofs://om/s3v/warehouse"));

    assertThat(mapper.toPhysical("s3://warehouse", false).location())
        .isEqualTo("ofs://om/s3v/warehouse");
    assertThat(mapper.toPhysical("s3://warehouse/", false).location())
        .isEqualTo("ofs://om/s3v/warehouse/");
    assertThat(mapper.toPhysical("S3://warehouse/a%20b/%2F/雪//c", false).location())
        .isEqualTo("ofs://om/s3v/warehouse/a%20b/%2F/雪//c");
  }

  @Test
  void usesLongestCanonicalPrefix() {
    PathMapper mapper =
        mapper(
            rule("root", "s3://warehouse/", "ofs://om/s3v/warehouse/"),
            rule("special", "s3://warehouse/special/", "ofs://other/special/"));

    MappingResult result = mapper.toPhysical("s3://warehouse/special/a.parquet", false);
    assertThat(result.location()).isEqualTo("ofs://other/special/a.parquet");
    assertThat(result.rule().id()).isEqualTo("special");
  }

  @Test
  void reverseMapsPhysicalLocation() {
    PathMapper mapper = mapper(rule("root", "s3://warehouse/", "ofs://om/s3v/warehouse/"));

    assertThat(mapper.toCanonical("ofs://om/s3v/warehouse/db/t/a.parquet").location())
        .isEqualTo("s3://warehouse/db/t/a.parquet");
  }

  @Test
  void appliesUnmatchedPolicy() {
    MappingRule rule = rule("root", "s3://warehouse/", "ofs://om/s3v/warehouse/");
    PathMapper passthrough =
        new PathMapper(
            true, PathMapper.UnmatchedPolicy.PASSTHROUGH, Collections.singletonList(rule));
    PathMapper fail =
        new PathMapper(true, PathMapper.UnmatchedPolicy.FAIL, Collections.singletonList(rule));

    assertThat(passthrough.toPhysical("s3://external/a", false).location())
        .isEqualTo("s3://external/a");
    assertThatThrownBy(() -> fail.toPhysical("s3://external/a", false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No path alias rule matches");
  }

  @Test
  void rejectsPhysicalAliasesForStrictOperations() {
    PathMapper mapper = mapper(rule("root", "s3://warehouse/", "ofs://om/s3v/warehouse/"));

    assertThatThrownBy(() -> mapper.toPhysical("ofs://om/s3v/warehouse/data/a.parquet", true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("physical alias");
  }

  @Test
  void rejectsDuplicateAndOverlappingPhysicalPrefixes() {
    assertThatThrownBy(
            () ->
                mapper(
                    rule("a", "s3://a/", "ofs://om/common/"),
                    rule("b", "s3://b/", "ofs://om/common/")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("duplicate physical prefix");

    assertThatThrownBy(
            () ->
                mapper(
                    rule("a", "s3://a/", "ofs://om/common/"),
                    rule("b", "s3://b/", "ofs://om/common/nested/")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("physical prefixes overlap");
  }

  @Test
  void rejectsCanonicalAndPhysicalNamespaceOverlap() {
    assertThatThrownBy(
            () ->
                mapper(
                    rule("a", "s3://warehouse/", "ofs://om/common/"),
                    rule("b", "ofs://om/common/nested/", "file:/tmp/nested/")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("canonical prefix for rule b overlaps physical prefix for rule a");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "relative/path",
        "s3:///missing-authority",
        "s3://user@warehouse/a",
        "s3://warehouse/a?secret=value",
        "s3://warehouse/a#fragment",
        "s3://warehouse/../secret",
        "s3://warehouse/%2e%2e/secret",
        "s3://warehouse/a%2F..%2Fsecret",
        "s3://warehouse/%ZZ"
      })
  void rejectsUnsafeLocations(String location) {
    PathMapper mapper = mapper(rule("root", "s3://warehouse/", "ofs://om/s3v/warehouse/"));

    assertThatThrownBy(() -> mapper.toPhysical(location, false))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void disabledMapperIsACompleteKillSwitch() {
    PathMapper mapper =
        new PathMapper(
            false,
            PathMapper.UnmatchedPolicy.FAIL,
            Collections.singletonList(rule("root", "s3://warehouse/", "ofs://om/s3v/warehouse/")));

    assertThat(mapper.toPhysical("not even a URI", true).location()).isEqualTo("not even a URI");
  }

  private static PathMapper mapper(MappingRule... rules) {
    return new PathMapper(true, PathMapper.UnmatchedPolicy.PASSTHROUGH, Arrays.asList(rules));
  }

  private static MappingRule rule(String id, String canonical, String physical) {
    return new MappingRule(id, canonical, physical);
  }
}
