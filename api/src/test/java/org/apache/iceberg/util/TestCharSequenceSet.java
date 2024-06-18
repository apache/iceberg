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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

public class TestCharSequenceSet {

  // This test just verifies https://errorprone.info/bugpattern/CollectionUndefinedEquality
  @Test
  public void testSearchingInCharSequenceCollection() {
    Set<CharSequence> set = CharSequenceSet.of(Arrays.asList("abc", new StringBuffer("def")));
    assertThat(set).contains("abc");
    assertThat(set.stream().anyMatch("def"::contains)).isTrue();

    // this would fail with a normal Set<CharSequence>
    assertThat(set.contains("def")).isTrue();
  }

  @Test
  public void nullString() {
    assertThat(CharSequenceSet.of(Arrays.asList((String) null))).contains((String) null);
    assertThat(CharSequenceSet.empty()).doesNotContain((String) null);
  }

  @Test
  public void testRetainAll() {
    CharSequenceSet set = CharSequenceSet.of(ImmutableList.of("123", "456"));

    assertThat(set.retainAll(ImmutableList.of("456", "789", 123)))
        .overridingErrorMessage("Set should be changed")
        .isTrue();

    assertThat(set).hasSize(1).contains("456");

    set = CharSequenceSet.of(ImmutableList.of("123", "456"));
    assertThat(set.retainAll(ImmutableList.of("123", "456")))
        .overridingErrorMessage("Set should not be changed")
        .isFalse();

    assertThat(set.retainAll(ImmutableList.of(123, 456)))
        .overridingErrorMessage("Set should be changed")
        .isTrue();

    assertThat(set).isEmpty();
  }

  @Test
  public void testRemoveAll() {
    CharSequenceSet set = CharSequenceSet.of(ImmutableList.of("123", "456"));
    assertThat(set.removeAll(ImmutableList.of("456", "789", 123)))
        .overridingErrorMessage("Set should be changed")
        .isTrue();

    assertThat(set).hasSize(1).contains("123");

    set = CharSequenceSet.of(ImmutableList.of("123", "456"));
    assertThat(set.removeAll(ImmutableList.of(123, 456)))
        .overridingErrorMessage("Set should not be changed")
        .isFalse();

    assertThat(set.removeAll(ImmutableList.of("123", "456")))
        .overridingErrorMessage("Set should be changed")
        .isTrue();

    assertThat(set).isEmpty();
  }

  @Test
  public void testEqualsAndHashCode() {
    CharSequenceSet set1 = CharSequenceSet.empty();
    CharSequenceSet set2 = CharSequenceSet.empty();

    assertThat(set1).isEqualTo(set2);
    assertThat(set1.hashCode()).isEqualTo(set2.hashCode());

    set1.add("v1");
    set1.add("v2");
    set1.add("v3");

    set2.add(new StringBuilder("v1"));
    set2.add(new StringBuffer("v2"));
    set2.add("v3");

    Set<CharSequence> set3 = Collections.unmodifiableSet(set2);

    Set<CharSequenceWrapper> set4 =
        ImmutableSet.of(
            CharSequenceWrapper.wrap("v1"),
            CharSequenceWrapper.wrap(new StringBuffer("v2")),
            CharSequenceWrapper.wrap(new StringBuffer("v3")));

    assertThat(set1).isEqualTo(set2).isEqualTo(set3).isEqualTo(set4);
    assertThat(set1.hashCode())
        .isEqualTo(set2.hashCode())
        .isEqualTo(set3.hashCode())
        .isEqualTo(set4.hashCode());
  }
}
