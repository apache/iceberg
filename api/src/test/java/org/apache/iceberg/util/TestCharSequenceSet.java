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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import org.apache.iceberg.TestHelpers;
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
    assertThatThrownBy(() -> CharSequenceSet.of(Arrays.asList((String) null)))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid object: null");
    assertThat(CharSequenceSet.empty()).doesNotContain((String) null);
  }

  @Test
  public void emptySet() {
    assertThat(CharSequenceSet.empty()).isEmpty();
    assertThat(CharSequenceSet.empty()).doesNotContain("a", "b", "c");
  }

  @Test
  public void insertionOrderIsMaintained() {
    CharSequenceSet set = CharSequenceSet.empty();
    set.addAll(ImmutableList.of("d", "a", "c"));
    set.add("b");
    set.add("d");

    assertThat(set).hasSize(4).containsExactly("d", "a", "c", "b");
  }

  @Test
  public void clear() {
    CharSequenceSet set = CharSequenceSet.of(ImmutableList.of("a", "b"));
    set.clear();
    assertThat(set).isEmpty();
  }

  @Test
  public void addAll() {
    CharSequenceSet empty = CharSequenceSet.empty();
    assertThatThrownBy(() -> empty.add(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid object: null");

    assertThatThrownBy(() -> empty.addAll(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid collection: null");

    assertThatThrownBy(() -> empty.addAll(Collections.singletonList(null)))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid object: null");

    assertThatThrownBy(() -> empty.addAll(Arrays.asList("a", null)))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid object: null");

    CharSequenceSet set = CharSequenceSet.empty();
    set.addAll(ImmutableList.of("b", "a", "c", "a"));
    assertThat(set).hasSize(3).containsExactly("b", "a", "c");
  }

  @Test
  public void contains() {
    CharSequenceSet set = CharSequenceSet.of(ImmutableList.of("b", "a"));
    assertThatThrownBy(() -> set.contains(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid object: null");

    assertThat(set).hasSize(2).containsExactly("b", "a").doesNotContain("c").doesNotContain("d");

    assertThatThrownBy(() -> CharSequenceSet.of(Arrays.asList("c", "b", null, "a")))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid object: null");
  }

  @Test
  public void containsAll() {
    CharSequenceSet set = CharSequenceSet.of(ImmutableList.of("b", "a"));
    assertThatThrownBy(() -> set.containsAll(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid collection: null");

    assertThatThrownBy(() -> set.containsAll(Collections.singletonList(null)))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid object: null");

    assertThatThrownBy(() -> set.containsAll(Arrays.asList("a", null)))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid object: null");

    assertThat(set.containsAll(ImmutableList.of("a", "b"))).isTrue();
    assertThat(set.containsAll(ImmutableList.of("b", "a", "c"))).isFalse();
    assertThat(set.containsAll(ImmutableList.of("b"))).isTrue();
  }

  @Test
  public void testRetainAll() {
    CharSequenceSet empty = CharSequenceSet.empty();
    assertThatThrownBy(() -> empty.retainAll(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid collection: null");

    assertThatThrownBy(() -> empty.retainAll(Collections.singletonList(null)))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid object: null");

    assertThatThrownBy(() -> empty.retainAll(Arrays.asList("123", null)))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid object: null");

    assertThatThrownBy(() -> empty.retainAll(ImmutableList.of("456", "789", 123)))
        .isInstanceOf(ClassCastException.class)
        .hasMessage("Cannot cast java.lang.Integer to java.lang.CharSequence");

    CharSequenceSet set = CharSequenceSet.of(ImmutableList.of("123", "456"));

    assertThat(set.retainAll(ImmutableList.of("456", "789", "555")))
        .overridingErrorMessage("Set should be changed")
        .isTrue();

    assertThat(set).hasSize(1).contains("456");

    set = CharSequenceSet.of(ImmutableList.of("123", "456"));
    assertThat(set.retainAll(ImmutableList.of("123", "456")))
        .overridingErrorMessage("Set should not be changed")
        .isFalse();

    assertThat(set.retainAll(ImmutableList.of("555", "789")))
        .overridingErrorMessage("Set should be changed")
        .isTrue();

    assertThat(set).isEmpty();
  }

  @Test
  public void toArray() {
    CharSequenceSet set = CharSequenceSet.of(ImmutableList.of("b", "a"));
    assertThat(set.toArray()).hasSize(2).containsExactly("b", "a");

    CharSequence[] array = new CharSequence[1];
    assertThat(set.toArray(array)).hasSize(2).containsExactly("b", "a");

    array = new CharSequence[0];
    assertThat(set.toArray(array)).hasSize(2).containsExactly("b", "a");

    array = new CharSequence[5];
    assertThat(set.toArray(array)).hasSize(5).containsExactly("b", "a", null, null, null);

    array = new CharSequence[2];
    assertThat(set.toArray(array)).hasSize(2).containsExactly("b", "a");
  }

  @Test
  public void remove() {
    CharSequenceSet set = CharSequenceSet.of(ImmutableSet.of("a", "b", "c"));
    assertThatThrownBy(() -> set.remove(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid object: null");

    set.remove("a");
    assertThat(set).containsExactly("b", "c");
    set.remove("b");
    assertThat(set).containsExactly("c");
    set.remove("c");
    assertThat(set).isEmpty();
  }

  @Test
  public void testRemoveAll() {
    CharSequenceSet empty = CharSequenceSet.empty();
    assertThatThrownBy(() -> empty.removeAll(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid collection: null");

    assertThatThrownBy(() -> empty.removeAll(Collections.singletonList(null)))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid object: null");

    assertThatThrownBy(() -> empty.removeAll(Arrays.asList("123", null)))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid object: null");

    assertThatThrownBy(() -> empty.removeAll(ImmutableList.of("123", 456)))
        .isInstanceOf(ClassCastException.class)
        .hasMessage("Cannot cast java.lang.Integer to java.lang.CharSequence");

    CharSequenceSet set = CharSequenceSet.of(ImmutableList.of("123", "456"));
    assertThat(set.removeAll(ImmutableList.of("456", "789")))
        .overridingErrorMessage("Set should be changed")
        .isTrue();

    assertThat(set).hasSize(1).contains("123");

    set = CharSequenceSet.of(ImmutableList.of("123", "456"));
    assertThat(set.removeAll(ImmutableList.of("333", "789")))
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

  @Test
  public void kryoSerialization() throws Exception {
    CharSequenceSet charSequences = CharSequenceSet.of(ImmutableList.of("c", "b", "a"));
    assertThat(TestHelpers.KryoHelpers.roundTripSerialize(charSequences)).isEqualTo(charSequences);
  }

  @Test
  public void javaSerialization() throws Exception {
    CharSequenceSet charSequences = CharSequenceSet.of(ImmutableList.of("c", "b", "a"));
    CharSequenceSet deserialize = TestHelpers.deserialize(TestHelpers.serialize(charSequences));
    assertThat(deserialize).isEqualTo(charSequences);
  }
}
