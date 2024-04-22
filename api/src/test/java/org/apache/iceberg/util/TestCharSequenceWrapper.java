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

import org.junit.jupiter.api.Test;

public class TestCharSequenceWrapper {

  @Test
  public void nullWrapper() {
    CharSequenceWrapper one = CharSequenceWrapper.wrap(null);
    CharSequenceWrapper two = CharSequenceWrapper.wrap(null);

    // at this point hashCode is not computed yet
    assertThat(one).isEqualTo(two);

    // hashCode is lazily computed and stored
    assertThat(one.hashCode()).isEqualTo(two.hashCode()).isEqualTo(0);

    assertThat(one).isEqualTo(two);
  }

  @Test
  public void equalsWithLazyHashCode() {
    CharSequenceWrapper string = CharSequenceWrapper.wrap("v1");
    CharSequenceWrapper buffer = CharSequenceWrapper.wrap(new StringBuffer("v1"));
    CharSequenceWrapper builder = CharSequenceWrapper.wrap(new StringBuilder("v1"));

    // at this point hashCode is 0 for all
    assertThat(string).isEqualTo(buffer).isEqualTo(builder);

    // hashCode is lazily computed and stored
    assertThat(string.hashCode()).isEqualTo(buffer.hashCode()).isEqualTo(builder.hashCode());

    assertThat(string).isEqualTo(buffer).isEqualTo(builder);
  }

  @Test
  public void notEqualsWithLazyHashCode() {
    CharSequenceWrapper v1 = CharSequenceWrapper.wrap("v1");
    CharSequenceWrapper v2 = CharSequenceWrapper.wrap("v2");

    // at this point hashCode is 0 for all
    assertThat(v1).isNotEqualTo(v2);

    // hashCode is lazily computed and stored
    assertThat(v1.hashCode()).isNotEqualTo(v2.hashCode());

    assertThat(v1).isNotEqualTo(v2);
  }

  @Test
  public void hashCodeIsRecomputed() {
    CharSequenceWrapper wrapper = CharSequenceWrapper.wrap("v1");
    assertThat(wrapper.hashCode()).isEqualTo(173804);

    wrapper.set("v2");
    assertThat(wrapper.hashCode()).isEqualTo(173805);

    wrapper.set(new StringBuffer("v2"));
    assertThat(wrapper.hashCode()).isEqualTo(173805);

    wrapper.set(new StringBuilder("v2"));
    assertThat(wrapper.hashCode()).isEqualTo(173805);

    wrapper.set("v3");
    assertThat(wrapper.hashCode()).isEqualTo(173806);

    wrapper.set(null);
    assertThat(wrapper.hashCode()).isEqualTo(0);

    wrapper.set("v2");
    assertThat(wrapper.hashCode()).isEqualTo(173805);
  }
}
